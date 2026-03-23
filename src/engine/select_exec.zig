const std = @import("std");
const sql = @import("../sql.zig");
const expr_mod = @import("../expr/mod.zig");
const ops = @import("value_ops.zig");
const result_utils = @import("result_utils.zig");
const select_helpers = @import("select_helpers.zig");
const shared = @import("shared.zig");
const filter_plan = @import("filter_plan.zig");
const source_materialize = @import("source_materialize.zig");

const RowSet = shared.RowSet;
const Table = shared.Table;
const EvalCtx = shared.EvalCtx;
const SortRow = shared.SortRow;
const QueryRuntime = shared.QueryRuntime;
const DepthFilterPlan = shared.DepthFilterPlan;
const SourceRef = shared.SourceRef;
const Error = shared.Error;

pub fn executeSelect(
    self: anytype,
    allocator: std.mem.Allocator,
    sel: sql.Select,
    parent_ctx: ?*const EvalCtx,
    runtime: ?*QueryRuntime,
) Error!RowSet {
    var result = RowSet.init(allocator);
    errdefer result.deinit();

    var local_runtime = QueryRuntime.init(allocator);
    defer if (runtime == null) local_runtime.deinit(allocator);
    const rt = runtime orelse &local_runtime;

    var expr_arena = std.heap.ArenaAllocator.init(allocator);
    defer expr_arena.deinit();
    const ea = expr_arena.allocator();

    var temp_tables = std.ArrayList(*Table).empty;
    defer {
        for (temp_tables.items) |temp_table| {
            temp_table.deinit(allocator);
            allocator.destroy(temp_table);
        }
        temp_tables.deinit(allocator);
    }

    var sources = std.ArrayList(SourceRef).empty;
    defer sources.deinit(ea);
    for (sel.from) |from_item| {
        const table = if (from_item.subquery_sql != null)
            try source_materialize.materializeSubquery(self, allocator, from_item, parent_ctx, rt, &temp_tables)
        else if (self.tables.getPtr(from_item.table_name)) |existing|
            existing
        else if (self.views.getPtr(from_item.table_name)) |view|
            try self.materializeView(allocator, view, parent_ctx, rt, &temp_tables)
        else
            return Error.UnknownTable;
        try sources.append(ea, .{
            .table = table,
            .table_name = from_item.table_name,
            .alias = from_item.alias,
        });
    }

    const ProjectionKind = enum { expr, star_all, star_qualifier };
    const Projection = struct {
        kind: ProjectionKind,
        expr: ?*expr_mod.Expr,
        qualifier: ?[]const u8,
    };

    var projections = std.ArrayList(Projection).empty;
    defer projections.deinit(ea);
    for (sel.projections) |text| {
        const trimmed = normalizeProjectionExpr(text);
        if (std.mem.eql(u8, trimmed, "*")) {
            try projections.append(ea, .{ .kind = .star_all, .expr = null, .qualifier = null });
            continue;
        }
        if (select_helpers.parseQualifiedStar(trimmed)) |qual| {
            try projections.append(ea, .{
                .kind = .star_qualifier,
                .expr = null,
                .qualifier = try ea.dupe(u8, qual),
            });
            continue;
        }
        const parsed = expr_mod.parse(ea, trimmed) catch return Error.InvalidSql;
        try projections.append(ea, .{ .kind = .expr, .expr = parsed, .qualifier = null });
    }

    const where_expr = if (sel.where_expr) |w|
        expr_mod.parse(ea, w) catch return Error.InvalidSql
    else
        null;

    var order_exprs = std.ArrayList(?*expr_mod.Expr).empty;
    defer order_exprs.deinit(ea);
    for (sel.order_by) |term| {
        if (term.is_ordinal) {
            try order_exprs.append(ea, null);
        } else {
            const oe = expr_mod.parse(ea, term.expr) catch return Error.InvalidSql;
            try order_exprs.append(ea, oe);
        }
    }

    if (sel.from.len == 0) {
        var dummy_table = Table{
            .name = "",
            .columns = std.ArrayList([]const u8).empty,
            .integer_affinity = std.ArrayList(bool).empty,
            .rows = std.ArrayList([]@import("../value.zig").Value).empty,
            .primary_key_col = null,
        };
        var ctx = EvalCtx{
            .table = &dummy_table,
            .table_name = "",
            .alias = null,
            .row = null,
            .parent = parent_ctx,
        };

        if (where_expr) |w| {
            const cond = try self.evalExpr(allocator, w, &ctx, rt);
            if (!ops.toSqlBool(cond)) return result;
        }

        const out_row = try allocator.alloc(@import("../value.zig").Value, projections.items.len);
        errdefer allocator.free(out_row);
        for (projections.items, 0..) |p, i| {
            if (p.kind != .expr) return Error.InvalidSql;
            const value = try self.evalExpr(allocator, p.expr.?, &ctx, rt);
            out_row[i] = try result_utils.cloneResultValue(allocator, value);
        }
        try result.rows.append(allocator, out_row);
        return result;
    }

    var aggregate_exprs = std.ArrayList(*expr_mod.Expr).empty;
    defer aggregate_exprs.deinit(ea);
    for (projections.items) |p| {
        if (p.kind == .expr) try aggregate_exprs.append(ea, p.expr.?);
    }

    if (ops.isAggregateProjectionList(aggregate_exprs.items)) {
        if (aggregate_exprs.items.len != projections.items.len) return Error.InvalidSql;
        if (sources.items.len != 1) return Error.UnsupportedSql;
        const primary = sources.items[0];
        const row = self.evalAggregateRow(
            allocator,
            primary.table,
            sel,
            aggregate_exprs.items,
            where_expr,
            parent_ctx,
            primary.alias orelse primary.table_name,
            rt,
        ) catch |err| {
            if (err == Error.AggregateEmpty) return result;
            return err;
        };
        try result.rows.append(allocator, row);
        return result;
    }

    const order_by_count = sel.order_by.len;
    var rows = std.ArrayList(SortRow).empty;
    defer {
        for (rows.items) |entry| {
            allocator.free(entry.keys);
            allocator.free(entry.descending);
        }
        rows.deinit(allocator);
    }

    for (sources.items) |source| {
        if (source.table.rows.items.len == 0) return result;
    }

    var local_filters = try allocator.alloc(std.ArrayList(*expr_mod.Expr), sources.items.len);
    defer {
        for (local_filters) |*list| list.deinit(allocator);
        allocator.free(local_filters);
    }
    for (local_filters) |*list| list.* = std.ArrayList(*expr_mod.Expr).empty;

    var all_conjuncts = std.ArrayList(*expr_mod.Expr).empty;
    defer all_conjuncts.deinit(allocator);
    var pair_masks = std.ArrayList(u64).empty;
    defer pair_masks.deinit(allocator);

    var residual_filters = std.ArrayList(*expr_mod.Expr).empty;
    defer residual_filters.deinit(allocator);

    if (where_expr) |w| {
        var conjuncts = std.ArrayList(*expr_mod.Expr).empty;
        defer conjuncts.deinit(allocator);
        try select_helpers.collectAndConjuncts(allocator, w, &conjuncts);
        for (conjuncts.items) |conjunct| {
            try all_conjuncts.append(allocator, conjunct);
            const dep = try select_helpers.exprDependency(conjunct, sources.items, ops.eqlIgnoreCase);
            if (dep.supported and dep.mask != 0 and @popCount(dep.mask) == 1) {
                const single_idx = @ctz(dep.mask);
                try local_filters[single_idx].append(allocator, conjunct);
            }
            if (dep.supported and @popCount(dep.mask) == 2) {
                try pair_masks.append(allocator, dep.mask);
            }
        }
    }

    var candidate_rows = try allocator.alloc(std.ArrayList(usize), sources.items.len);
    defer {
        for (candidate_rows) |*list| list.deinit(allocator);
        allocator.free(candidate_rows);
    }
    for (candidate_rows) |*list| list.* = std.ArrayList(usize).empty;

    for (sources.items, 0..) |source, i| {
        if (local_filters[i].items.len == 0) {
            for (source.table.rows.items, 0..) |_, row_idx| {
                try candidate_rows[i].append(allocator, row_idx);
            }
        } else {
            for (source.table.rows.items, 0..) |row, row_idx| {
                var local_ctx = EvalCtx{
                    .table = source.table,
                    .table_name = source.table_name,
                    .alias = source.alias,
                    .row = row,
                    .parent = parent_ctx,
                };
                var keep = true;
                for (local_filters[i].items) |filter_expr| {
                    const cond_val = try self.evalExpr(allocator, filter_expr, &local_ctx, rt);
                    if (!ops.toSqlBool(cond_val)) {
                        keep = false;
                        break;
                    }
                }
                if (keep) try candidate_rows[i].append(allocator, row_idx);
            }
        }
        if (candidate_rows[i].items.len == 0) return result;
    }

    const source_lengths = try allocator.alloc(usize, sources.items.len);
    defer allocator.free(source_lengths);
    for (candidate_rows, 0..) |rows_for_source, i| source_lengths[i] = rows_for_source.items.len;

    const iter_order = try allocator.alloc(usize, sources.items.len);
    defer allocator.free(iter_order);
    const source_to_iter_pos = try allocator.alloc(usize, sources.items.len);
    defer allocator.free(source_to_iter_pos);
    const chosen = try allocator.alloc(bool, sources.items.len);
    defer allocator.free(chosen);
    @memset(chosen, false);

    var chosen_mask: u64 = 0;
    for (iter_order) |*slot| {
        var best_idx: ?usize = null;
        var best_connected: usize = 0;
        var best_local: usize = 0;
        var best_len: usize = 0;

        for (sources.items, 0..) |_, candidate_idx| {
            if (chosen[candidate_idx]) continue;

            var connected: usize = 0;
            if (chosen_mask != 0) {
                const bit = (@as(u64, 1) << @intCast(candidate_idx));
                for (pair_masks.items) |mask| {
                    if ((mask & bit) == 0) continue;
                    if ((mask & chosen_mask) != 0) connected += 1;
                }
            }
            const local_count = local_filters[candidate_idx].items.len;
            const candidate_len = source_lengths[candidate_idx];

            if (best_idx == null) {
                best_idx = candidate_idx;
                best_connected = connected;
                best_local = local_count;
                best_len = candidate_len;
                continue;
            }

            const prefer = (connected > best_connected) or
                (connected == best_connected and local_count > best_local) or
                (connected == best_connected and local_count == best_local and candidate_len < best_len) or
                (connected == best_connected and local_count == best_local and candidate_len == best_len and candidate_idx < best_idx.?);

            if (prefer) {
                best_idx = candidate_idx;
                best_connected = connected;
                best_local = local_count;
                best_len = candidate_len;
            }
        }

        const picked = best_idx orelse return Error.InvalidSql;
        slot.* = picked;
        chosen[picked] = true;
        chosen_mask |= (@as(u64, 1) << @intCast(picked));
    }
    for (iter_order, 0..) |src_idx, pos| source_to_iter_pos[src_idx] = pos;

    var depth_filters = try allocator.alloc(std.ArrayList(DepthFilterPlan), sources.items.len);
    defer {
        for (depth_filters) |*list| list.deinit(allocator);
        allocator.free(depth_filters);
    }
    for (depth_filters) |*list| list.* = std.ArrayList(DepthFilterPlan).empty;

    for (all_conjuncts.items) |conjunct| {
        const dep = try select_helpers.exprDependency(conjunct, sources.items, ops.eqlIgnoreCase);
        if (!dep.supported or dep.mask == 0) {
            try residual_filters.append(allocator, conjunct);
            continue;
        }
        if (@popCount(dep.mask) == 1) continue;
        var max_pos: usize = 0;
        for (sources.items, 0..) |_, src_idx| {
            if ((dep.mask & (@as(u64, 1) << @intCast(src_idx))) != 0) {
                max_pos = @max(max_pos, source_to_iter_pos[src_idx]);
            }
        }
        try depth_filters[max_pos].append(allocator, try filter_plan.buildDepthFilterPlan(conjunct, sources.items));
    }

    const projection_source_idxs = try allocator.alloc(?usize, projections.items.len);
    defer allocator.free(projection_source_idxs);
    var out_count: usize = 0;
    for (projections.items, 0..) |p, i| {
        switch (p.kind) {
            .expr => {
                projection_source_idxs[i] = null;
                out_count += 1;
            },
            .star_all => {
                projection_source_idxs[i] = null;
                for (sources.items) |src_ref| out_count += src_ref.table.columns.items.len;
            },
            .star_qualifier => {
                const qualified_src_idx = select_helpers.findQualifiedSourceIndex(sources.items, p.qualifier.?, ops.eqlIgnoreCase) orelse return Error.UnknownColumn;
                projection_source_idxs[i] = qualified_src_idx;
                out_count += sources.items[qualified_src_idx].table.columns.items.len;
            },
        }
    }

    const idxs = try allocator.alloc(usize, sources.items.len);
    defer allocator.free(idxs);
    @memset(idxs, 0);

    const ctx_by_source = try allocator.alloc(EvalCtx, sources.items.len);
    defer allocator.free(ctx_by_source);

    var current_depth: usize = 0;
    while (true) {
        const src_idx = iter_order[current_depth];
        if (idxs[src_idx] >= source_lengths[src_idx]) {
            idxs[src_idx] = 0;
            if (current_depth == 0) break;
            current_depth -= 1;
            const prev_src_idx = iter_order[current_depth];
            idxs[prev_src_idx] += 1;
            continue;
        }

        const source = sources.items[src_idx];
        const parent = if (current_depth == 0) parent_ctx else &ctx_by_source[iter_order[current_depth - 1]];

        ctx_by_source[src_idx] = .{
            .table = source.table,
            .table_name = source.table_name,
            .alias = source.alias,
            .row = source.table.rows.items[candidate_rows[src_idx].items[idxs[src_idx]]],
            .parent = parent,
        };

        var depth_ok = true;
        for (depth_filters[current_depth].items) |plan| {
            if (!try filter_plan.evalDepthFilterPlan(self, allocator, plan, &ctx_by_source[src_idx], ctx_by_source, rt)) {
                depth_ok = false;
                break;
            }
        }

        if (!depth_ok) {
            idxs[src_idx] += 1;
            continue;
        }

        if (current_depth + 1 < iter_order.len) {
            current_depth += 1;
            idxs[iter_order[current_depth]] = 0;
            continue;
        }

        const ctx = &ctx_by_source[src_idx];
        if (residual_filters.items.len > 0) {
            var residual_ok = true;
            for (residual_filters.items) |filter_expr| {
                const cond_val = try self.evalExpr(allocator, filter_expr, ctx, rt);
                if (!ops.toSqlBool(cond_val)) {
                    residual_ok = false;
                    break;
                }
            }
            if (!residual_ok) {
                idxs[src_idx] += 1;
                continue;
            }
        }

        const out_row = try allocator.alloc(@import("../value.zig").Value, out_count);
        var out_idx: usize = 0;
        for (projections.items, 0..) |p, proj_idx| {
            switch (p.kind) {
                .expr => {
                    const val = try self.evalExpr(allocator, p.expr.?, ctx, rt);
                    out_row[out_idx] = try result_utils.cloneResultValue(allocator, val);
                    out_idx += 1;
                },
                .star_all => {
                    for (sources.items, 0..) |_, source_idx| {
                        for (ctx_by_source[source_idx].row.?) |v| {
                            out_row[out_idx] = try result_utils.cloneResultValue(allocator, v);
                            out_idx += 1;
                        }
                    }
                },
                .star_qualifier => {
                    const qualified_src_idx = projection_source_idxs[proj_idx] orelse return Error.UnknownColumn;
                    for (ctx_by_source[qualified_src_idx].row.?) |v| {
                        out_row[out_idx] = try result_utils.cloneResultValue(allocator, v);
                        out_idx += 1;
                    }
                },
            }
        }

        if (order_by_count == 0) {
            try result.rows.append(allocator, out_row);
        } else {
            const keys = try allocator.alloc(@import("../value.zig").Value, order_by_count);
            const descending = try allocator.alloc(bool, order_by_count);
            for (sel.order_by, 0..) |term, i| {
                descending[i] = term.descending;
                if (term.is_ordinal) {
                    if (term.ordinal == 0 or term.ordinal > out_row.len) return Error.InvalidSql;
                    keys[i] = try result_utils.cloneResultValue(allocator, out_row[term.ordinal - 1]);
                } else {
                    const oexpr = order_exprs.items[i] orelse return Error.InvalidSql;
                    const kv = try self.evalExpr(allocator, oexpr, ctx, rt);
                    keys[i] = try result_utils.cloneResultValue(allocator, kv);
                }
            }
            try rows.append(allocator, .{ .values = out_row, .keys = keys, .descending = descending });
        }
        idxs[src_idx] += 1;
    }

    if (order_by_count > 0) {
        std.sort.heap(SortRow, rows.items, {}, result_utils.sortRowLessThan);
        for (rows.items) |entry| {
            try result.rows.append(allocator, entry.values);
        }
    }
    if (sel.distinct) result_utils.dedupRowsInPlace(allocator, &result);
    return result;
}

fn normalizeProjectionExpr(text: []const u8) []const u8 {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (findTopLevelProjectionAlias(trimmed)) |idx| {
        return std.mem.trim(u8, trimmed[0..idx], " \t\r\n");
    }
    if (findTopLevelBareAlias(trimmed)) |idx| {
        return std.mem.trim(u8, trimmed[0..idx], " \t\r\n");
    }
    return trimmed;
}

fn findTopLevelProjectionAlias(text: []const u8) ?usize {
    var in_string = false;
    var depth: usize = 0;
    var i: usize = 0;
    while (i + 4 <= text.len) : (i += 1) {
        const c = text[i];
        if (in_string) {
            if (c == '\'') in_string = false;
            continue;
        }
        if (c == '\'') {
            in_string = true;
            continue;
        }
        if (c == '(') {
            depth += 1;
            continue;
        }
        if (c == ')') {
            if (depth > 0) depth -= 1;
            continue;
        }
        if (depth != 0) continue;
        if (std.mem.eql(u8, text[i .. i + 4], " AS ")) {
            return i;
        }
    }
    return null;
}

fn findTopLevelBareAlias(text: []const u8) ?usize {
    var in_string = false;
    var depth: usize = 0;
    var last_space: ?usize = null;
    var i: usize = 0;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (in_string) {
            if (c == '\'') in_string = false;
            continue;
        }
        if (c == '\'') {
            in_string = true;
            continue;
        }
        if (c == '(') {
            depth += 1;
            continue;
        }
        if (c == ')') {
            if (depth > 0) depth -= 1;
            continue;
        }
        if (depth == 0 and std.ascii.isWhitespace(c)) last_space = i;
    }

    const idx = last_space orelse return null;
    const prefix = std.mem.trim(u8, text[0..idx], " \t\r\n");
    const suffix = std.mem.trim(u8, text[idx..], " \t\r\n");
    if (prefix.len == 0 or suffix.len == 0) return null;
    for (suffix) |c| {
        if (!std.ascii.isAlphanumeric(c) and c != '_') return null;
    }
    const last = prefix[prefix.len - 1];
    if (last == '+' or last == '-' or last == '*' or last == '/' or last == '(' or last == ',' or last == '<' or last == '>' or last == '=') {
        return null;
    }
    return idx;
}
