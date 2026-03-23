const std = @import("std");
const Value = @import("value.zig").Value;
const sql = @import("sql.zig");
const vm = @import("vm.zig");
const expr_mod = @import("expr/mod.zig");
const ops = @import("engine/value_ops.zig");
const types = @import("engine/types.zig");
pub const RowSet = types.RowSet;
const Table = types.Table;
const EvalCtx = types.EvalCtx;
const SortRow = types.SortRow;

pub const Error = error{
    OutOfMemory,
    TableAlreadyExists,
    UnknownTable,
    ColumnCountMismatch,
    UnknownColumn,
    InvalidSql,
    UnsupportedSql,
    InvalidLiteral,
};

pub const ExecResult = struct {
    rows_affected: usize = 0,
};

pub const RowState = enum { row, done };

pub const Engine = struct {
    allocator: std.mem.Allocator,
    tables: std.StringHashMap(Table),

    pub fn init(allocator: std.mem.Allocator) Engine {
        return .{ .allocator = allocator, .tables = std.StringHashMap(Table).init(allocator) };
    }

    pub fn deinit(self: *Engine) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var table = entry.value_ptr.*;
            table.deinit(self.allocator);
        }
        self.tables.deinit();
    }

    pub fn exec(self: *Engine, sql_text: []const u8) Error!ExecResult {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);
        switch (stmt) {
            .create_table => |create| {
                try self.handleCreate(create);
                return .{ .rows_affected = 0 };
            },
            .create_index => {
                // Indexes are metadata only in this in-memory engine; treat as successful no-op.
                return .{ .rows_affected = 0 };
            },
            .insert => |ins| {
                try self.handleInsert(ins);
                return .{ .rows_affected = 1 };
            },
            .select => return Error.UnsupportedSql,
            .compound_select => return Error.UnsupportedSql,
        }
    }

    pub fn query(self: *Engine, allocator: std.mem.Allocator, sql_text: []const u8) Error!RowSet {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);
        return switch (stmt) {
            .select => |sel| try self.executeSelect(allocator, sel, null),
            .compound_select => |compound| try self.executeCompoundSelect(allocator, compound, null),
            else => Error.UnsupportedSql,
        };
    }

    fn handleCreate(self: *Engine, create: sql.CreateTable) Error!void {
        const key = try self.allocator.dupe(u8, create.table_name);
        errdefer self.allocator.free(key);
        const gop = try self.tables.getOrPut(key);
        if (gop.found_existing) {
            return Error.TableAlreadyExists;
        }

        var table = Table{
            .name = try self.allocator.dupe(u8, create.table_name),
            .columns = std.ArrayList([]const u8).empty,
            .rows = std.ArrayList([]Value).empty,
        };
        for (create.columns) |col| {
            try table.columns.append(self.allocator, try self.allocator.dupe(u8, col));
        }
        gop.value_ptr.* = table;
    }

    fn handleInsert(self: *Engine, ins: sql.Insert) Error!void {
        const table = self.tables.getPtr(ins.table_name) orelse return Error.UnknownTable;
        const row = try self.allocator.alloc(Value, table.columns.items.len);
        for (row) |*cell| cell.* = .null;

        if (ins.columns) |cols| {
            if (ins.values.len != cols.len) return Error.ColumnCountMismatch;
            for (cols, ins.values) |col_name, v| {
                const idx = vm.columnIndex(table.columns.items, col_name) orelse return Error.UnknownColumn;
                row[idx] = try v.clone(self.allocator);
            }
        } else {
            if (ins.values.len != table.columns.items.len) return Error.ColumnCountMismatch;
            for (ins.values, 0..) |v, i| row[i] = try v.clone(self.allocator);
        }
        try table.rows.append(self.allocator, row);
    }

    fn executeSelect(
        self: *Engine,
        allocator: std.mem.Allocator,
        sel: sql.Select,
        parent_ctx: ?*const EvalCtx,
    ) Error!RowSet {
        var result = RowSet.init(allocator);
        errdefer result.deinit();

        if (sel.from.len == 0) return Error.InvalidSql;

        var expr_arena = std.heap.ArenaAllocator.init(allocator);
        defer expr_arena.deinit();
        const ea = expr_arena.allocator();

        const SourceRef = struct {
            table: *const Table,
            table_name: []const u8,
            alias: ?[]const u8,
        };

        var sources = std.ArrayList(SourceRef).empty;
        defer sources.deinit(ea);
        for (sel.from) |from_item| {
            const table = self.tables.getPtr(from_item.table_name) orelse return Error.UnknownTable;
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
            const trimmed = std.mem.trim(u8, text, " \t\r\n");
            if (std.mem.eql(u8, trimmed, "*")) {
                try projections.append(ea, .{ .kind = .star_all, .expr = null, .qualifier = null });
                continue;
            }
            if (parseQualifiedStar(trimmed)) |qual| {
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

        var aggregate_exprs = std.ArrayList(*expr_mod.Expr).empty;
        defer aggregate_exprs.deinit(ea);
        for (projections.items) |p| {
            if (p.kind == .expr) try aggregate_exprs.append(ea, p.expr.?);
        }

        if (ops.isAggregateProjectionList(aggregate_exprs.items)) {
            if (aggregate_exprs.items.len != projections.items.len) return Error.InvalidSql;
            if (sources.items.len != 1) return Error.UnsupportedSql;
            const primary = sources.items[0];
            const row = try self.evalAggregateRow(
                allocator,
                primary.table,
                sel,
                aggregate_exprs.items,
                where_expr,
                parent_ctx,
                primary.alias orelse primary.table_name,
            );
            try result.rows.append(allocator, row);
            return result;
        }

        var rows = std.ArrayList(SortRow).empty;
        defer {
            for (rows.items) |entry| {
                allocator.free(entry.keys);
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
            try collectAndConjuncts(allocator, w, &conjuncts);
            for (conjuncts.items) |conjunct| {
                try all_conjuncts.append(allocator, conjunct);
                const dep = try exprDependency(conjunct, sources.items);
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
                        const cond_val = try self.evalExpr(allocator, filter_expr, &local_ctx);
                        if (!ops.toSqlBool(cond_val)) {
                            keep = false;
                            break;
                        }
                    }
                    if (keep) {
                        try candidate_rows[i].append(allocator, row_idx);
                    }
                }
            }
            if (candidate_rows[i].items.len == 0) return result;
        }

        const source_lengths = try allocator.alloc(usize, sources.items.len);
        defer allocator.free(source_lengths);
        for (candidate_rows, 0..) |rows_for_source, i| {
            source_lengths[i] = rows_for_source.items.len;
        }

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
        for (iter_order, 0..) |src_idx, pos| {
            source_to_iter_pos[src_idx] = pos;
        }

        var depth_filters = try allocator.alloc(std.ArrayList(*expr_mod.Expr), sources.items.len);
        defer {
            for (depth_filters) |*list| list.deinit(allocator);
            allocator.free(depth_filters);
        }
        for (depth_filters) |*list| list.* = std.ArrayList(*expr_mod.Expr).empty;

        for (all_conjuncts.items) |conjunct| {
            const dep = try exprDependency(conjunct, sources.items);
            if (!dep.supported or dep.mask == 0) {
                try residual_filters.append(allocator, conjunct);
                continue;
            }
            var max_pos: usize = 0;
            for (sources.items, 0..) |_, src_idx| {
                if ((dep.mask & (@as(u64, 1) << @intCast(src_idx))) != 0) {
                    max_pos = @max(max_pos, source_to_iter_pos[src_idx]);
                }
            }
            try depth_filters[max_pos].append(allocator, conjunct);
        }

        const idxs = try allocator.alloc(usize, sources.items.len);
        defer allocator.free(idxs);
        @memset(idxs, 0);

        const ctx_chain = try allocator.alloc(EvalCtx, sources.items.len);
        defer allocator.free(ctx_chain);

        while (true) {
            for (sources.items, 0..) |source, i| {
                const parent = if (i == 0) parent_ctx else &ctx_chain[i - 1];
                ctx_chain[i] = .{
                    .table = source.table,
                    .table_name = source.table_name,
                    .alias = source.alias,
                    .row = source.table.rows.items[candidate_rows[i].items[idxs[i]]],
                    .parent = parent,
                };
            }
            const ctx = &ctx_chain[sources.items.len - 1];

            var failed_depth: ?usize = null;
            var depth: usize = 0;
            while (depth < depth_filters.len and failed_depth == null) : (depth += 1) {
                for (depth_filters[depth].items) |filter_expr| {
                    const cond_val = try self.evalExpr(allocator, filter_expr, ctx);
                    if (!ops.toSqlBool(cond_val)) {
                        failed_depth = depth;
                        break;
                    }
                }
            }

            if (failed_depth) |d| {
                if (!advanceRowIndicesFromIterDepth(source_lengths, idxs, iter_order, d)) break;
                continue;
            }

            if (residual_filters.items.len > 0) {
                var residual_ok = true;
                for (residual_filters.items) |filter_expr| {
                    const cond_val = try self.evalExpr(allocator, filter_expr, ctx);
                    if (!ops.toSqlBool(cond_val)) {
                        residual_ok = false;
                        break;
                    }
                }
                if (!residual_ok) {
                    if (!advanceRowIndicesFromIterDepth(source_lengths, idxs, iter_order, iter_order.len - 1)) break;
                    continue;
                }
            }

            var out_count: usize = 0;
            for (projections.items) |p| {
                switch (p.kind) {
                    .expr => out_count += 1,
                    .star_all => {
                        for (sources.items) |source| {
                            out_count += source.table.columns.items.len;
                        }
                    },
                    .star_qualifier => {
                        const src_idx = findQualifiedSourceIndex(sources.items, p.qualifier.?) orelse return Error.UnknownColumn;
                        out_count += sources.items[src_idx].table.columns.items.len;
                    },
                }
            }

            const out_row = try allocator.alloc(Value, out_count);
            var out_idx: usize = 0;
            for (projections.items) |p| {
                switch (p.kind) {
                    .expr => {
                        const val = try self.evalExpr(allocator, p.expr.?, ctx);
                        out_row[out_idx] = try cloneResultValue(allocator, val);
                        out_idx += 1;
                    },
                    .star_all => {
                        for (ctx_chain) |entry| {
                            for (entry.row.?) |v| {
                                out_row[out_idx] = try cloneResultValue(allocator, v);
                                out_idx += 1;
                            }
                        }
                    },
                    .star_qualifier => {
                        const src_idx = findQualifiedSourceIndex(sources.items, p.qualifier.?) orelse return Error.UnknownColumn;
                        for (ctx_chain[src_idx].row.?) |v| {
                            out_row[out_idx] = try cloneResultValue(allocator, v);
                            out_idx += 1;
                        }
                    },
                }
            }

            const keys = try allocator.alloc(Value, sel.order_by.len);
            for (sel.order_by, 0..) |term, i| {
                if (term.is_ordinal) {
                    if (term.ordinal == 0 or term.ordinal > out_row.len) return Error.InvalidSql;
                    keys[i] = try cloneResultValue(allocator, out_row[term.ordinal - 1]);
                } else {
                    const oexpr = order_exprs.items[i] orelse return Error.InvalidSql;
                    const kv = try self.evalExpr(allocator, oexpr, ctx);
                    keys[i] = try cloneResultValue(allocator, kv);
                }
            }

            try rows.append(allocator, .{ .values = out_row, .keys = keys });
            if (!advanceRowIndicesFromIterDepth(source_lengths, idxs, iter_order, iter_order.len - 1)) break;
        }

        if (sel.order_by.len > 0) {
            std.sort.heap(SortRow, rows.items, {}, sortRowLessThan);
        }

        for (rows.items) |entry| {
            try result.rows.append(allocator, entry.values);
        }
        return result;
    }

    fn executeCompoundSelect(
        self: *Engine,
        allocator: std.mem.Allocator,
        compound: sql.CompoundSelect,
        parent_ctx: ?*const EvalCtx,
    ) Error!RowSet {
        if (compound.arms.len == 0) return Error.InvalidSql;
        if (compound.ops.len + 1 != compound.arms.len) return Error.InvalidSql;

        var arm_sets = std.ArrayList(RowSet).empty;
        defer {
            for (arm_sets.items) |*rs| rs.deinit();
            arm_sets.deinit(allocator);
        }
        for (compound.arms) |arm_sql| {
            try arm_sets.append(allocator, try self.queryTextWithParent(allocator, arm_sql, parent_ctx));
        }

        var groups = std.ArrayList(RowSet).empty;
        defer {
            for (groups.items) |*rs| rs.deinit();
            groups.deinit(allocator);
        }
        var group_ops = std.ArrayList(sql.SetOp).empty;
        defer group_ops.deinit(allocator);

        var current = try cloneRowSet(allocator, &arm_sets.items[0]);
        var current_owned = true;
        defer {
            if (current_owned) current.deinit();
        }

        for (compound.ops, 0..) |set_op, idx| {
            const rhs = &arm_sets.items[idx + 1];
            if (set_op == .intersect) {
                const next = try rowSetIntersect(allocator, &current, rhs);
                current.deinit();
                current = next;
                current_owned = true;
                continue;
            }

            try groups.append(allocator, current);
            current_owned = false;
            try group_ops.append(allocator, set_op);
            current = try cloneRowSet(allocator, rhs);
            current_owned = true;
        }
        try groups.append(allocator, current);
        current_owned = false;

        if (groups.items.len == 0) return Error.InvalidSql;
        var out = try cloneRowSet(allocator, &groups.items[0]);
        errdefer out.deinit();

        for (group_ops.items, 0..) |set_op, idx| {
            const rhs = &groups.items[idx + 1];
            switch (set_op) {
                .union_all => {
                    for (rhs.rows.items) |row| {
                        try appendClonedRow(allocator, &out, row);
                    }
                },
                .union_distinct => {
                    try dedupRowsInPlace(allocator, &out);
                    for (rhs.rows.items) |row| {
                        try appendDistinctRow(allocator, &out, row);
                    }
                },
                .except => {
                    const next = try rowSetExcept(allocator, &out, rhs);
                    out.deinit();
                    out = next;
                },
                .intersect => {
                    const next = try rowSetIntersect(allocator, &out, rhs);
                    out.deinit();
                    out = next;
                },
            }
        }

        if (compound.order_by.len > 0) {
            var rows = std.ArrayList(SortRow).empty;
            defer {
                for (rows.items) |entry| {
                    allocator.free(entry.keys);
                }
                rows.deinit(allocator);
            }

            for (out.rows.items) |row| {
                const keys = try allocator.alloc(Value, compound.order_by.len);
                for (compound.order_by, 0..) |term, i| {
                    if (!term.is_ordinal) return Error.InvalidSql;
                    if (term.ordinal == 0 or term.ordinal > row.len) return Error.InvalidSql;
                    keys[i] = try cloneResultValue(allocator, row[term.ordinal - 1]);
                }
                try rows.append(allocator, .{ .values = row, .keys = keys });
            }

            std.sort.heap(SortRow, rows.items, {}, sortRowLessThan);
            out.rows.clearRetainingCapacity();
            for (rows.items) |entry| {
                try out.rows.append(allocator, entry.values);
            }
        }

        return out;
    }

    fn evalAggregateRow(
        self: *Engine,
        allocator: std.mem.Allocator,
        table: *const Table,
        sel: sql.Select,
        projections: []const *expr_mod.Expr,
        where_expr: ?*expr_mod.Expr,
        parent_ctx: ?*const EvalCtx,
        alias: []const u8,
    ) Error![]Value {
        const row = try allocator.alloc(Value, projections.len);
        var seen_any = false;

        for (projections, 0..) |pexpr, i| {
            row[i] = try self.evalAggregateExpr(allocator, pexpr, table, sel, where_expr, parent_ctx, alias, &seen_any);
        }
        return row;
    }

    fn evalAggregateExpr(
        self: *Engine,
        allocator: std.mem.Allocator,
        pexpr: *expr_mod.Expr,
        table: *const Table,
        sel: sql.Select,
        where_expr: ?*expr_mod.Expr,
        parent_ctx: ?*const EvalCtx,
        alias: []const u8,
        seen_any_ptr: *bool,
    ) Error!Value {
        switch (pexpr.*) {
            .call => |call| {
                if (ops.eqlIgnoreCase(call.name, "count")) {
                    var count: i64 = 0;
                    for (table.rows.items) |source_row| {
                        var ctx = EvalCtx{
                            .table = table,
                            .table_name = sel.from[0].table_name,
                            .alias = alias,
                            .row = source_row,
                            .parent = parent_ctx,
                        };
                        if (where_expr) |w| {
                            const cond = try self.evalExpr(allocator, w, &ctx);
                            if (!ops.toSqlBool(cond)) continue;
                        }
                        seen_any_ptr.* = true;
                        if (call.star_arg) {
                            count += 1;
                        } else {
                            if (call.args.len != 1) return Error.InvalidSql;
                            const v = try self.evalExpr(allocator, call.args[0], &ctx);
                            if (v != .null) count += 1;
                        }
                    }
                    return Value{ .integer = count };
                }
                if (ops.eqlIgnoreCase(call.name, "avg")) {
                    if (call.args.len != 1) return Error.InvalidSql;
                    var sum: f64 = 0;
                    var cnt: usize = 0;
                    for (table.rows.items) |source_row| {
                        var ctx = EvalCtx{
                            .table = table,
                            .table_name = sel.from[0].table_name,
                            .alias = alias,
                            .row = source_row,
                            .parent = parent_ctx,
                        };
                        if (where_expr) |w| {
                            const cond = try self.evalExpr(allocator, w, &ctx);
                            if (!ops.toSqlBool(cond)) continue;
                        }
                        const v = try self.evalExpr(allocator, call.args[0], &ctx);
                        if (ops.toNumber(v)) |num| {
                            sum += num;
                            cnt += 1;
                            seen_any_ptr.* = true;
                        }
                    }
                    if (cnt == 0) return .null;
                    return Value{ .real = sum / @as(f64, @floatFromInt(cnt)) };
                }
                return Error.UnsupportedSql;
            },
            else => {
                if (!seen_any_ptr.*) return .null;
                return Error.UnsupportedSql;
            },
        }
    }

    fn evalExpr(self: *Engine, allocator: std.mem.Allocator, node: *expr_mod.Expr, ctx: *const EvalCtx) Error!Value {
        switch (node.*) {
            .literal => |v| return v,
            .ident => |id| return self.resolveIdentifier(ctx, id.qualifier, id.name),
            .unary => |u| {
                const inner = try self.evalExpr(allocator, u.expr, ctx);
                switch (u.op) {
                    .neg => {
                        if (inner == .integer) {
                            return Value{ .integer = -inner.integer };
                        }
                        if (inner == .real) {
                            return Value{ .real = -inner.real };
                        }
                        if (ops.toNumber(inner)) |num| {
                            return Value{ .real = -num };
                        }
                        return .null;
                    },
                    .not_op => {
                        const b = ops.toSqlBool(inner);
                        return Value{ .integer = if (b) 0 else 1 };
                    },
                }
            },
            .binary => |b| {
                const l = try self.evalExpr(allocator, b.left, ctx);
                const r = try self.evalExpr(allocator, b.right, ctx);
                return ops.evalBinary(b.op, l, r);
            },
            .between => |b| {
                const t = try self.evalExpr(allocator, b.target, ctx);
                const lo = try self.evalExpr(allocator, b.low, ctx);
                const hi = try self.evalExpr(allocator, b.high, ctx);
                if (t == .null or lo == .null or hi == .null) return .null;
                const left_cmp = ops.compareValues(t, lo);
                const right_cmp = ops.compareValues(t, hi);
                var ok = left_cmp >= 0 and right_cmp <= 0;
                if (b.not_between) ok = !ok;
                return Value{ .integer = if (ok) 1 else 0 };
            },
            .is_null => |n| {
                const v = try self.evalExpr(allocator, n.target, ctx);
                const is_null = v == .null;
                const out = if (n.not_null) !is_null else is_null;
                return Value{ .integer = if (out) 1 else 0 };
            },
            .in_list => |n| {
                const target = try self.evalExpr(allocator, n.target, ctx);
                if (target == .null) return .null;

                var has_null = false;
                for (n.items) |item| {
                    const rhs = try self.evalExpr(allocator, item, ctx);
                    if (rhs == .null) {
                        has_null = true;
                        continue;
                    }
                    if (ops.compareValues(target, rhs) == 0) {
                        return Value{ .integer = if (n.not_in) 0 else 1 };
                    }
                }
                if (has_null) return .null;
                return Value{ .integer = if (n.not_in) 1 else 0 };
            },
            .call => |call| {
                if (ops.eqlIgnoreCase(call.name, "abs")) {
                    if (call.args.len != 1) return Error.InvalidSql;
                    const v = try self.evalExpr(allocator, call.args[0], ctx);
                    if (v == .integer) {
                        const i = v.integer;
                        if (i == std.math.minInt(i64)) return Error.InvalidSql;
                        return Value{ .integer = if (i < 0) -i else i };
                    }
                    if (v == .real) return Value{ .real = @abs(v.real) };
                    if (ops.toNumber(v)) |n| return Value{ .real = @abs(n) };
                    return .null;
                }
                if (ops.eqlIgnoreCase(call.name, "coalesce")) {
                    for (call.args) |arg| {
                        const v = try self.evalExpr(allocator, arg, ctx);
                        if (v != .null) return v;
                    }
                    return .null;
                }
                return Error.UnsupportedSql;
            },
            .case_expr => |c| {
                if (c.base) |base_expr| {
                    const base = try self.evalExpr(allocator, base_expr, ctx);
                    for (c.whens) |w| {
                        const when_val = try self.evalExpr(allocator, w.cond, ctx);
                        const cmp = ops.evalBinary(.eq, base, when_val);
                        if (ops.toSqlBool(cmp)) return self.evalExpr(allocator, w.value, ctx);
                    }
                } else {
                    for (c.whens) |w| {
                        const cond = try self.evalExpr(allocator, w.cond, ctx);
                        if (ops.toSqlBool(cond)) return self.evalExpr(allocator, w.value, ctx);
                    }
                }
                if (c.else_expr) |e| return self.evalExpr(allocator, e, ctx);
                return .null;
            },
            .subquery => |sql_text| {
                var rs = try self.queryWithParent(allocator, sql_text, ctx);
                defer rs.deinit();
                if (rs.rows.items.len == 0) return .null;
                if (rs.rows.items[0].len == 0) return .null;
                return rs.rows.items[0][0];
            },
            .exists_subquery => |sql_text| {
                var rs = try self.queryWithParent(allocator, sql_text, ctx);
                defer rs.deinit();
                return Value{ .integer = if (rs.rows.items.len > 0) 1 else 0 };
            },
        }
    }

    fn queryWithParent(self: *Engine, allocator: std.mem.Allocator, sql_text: []const u8, parent: *const EvalCtx) Error!RowSet {
        return self.queryTextWithParent(allocator, sql_text, parent);
    }

    fn queryTextWithParent(
        self: *Engine,
        allocator: std.mem.Allocator,
        sql_text: []const u8,
        parent: ?*const EvalCtx,
    ) Error!RowSet {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);
        return switch (stmt) {
            .select => |sel| try self.executeSelect(allocator, sel, parent),
            .compound_select => |compound| try self.executeCompoundSelect(allocator, compound, parent),
            else => Error.InvalidSql,
        };
    }

    fn resolveIdentifier(_: *Engine, ctx: *const EvalCtx, qualifier: ?[]const u8, name: []const u8) Error!Value {
        var cur: ?*const EvalCtx = ctx;
        while (cur) |c| {
            const matches_qualifier = if (qualifier) |q|
                if (c.alias) |a|
                    ops.eqlIgnoreCase(q, a)
                else
                    ops.eqlIgnoreCase(q, c.table_name)
            else
                true;
            if (matches_qualifier and c.row != null) {
                if (vm.columnIndex(c.table.columns.items, name)) |idx| return c.row.?[idx];
            }
            cur = c.parent;
        }
        return Error.UnknownColumn;
    }

    pub fn prepare(self: *Engine, allocator: std.mem.Allocator, sql_text: []const u8) Error!Stmt {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return Engine.mapParseError(err);

        return switch (stmt) {
            .select, .compound_select => .{ .row_set = try self.query(allocator, sql_text), .cursor = 0 },
            else => blk: {
                try self.exec(sql_text);
                break :blk .{ .row_set = null, .cursor = 0 };
            },
        };
    }

    fn mapParseError(err: anyerror) Error {
        return switch (err) {
            sql.ParseError.InvalidSql => Error.InvalidSql,
            sql.ParseError.UnsupportedSql => Error.UnsupportedSql,
            sql.ParseError.InvalidLiteral => Error.InvalidLiteral,
            sql.ParseError.OutOfMemory => Error.OutOfMemory,
            else => Error.InvalidSql,
        };
    }
};

fn parseQualifiedStar(text: []const u8) ?[]const u8 {
    if (text.len < 3) return null;
    if (text[text.len - 2] != '.' or text[text.len - 1] != '*') return null;
    const qualifier = std.mem.trim(u8, text[0 .. text.len - 2], " \t\r\n");
    if (qualifier.len == 0) return null;
    return qualifier;
}

fn findQualifiedSourceIndex(sources: anytype, qualifier: []const u8) ?usize {
    var found: ?usize = null;
    for (sources, 0..) |source, i| {
        const matches = ops.eqlIgnoreCase(qualifier, source.table_name) or
            (source.alias != null and ops.eqlIgnoreCase(qualifier, source.alias.?));
        if (!matches) continue;
        if (found != null) return null;
        found = i;
    }
    return found;
}

const ExprDependency = struct {
    supported: bool,
    mask: u64,
};

fn collectAndConjuncts(
    allocator: std.mem.Allocator,
    node: *expr_mod.Expr,
    out: *std.ArrayList(*expr_mod.Expr),
) Error!void {
    switch (node.*) {
        .binary => |b| {
            if (b.op == .and_op) {
                try collectAndConjuncts(allocator, b.left, out);
                try collectAndConjuncts(allocator, b.right, out);
                return;
            }
        },
        else => {},
    }
    try out.append(allocator, node);
}

fn exprDependency(node: *expr_mod.Expr, sources: anytype) Error!ExprDependency {
    return switch (node.*) {
        .literal => .{ .supported = true, .mask = 0 },
        .ident => |id| .{ .supported = true, .mask = @as(u64, 1) << @intCast(try resolveIdentifierSourceIndex(sources, id.qualifier, id.name)) },
        .unary => |u| try exprDependency(u.expr, sources),
        .binary => |b| combineDependency(try exprDependency(b.left, sources), try exprDependency(b.right, sources)),
        .between => |b| combineDependency(
            combineDependency(try exprDependency(b.target, sources), try exprDependency(b.low, sources)),
            try exprDependency(b.high, sources),
        ),
        .is_null => |n| try exprDependency(n.target, sources),
        .in_list => |n| blk: {
            var dep = try exprDependency(n.target, sources);
            for (n.items) |item| {
                dep = combineDependency(dep, try exprDependency(item, sources));
            }
            break :blk dep;
        },
        .call => |c| blk: {
            var dep = ExprDependency{ .supported = true, .mask = 0 };
            for (c.args) |arg| {
                dep = combineDependency(dep, try exprDependency(arg, sources));
            }
            break :blk dep;
        },
        .case_expr => |c| blk: {
            var dep = ExprDependency{ .supported = true, .mask = 0 };
            if (c.base) |base| dep = combineDependency(dep, try exprDependency(base, sources));
            for (c.whens) |w| {
                dep = combineDependency(dep, try exprDependency(w.cond, sources));
                dep = combineDependency(dep, try exprDependency(w.value, sources));
            }
            if (c.else_expr) |e| dep = combineDependency(dep, try exprDependency(e, sources));
            break :blk dep;
        },
        .subquery, .exists_subquery => .{ .supported = false, .mask = 0 },
    };
}

fn combineDependency(a: ExprDependency, b: ExprDependency) ExprDependency {
    if (!a.supported or !b.supported) return .{ .supported = false, .mask = 0 };
    return .{ .supported = true, .mask = a.mask | b.mask };
}

fn resolveIdentifierSourceIndex(sources: anytype, qualifier: ?[]const u8, name: []const u8) Error!usize {
    if (qualifier) |q| {
        const idx = findQualifiedSourceIndex(sources, q) orelse return Error.UnknownColumn;
        if (vm.columnIndex(sources[idx].table.columns.items, name) == null) return Error.UnknownColumn;
        return idx;
    }

    var found: ?usize = null;
    for (sources, 0..) |source, i| {
        if (vm.columnIndex(source.table.columns.items, name) != null) {
            if (found != null) return Error.UnknownColumn;
            found = i;
        }
    }
    return found orelse Error.UnknownColumn;
}

fn advanceRowIndicesByLengths(lengths: []const usize, idxs: []usize) bool {
    if (idxs.len == 0) return false;
    var pos = idxs.len;
    while (pos > 0) {
        pos -= 1;
        idxs[pos] += 1;
        if (idxs[pos] < lengths[pos]) return true;
        idxs[pos] = 0;
    }
    return false;
}

fn advanceRowIndicesFromDepth(lengths: []const usize, idxs: []usize, depth: usize) bool {
    if (idxs.len == 0) return false;
    var pos = depth + 1;
    while (pos > 0) {
        pos -= 1;
        idxs[pos] += 1;
        if (idxs[pos] < lengths[pos]) {
            var reset_pos = pos + 1;
            while (reset_pos < idxs.len) : (reset_pos += 1) {
                idxs[reset_pos] = 0;
            }
            return true;
        }
        idxs[pos] = 0;
    }
    return false;
}

fn advanceRowIndicesFromIterDepth(
    lengths: []const usize,
    idxs: []usize,
    iter_order: []const usize,
    iter_depth: usize,
) bool {
    if (idxs.len == 0) return false;
    var pos = iter_depth + 1;
    while (pos > 0) {
        pos -= 1;
        const src = iter_order[pos];
        idxs[src] += 1;
        if (idxs[src] < lengths[src]) {
            var reset_pos = pos + 1;
            while (reset_pos < iter_order.len) : (reset_pos += 1) {
                const reset_src = iter_order[reset_pos];
                idxs[reset_src] = 0;
            }
            return true;
        }
        idxs[src] = 0;
    }
    return false;
}

pub const Stmt = struct {
    row_set: ?RowSet,
    cursor: usize,

    pub fn deinit(self: *Stmt) void {
        if (self.row_set) |*rs| rs.deinit();
    }

    pub fn step(self: *Stmt) RowState {
        const rs = &(self.row_set orelse return .done);
        if (self.cursor >= rs.rows.items.len) return .done;
        self.cursor += 1;
        return .row;
    }

    pub fn column(self: *Stmt, idx: usize) Value {
        const rs = &(self.row_set orelse unreachable);
        const row_idx = self.cursor - 1;
        return rs.rows.items[row_idx][idx];
    }

    pub fn bind(_: *Stmt, _: usize, _: Value) Error!void {
        return Error.UnsupportedSql;
    }
};

fn cloneResultValue(allocator: std.mem.Allocator, v: Value) Error!Value {
    return switch (v) {
        .text => |t| .{ .text = try allocator.dupe(u8, t) },
        .blob => |b| .{ .blob = try allocator.dupe(u8, b) },
        else => v,
    };
}

fn sortRowLessThan(_: void, a: SortRow, b: SortRow) bool {
    var i: usize = 0;
    while (i < a.keys.len and i < b.keys.len) : (i += 1) {
        const cmp = ops.compareValues(a.keys[i], b.keys[i]);
        if (cmp < 0) return true;
        if (cmp > 0) return false;
    }
    return false;
}

fn cloneRowSet(allocator: std.mem.Allocator, src: *const RowSet) Error!RowSet {
    var out = RowSet.init(allocator);
    errdefer out.deinit();
    for (src.rows.items) |row| {
        try appendClonedRow(allocator, &out, row);
    }
    return out;
}

fn rowSetIntersect(allocator: std.mem.Allocator, left: *const RowSet, right: *const RowSet) Error!RowSet {
    var out = RowSet.init(allocator);
    errdefer out.deinit();
    for (left.rows.items) |row| {
        if (containsRow(right.rows.items, row) and !containsRow(out.rows.items, row)) {
            try appendClonedRow(allocator, &out, row);
        }
    }
    return out;
}

fn rowSetExcept(allocator: std.mem.Allocator, left: *const RowSet, right: *const RowSet) Error!RowSet {
    var out = RowSet.init(allocator);
    errdefer out.deinit();
    for (left.rows.items) |row| {
        if (!containsRow(right.rows.items, row) and !containsRow(out.rows.items, row)) {
            try appendClonedRow(allocator, &out, row);
        }
    }
    return out;
}

fn dedupRowsInPlace(allocator: std.mem.Allocator, rs: *RowSet) Error!void {
    var i: usize = 0;
    while (i < rs.rows.items.len) : (i += 1) {
        var j: usize = i + 1;
        while (j < rs.rows.items.len) {
            if (rowsEqual(rs.rows.items[i], rs.rows.items[j])) {
                const removed = rs.rows.orderedRemove(j);
                freeOwnedRow(allocator, removed);
                continue;
            }
            j += 1;
        }
    }
}

fn freeOwnedRow(allocator: std.mem.Allocator, row: []Value) void {
    for (row) |v| switch (v) {
        .text => |t| allocator.free(t),
        .blob => |b| allocator.free(b),
        else => {},
    };
    allocator.free(row);
}

fn appendClonedRow(allocator: std.mem.Allocator, rs: *RowSet, row: []const Value) Error!void {
    const copied = try allocator.alloc(Value, row.len);
    for (row, 0..) |v, i| {
        copied[i] = try cloneResultValue(allocator, v);
    }
    try rs.rows.append(allocator, copied);
}

fn appendDistinctRow(allocator: std.mem.Allocator, rs: *RowSet, row: []const Value) Error!void {
    if (containsRow(rs.rows.items, row)) return;
    try appendClonedRow(allocator, rs, row);
}

fn containsRow(rows: []const []Value, target: []const Value) bool {
    for (rows) |row| {
        if (rowsEqual(row, target)) return true;
    }
    return false;
}

fn rowsEqual(a: []const Value, b: []const Value) bool {
    if (a.len != b.len) return false;
    for (a, b) |av, bv| {
        if (ops.compareValues(av, bv) != 0) return false;
    }
    return true;
}
