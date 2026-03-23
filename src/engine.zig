const std = @import("std");
const Value = @import("value.zig").Value;
const sql = @import("sql.zig");
const vm = @import("vm.zig");
const expr_mod = @import("expr/mod.zig");
const ops = @import("engine/value_ops.zig");
const types = @import("engine/types.zig");
const runtime_mod = @import("engine/runtime.zig");
const result_utils = @import("engine/result_utils.zig");
const select_helpers = @import("engine/select_helpers.zig");
pub const RowSet = types.RowSet;
const Table = types.Table;
const EvalCtx = types.EvalCtx;
const SortRow = types.SortRow;
pub const QueryMetrics = runtime_mod.QueryMetrics;
const QueryRuntime = runtime_mod.QueryRuntime;
const DepthFilterPlan = union(enum) {
    expr: *expr_mod.Expr,
    eq_columns: struct {
        left_src: usize,
        left_col: usize,
        right_src: usize,
        right_col: usize,
    },
};
const SourceRef = struct {
    table: *const Table,
    table_name: []const u8,
    alias: ?[]const u8,
};
const ViewDef = struct {
    name: []const u8,
    select_sql: []const u8,
    columns: std.ArrayList([]const u8),

    fn deinit(self: *ViewDef, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.select_sql);
        for (self.columns.items) |col| allocator.free(col);
        self.columns.deinit(allocator);
    }
};

const TriggerDef = struct {
    name: []const u8,
    table_name: []const u8,
    timing: sql.TriggerTiming,
    event: sql.TriggerEvent,
    body_sql: []const u8,

    fn deinit(self: *TriggerDef, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.table_name);
        allocator.free(self.body_sql);
    }
};

pub const Error = error{
    OutOfMemory,
    TableAlreadyExists,
    UnknownTable,
    ColumnCountMismatch,
    UnknownColumn,
    InvalidSql,
    UnsupportedSql,
    InvalidLiteral,
    AggregateEmpty,
};

pub const ExecResult = struct {
    rows_affected: usize = 0,
};

pub const RowState = enum { row, done };

pub const Engine = struct {
    allocator: std.mem.Allocator,
    tables: std.StringHashMap(Table),
    views: std.StringHashMap(ViewDef),
    indexes: std.StringHashMap([]const u8),
    triggers: std.StringHashMap(TriggerDef),
    metrics_data: QueryMetrics,
    metrics_enabled: bool,

    pub fn init(allocator: std.mem.Allocator) Engine {
        return .{
            .allocator = allocator,
            .tables = std.StringHashMap(Table).init(allocator),
            .views = std.StringHashMap(ViewDef).init(allocator),
            .indexes = std.StringHashMap([]const u8).init(allocator),
            .triggers = std.StringHashMap(TriggerDef).init(allocator),
            .metrics_data = .{},
            .metrics_enabled = false,
        };
    }

    pub fn deinit(self: *Engine) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var table = entry.value_ptr.*;
            table.deinit(self.allocator);
        }
        self.tables.deinit();

        var view_it = self.views.iterator();
        while (view_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var view = entry.value_ptr.*;
            view.deinit(self.allocator);
        }
        self.views.deinit();

        var index_it = self.indexes.iterator();
        while (index_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.indexes.deinit();

        var trigger_it = self.triggers.iterator();
        while (trigger_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var trigger = entry.value_ptr.*;
            trigger.deinit(self.allocator);
        }
        self.triggers.deinit();
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
            .create_index => |create_index| {
                try self.handleCreateIndex(create_index);
                return .{ .rows_affected = 0 };
            },
            .create_view => |create_view| {
                try self.handleCreateView(create_view);
                return .{ .rows_affected = 0 };
            },
            .create_trigger => |create_trigger| {
                try self.handleCreateTrigger(create_trigger);
                return .{ .rows_affected = 0 };
            },
            .insert => |ins| {
                try self.handleInsert(ins);
                return .{ .rows_affected = 1 };
            },
            .update => |upd| {
                return .{ .rows_affected = try self.handleUpdate(upd) };
            },
            .drop_table => |drop_table| {
                try self.handleDropTable(drop_table);
                return .{ .rows_affected = 0 };
            },
            .drop_index => |drop_index| {
                try self.handleDropIndex(drop_index);
                return .{ .rows_affected = 0 };
            },
            .drop_trigger => |drop_trigger| {
                try self.handleDropTrigger(drop_trigger);
                return .{ .rows_affected = 0 };
            },
            .drop_view => |drop_view| {
                try self.handleDropView(drop_view);
                return .{ .rows_affected = 0 };
            },
            .reindex => |reindex| {
                try self.handleReindex(reindex);
                return .{ .rows_affected = 0 };
            },
            .select => return Error.UnsupportedSql,
            .compound_select => return Error.UnsupportedSql,
        }
    }

    pub fn query(self: *Engine, allocator: std.mem.Allocator, sql_text: []const u8) Error!RowSet {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);
        return self.queryParsedWithParent(allocator, stmt, null, null);
    }

    pub fn resetMetrics(self: *Engine) void {
        self.metrics_data = .{};
        self.metrics_enabled = true;
    }

    pub fn metrics(self: *const Engine) QueryMetrics {
        return self.metrics_data;
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
            .integer_affinity = std.ArrayList(bool).empty,
            .rows = std.ArrayList([]Value).empty,
            .primary_key_col = create.primary_key_col,
        };
        for (create.columns) |col| {
            try table.columns.append(self.allocator, try self.allocator.dupe(u8, col));
        }
        for (create.integer_affinity) |affinity| {
            try table.integer_affinity.append(self.allocator, affinity);
        }
        gop.value_ptr.* = table;
    }

    fn handleCreateIndex(self: *Engine, create: sql.CreateIndex) Error!void {
        if (self.tables.getPtr(create.table_name) == null) return Error.UnknownTable;
        const key = try self.allocator.dupe(u8, create.index_name);
        errdefer self.allocator.free(key);
        const gop = try self.indexes.getOrPut(key);
        if (gop.found_existing) return Error.TableAlreadyExists;
        gop.value_ptr.* = try self.allocator.dupe(u8, create.table_name);
    }

    fn handleCreateView(self: *Engine, create: sql.CreateView) Error!void {
        if (self.tables.contains(create.view_name) or self.views.contains(create.view_name)) return Error.TableAlreadyExists;
        const key = try self.allocator.dupe(u8, create.view_name);
        errdefer self.allocator.free(key);
        const gop = try self.views.getOrPut(key);
        if (gop.found_existing) return Error.TableAlreadyExists;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), create.select_sql) catch return Error.InvalidSql;
        if (stmt != .select and stmt != .compound_select) return Error.InvalidSql;

        var columns = std.ArrayList([]const u8).empty;
        errdefer {
            for (columns.items) |col| self.allocator.free(col);
            columns.deinit(self.allocator);
        }
        switch (stmt) {
            .select => |sel| {
                for (sel.projections) |projection| {
                    const trimmed = std.mem.trim(u8, projection, " \t\r\n");
                    if (std.mem.eql(u8, trimmed, "*")) return Error.UnsupportedSql;
                    const dot_star = select_helpers.parseQualifiedStar(trimmed);
                    if (dot_star != null) return Error.UnsupportedSql;
                    try columns.append(self.allocator, try self.allocator.dupe(u8, trimmed));
                }
            },
            .compound_select => return Error.UnsupportedSql,
            else => unreachable,
        }

        gop.value_ptr.* = .{
            .name = try self.allocator.dupe(u8, create.view_name),
            .select_sql = try self.allocator.dupe(u8, create.select_sql),
            .columns = columns,
        };
    }

    fn handleCreateTrigger(self: *Engine, create: sql.CreateTrigger) Error!void {
        if (self.tables.getPtr(create.table_name) == null) return Error.UnknownTable;
        const key = try self.allocator.dupe(u8, create.trigger_name);
        errdefer self.allocator.free(key);
        const gop = try self.triggers.getOrPut(key);
        if (gop.found_existing) return Error.TableAlreadyExists;
        gop.value_ptr.* = .{
            .name = try self.allocator.dupe(u8, create.trigger_name),
            .table_name = try self.allocator.dupe(u8, create.table_name),
            .timing = create.timing,
            .event = create.event,
            .body_sql = try self.allocator.dupe(u8, create.body_sql),
        };
    }

    fn handleInsert(self: *Engine, ins: sql.Insert) Error!void {
        const table = self.tables.getPtr(ins.table_name) orelse return Error.UnknownTable;
        if (ins.values) |values| {
            const row = try self.allocator.alloc(Value, table.columns.items.len);
            errdefer self.allocator.free(row);
            for (row) |*cell| cell.* = .null;

            if (ins.columns) |cols| {
                if (values.len != cols.len) return Error.ColumnCountMismatch;
                for (cols, values) |col_name, v| {
                    const idx = vm.columnIndex(table.columns.items, col_name) orelse return Error.UnknownColumn;
                    row[idx] = try self.coerceValueForColumn(table, idx, v);
                }
            } else {
                if (values.len != table.columns.items.len) return Error.ColumnCountMismatch;
                for (values, 0..) |v, i| row[i] = try self.coerceValueForColumn(table, i, v);
            }
            try self.storeInsertedRow(table, row, ins.or_replace);
            return;
        }

        const select_sql = ins.select_sql orelse return Error.InvalidSql;
        var rows = try self.query(self.allocator, select_sql);
        defer rows.deinit();

        const target_indexes = if (ins.columns) |cols| blk: {
            const idxs = try self.allocator.alloc(usize, cols.len);
            defer self.allocator.free(idxs);
            for (cols, 0..) |col_name, i| {
                idxs[i] = vm.columnIndex(table.columns.items, col_name) orelse return Error.UnknownColumn;
            }
            break :blk try self.allocator.dupe(usize, idxs);
        } else null;
        defer if (target_indexes) |idxs| self.allocator.free(idxs);

        for (rows.rows.items) |src_row| {
            const row = try self.allocator.alloc(Value, table.columns.items.len);
            errdefer self.allocator.free(row);
            for (row) |*cell| cell.* = .null;

            if (target_indexes) |idxs| {
                if (src_row.len != idxs.len) return Error.ColumnCountMismatch;
                for (src_row, 0..) |v, i| {
                    row[idxs[i]] = try self.coerceValueForColumn(table, idxs[i], v);
                }
            } else {
                if (src_row.len != table.columns.items.len) return Error.ColumnCountMismatch;
                for (src_row, 0..) |v, i| row[i] = try self.coerceValueForColumn(table, i, v);
            }
            try self.storeInsertedRow(table, row, ins.or_replace);
        }
    }

    fn storeInsertedRow(self: *Engine, table: *Table, row: []Value, or_replace: bool) Error!void {
        if (or_replace) {
            if (table.primary_key_col) |pk_idx| {
                const key = row[pk_idx];
                for (table.rows.items) |*existing_row| {
                    if (existing_row.*[pk_idx].eql(key)) {
                        result_utils.freeOwnedRow(self.allocator, existing_row.*);
                        existing_row.* = row;
                        return;
                    }
                }
            }
        }
        try table.rows.append(self.allocator, row);
    }

    fn coerceValueForColumn(self: *Engine, table: *const Table, col_idx: usize, value: Value) Error!Value {
        if (col_idx >= table.integer_affinity.items.len or !table.integer_affinity.items[col_idx]) {
            return value.clone(self.allocator);
        }
        return switch (value) {
            .real => |v| {
                if (@floor(v) == v and v >= @as(f64, @floatFromInt(std.math.minInt(i64))) and v <= @as(f64, @floatFromInt(std.math.maxInt(i64)))) {
                    return Value{ .integer = @as(i64, @intFromFloat(v)) };
                }
                return Value{ .real = v };
            },
            else => return value.clone(self.allocator),
        };
    }

    fn handleUpdate(self: *Engine, upd: sql.Update) Error!usize {
        const table = self.tables.getPtr(upd.table_name) orelse return Error.UnknownTable;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const ea = arena.allocator();

        const where_expr = if (upd.where_expr) |text|
            expr_mod.parse(ea, text) catch return Error.InvalidSql
        else
            null;

        const target_indexes = try self.allocator.alloc(usize, upd.assignments.len);
        defer self.allocator.free(target_indexes);
        const rhs_exprs = try self.allocator.alloc(*expr_mod.Expr, upd.assignments.len);
        defer self.allocator.free(rhs_exprs);

        for (upd.assignments, 0..) |assignment, i| {
            target_indexes[i] = vm.columnIndex(table.columns.items, assignment.column_name) orelse return Error.UnknownColumn;
            rhs_exprs[i] = expr_mod.parse(ea, assignment.expr) catch return Error.InvalidSql;
        }

        var runtime = QueryRuntime.init(self.allocator);
        defer runtime.deinit(self.allocator);

        var rows_affected: usize = 0;
        for (table.rows.items) |row| {
            var ctx = EvalCtx{
                .table = table,
                .table_name = upd.table_name,
                .alias = null,
                .row = row,
                .parent = null,
            };
            if (where_expr) |w| {
                const cond = try self.evalExpr(self.allocator, w, &ctx, &runtime);
                if (!ops.toSqlBool(cond)) continue;
            }

            const evaluated = try self.allocator.alloc(Value, upd.assignments.len);
            defer {
                for (evaluated) |value| switch (value) {
                    .text => |t| self.allocator.free(t),
                    .blob => |b| self.allocator.free(b),
                    else => {},
                };
                self.allocator.free(evaluated);
            }
            for (rhs_exprs, 0..) |rhs, i| {
                const value = try self.evalExpr(self.allocator, rhs, &ctx, &runtime);
                evaluated[i] = try result_utils.cloneResultValue(self.allocator, value);
            }

            for (evaluated, 0..) |value, i| {
                switch (row[target_indexes[i]]) {
                    .text => |t| self.allocator.free(t),
                    .blob => |b| self.allocator.free(b),
                    else => {},
                }
                row[target_indexes[i]] = try self.coerceValueForColumn(table, target_indexes[i], value);
                switch (value) {
                    .text => |t| self.allocator.free(t),
                    .blob => |b| self.allocator.free(b),
                    else => {},
                }
                evaluated[i] = .null;
            }
            rows_affected += 1;
        }

        return rows_affected;
    }

    fn handleDropTable(self: *Engine, drop_table: sql.DropObject) Error!void {
        const removed = self.tables.fetchRemove(drop_table.object_name);
        if (removed) |entry| {
            self.allocator.free(entry.key);
            var table = entry.value;
            table.deinit(self.allocator);

            var index_names = std.ArrayList([]const u8).empty;
            defer index_names.deinit(self.allocator);
            var it = self.indexes.iterator();
            while (it.next()) |index_entry| {
                if (std.mem.eql(u8, index_entry.value_ptr.*, drop_table.object_name)) {
                    try index_names.append(self.allocator, index_entry.key_ptr.*);
                }
            }
            for (index_names.items) |index_name| {
                try self.handleDropIndex(.{ .object_name = index_name, .if_exists = false });
            }

            var trigger_names = std.ArrayList([]const u8).empty;
            defer trigger_names.deinit(self.allocator);
            var trigger_it = self.triggers.iterator();
            while (trigger_it.next()) |trigger_entry| {
                if (std.mem.eql(u8, trigger_entry.value_ptr.table_name, drop_table.object_name)) {
                    try trigger_names.append(self.allocator, trigger_entry.key_ptr.*);
                }
            }
            for (trigger_names.items) |trigger_name| {
                try self.handleDropTrigger(.{ .object_name = trigger_name, .if_exists = false });
            }
            return;
        }
        if (drop_table.if_exists) return;
        return Error.UnknownTable;
    }

    fn handleDropIndex(self: *Engine, drop_index: sql.DropObject) Error!void {
        const removed = self.indexes.fetchRemove(drop_index.object_name);
        if (removed) |entry| {
            self.allocator.free(entry.key);
            self.allocator.free(entry.value);
            return;
        }
        if (drop_index.if_exists) return;
        return Error.UnknownTable;
    }

    fn handleDropTrigger(self: *Engine, drop_trigger: sql.DropObject) Error!void {
        const removed = self.triggers.fetchRemove(drop_trigger.object_name);
        if (removed) |entry| {
            self.allocator.free(entry.key);
            var trigger = entry.value;
            trigger.deinit(self.allocator);
            return;
        }
        if (drop_trigger.if_exists) return;
        return Error.UnknownTable;
    }

    fn handleDropView(self: *Engine, drop_view: sql.DropObject) Error!void {
        const removed = self.views.fetchRemove(drop_view.object_name);
        if (removed) |entry| {
            self.allocator.free(entry.key);
            var view = entry.value;
            view.deinit(self.allocator);
            return;
        }
        if (drop_view.if_exists) return;
        return Error.UnknownTable;
    }

    fn handleReindex(self: *Engine, reindex: sql.Reindex) Error!void {
        if (self.indexes.contains(reindex.target_name)) return;
        return Error.UnknownTable;
    }

    fn materializeView(
        self: *Engine,
        allocator: std.mem.Allocator,
        view: *const ViewDef,
        parent_ctx: ?*const EvalCtx,
        runtime: *QueryRuntime,
        temp_tables: *std.ArrayList(*Table),
    ) Error!*Table {
        var row_set = try self.queryTextWithParent(allocator, view.select_sql, parent_ctx, runtime);
        defer row_set.deinit();

        const temp_table = try allocator.create(Table);
        errdefer allocator.destroy(temp_table);
        temp_table.* = .{
            .name = try allocator.dupe(u8, view.name),
            .columns = std.ArrayList([]const u8).empty,
            .integer_affinity = std.ArrayList(bool).empty,
            .rows = std.ArrayList([]Value).empty,
            .primary_key_col = null,
        };
        errdefer temp_table.deinit(allocator);

        for (view.columns.items) |column_name| {
            try temp_table.columns.append(allocator, try allocator.dupe(u8, column_name));
            try temp_table.integer_affinity.append(allocator, false);
        }
        for (row_set.rows.items) |row| {
            const copied = try allocator.alloc(Value, row.len);
            for (row, 0..) |value, i| {
                copied[i] = try result_utils.cloneResultValue(allocator, value);
            }
            try temp_table.rows.append(allocator, copied);
        }
        try temp_tables.append(allocator, temp_table);
        return temp_table;
    }

    fn executeSelect(
        self: *Engine,
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
            const table = if (self.tables.getPtr(from_item.table_name)) |existing|
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
            const trimmed = std.mem.trim(u8, text, " \t\r\n");
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
                .rows = std.ArrayList([]Value).empty,
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

            const out_row = try allocator.alloc(Value, projections.items.len);
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
            if (@popCount(dep.mask) == 1) {
                continue;
            }
            var max_pos: usize = 0;
            for (sources.items, 0..) |_, src_idx| {
                if ((dep.mask & (@as(u64, 1) << @intCast(src_idx))) != 0) {
                    max_pos = @max(max_pos, source_to_iter_pos[src_idx]);
                }
            }
            try depth_filters[max_pos].append(allocator, try buildDepthFilterPlan(conjunct, sources.items));
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
                    for (sources.items) |src_ref| {
                        out_count += src_ref.table.columns.items.len;
                    }
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
            const parent = if (current_depth == 0)
                parent_ctx
            else
                &ctx_by_source[iter_order[current_depth - 1]];

            ctx_by_source[src_idx] = .{
                .table = source.table,
                .table_name = source.table_name,
                .alias = source.alias,
                .row = source.table.rows.items[candidate_rows[src_idx].items[idxs[src_idx]]],
                .parent = parent,
            };

            var depth_ok = true;
            for (depth_filters[current_depth].items) |filter_plan| {
                if (!try self.evalDepthFilterPlan(allocator, filter_plan, &ctx_by_source[src_idx], ctx_by_source, rt)) {
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

            const out_row = try allocator.alloc(Value, out_count);
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
                const keys = try allocator.alloc(Value, order_by_count);
                for (sel.order_by, 0..) |term, i| {
                    if (term.is_ordinal) {
                        if (term.ordinal == 0 or term.ordinal > out_row.len) return Error.InvalidSql;
                        keys[i] = try result_utils.cloneResultValue(allocator, out_row[term.ordinal - 1]);
                    } else {
                        const oexpr = order_exprs.items[i] orelse return Error.InvalidSql;
                        const kv = try self.evalExpr(allocator, oexpr, ctx, rt);
                        keys[i] = try result_utils.cloneResultValue(allocator, kv);
                    }
                }
                try rows.append(allocator, .{ .values = out_row, .keys = keys });
            }
            idxs[src_idx] += 1;
        }

        if (order_by_count > 0) {
            std.sort.heap(SortRow, rows.items, {}, result_utils.sortRowLessThan);
            for (rows.items) |entry| {
                try result.rows.append(allocator, entry.values);
            }
        }
        if (sel.distinct) {
            result_utils.dedupRowsInPlace(allocator, &result);
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
            try arm_sets.append(allocator, try self.queryTextWithParent(allocator, arm_sql, parent_ctx, null));
        }

        var groups = std.ArrayList(RowSet).empty;
        defer {
            for (groups.items) |*rs| rs.deinit();
            groups.deinit(allocator);
        }
        var group_ops = std.ArrayList(sql.SetOp).empty;
        defer group_ops.deinit(allocator);

        var current = try result_utils.cloneRowSet(allocator, &arm_sets.items[0]);
        var current_owned = true;
        defer {
            if (current_owned) current.deinit();
        }

        for (compound.ops, 0..) |set_op, idx| {
            const rhs = &arm_sets.items[idx + 1];
            if (set_op == .intersect) {
                const next = try result_utils.rowSetIntersect(allocator, &current, rhs);
                current.deinit();
                current = next;
                current_owned = true;
                continue;
            }

            try groups.append(allocator, current);
            current_owned = false;
            try group_ops.append(allocator, set_op);
            current = try result_utils.cloneRowSet(allocator, rhs);
            current_owned = true;
        }
        try groups.append(allocator, current);
        current_owned = false;

        if (groups.items.len == 0) return Error.InvalidSql;
        var out = try result_utils.cloneRowSet(allocator, &groups.items[0]);
        errdefer out.deinit();

        for (group_ops.items, 0..) |set_op, idx| {
            const rhs = &groups.items[idx + 1];
            switch (set_op) {
                .union_all => {
                    for (rhs.rows.items) |row| {
                        try result_utils.appendClonedRow(allocator, &out, row);
                    }
                },
                .union_distinct => {
                    result_utils.dedupRowsInPlace(allocator, &out);
                    for (rhs.rows.items) |row| {
                        try result_utils.appendDistinctRow(allocator, &out, row);
                    }
                },
                .except => {
                    const next = try result_utils.rowSetExcept(allocator, &out, rhs);
                    out.deinit();
                    out = next;
                },
                .intersect => {
                    const next = try result_utils.rowSetIntersect(allocator, &out, rhs);
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
                    keys[i] = try result_utils.cloneResultValue(allocator, row[term.ordinal - 1]);
                }
                try rows.append(allocator, .{ .values = row, .keys = keys });
            }

            std.sort.heap(SortRow, rows.items, {}, result_utils.sortRowLessThan);
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
        runtime: *QueryRuntime,
    ) Error![]Value {
        const row = try allocator.alloc(Value, projections.len);
        var seen_any = false;

        for (projections, 0..) |pexpr, i| {
            row[i] = try self.evalAggregateExpr(allocator, pexpr, table, sel, where_expr, parent_ctx, alias, runtime, &seen_any);
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
        runtime: *QueryRuntime,
        seen_any_ptr: *bool,
    ) Error!Value {
        switch (pexpr.*) {
            .call => |call| {
                if (call.star_arg and call.distinct) return Error.InvalidSql;
                if (call.star_arg and !ops.eqlIgnoreCase(call.name, "count")) return Error.InvalidSql;

                var distinct_values = std.ArrayList(Value).empty;
                defer {
                    for (distinct_values.items) |value| switch (value) {
                        .text => |t| allocator.free(t),
                        .blob => |b| allocator.free(b),
                        else => {},
                    };
                    distinct_values.deinit(allocator);
                }

                var count: i64 = 0;
                var sum: f64 = 0;
                var total: f64 = 0;
                var numeric_count: usize = 0;
                var int_sum: i64 = 0;
                var sum_all_integer = true;
                var sum_overflow = false;
                var concat_parts = std.ArrayList([]const u8).empty;
                defer {
                    for (concat_parts.items) |part| allocator.free(part);
                    concat_parts.deinit(allocator);
                }
                var min_value: ?f64 = null;
                var max_value: ?f64 = null;

                for (table.rows.items) |source_row| {
                    var ctx = EvalCtx{
                        .table = table,
                        .table_name = sel.from[0].table_name,
                        .alias = alias,
                        .row = source_row,
                        .parent = parent_ctx,
                    };
                    if (where_expr) |w| {
                        const cond = try self.evalExpr(allocator, w, &ctx, runtime);
                        if (!ops.toSqlBool(cond)) continue;
                    }

                    if (call.star_arg) {
                        count += 1;
                        seen_any_ptr.* = true;
                        continue;
                    }
                    if (call.args.len == 0 or call.args.len > 2) return Error.InvalidSql;

                    const value = try self.evalExpr(allocator, call.args[0], &ctx, runtime);
                    if (value == .null) continue;

                    if (call.distinct) {
                        var found = false;
                        for (distinct_values.items) |existing| {
                            if (existing.eql(value)) {
                                found = true;
                                break;
                            }
                        }
                        if (found) continue;
                        try distinct_values.append(allocator, try result_utils.cloneResultValue(allocator, value));
                    }

                    if (ops.eqlIgnoreCase(call.name, "count")) {
                        count += 1;
                        seen_any_ptr.* = true;
                        continue;
                    }

                    if (ops.eqlIgnoreCase(call.name, "group_concat")) {
                        if (call.distinct and call.args.len != 1) return Error.InvalidSql;
                        const text = try aggregateValueText(allocator, value);
                        try concat_parts.append(allocator, text);
                        seen_any_ptr.* = true;
                        continue;
                    }

                    const number = ops.toNumber(value) orelse 0;
                    if (ops.eqlIgnoreCase(call.name, "avg")) {
                        sum += number;
                        numeric_count += 1;
                        seen_any_ptr.* = true;
                        continue;
                    }
                    if (ops.eqlIgnoreCase(call.name, "sum")) {
                        sum += number;
                        numeric_count += 1;
                        if ((value == .integer or (value == .real and @floor(value.real) == value.real)) and sum_all_integer and !sum_overflow) {
                            const int_value = if (value == .integer) value.integer else @as(i64, @intFromFloat(value.real));
                            int_sum = std.math.add(i64, int_sum, int_value) catch blk: {
                                sum_overflow = true;
                                break :blk int_sum;
                            };
                        } else {
                            sum_all_integer = false;
                        }
                        seen_any_ptr.* = true;
                        continue;
                    }
                    if (ops.eqlIgnoreCase(call.name, "total")) {
                        total += number;
                        seen_any_ptr.* = true;
                        continue;
                    }
                    if (ops.eqlIgnoreCase(call.name, "min")) {
                        if (min_value == null or number < min_value.?) min_value = number;
                        seen_any_ptr.* = true;
                        continue;
                    }
                    if (ops.eqlIgnoreCase(call.name, "max")) {
                        if (max_value == null or number > max_value.?) max_value = number;
                        seen_any_ptr.* = true;
                        continue;
                    }
                    return Error.UnsupportedSql;
                }

                if (ops.eqlIgnoreCase(call.name, "count")) {
                    return Value{ .integer = count };
                }
                if (ops.eqlIgnoreCase(call.name, "avg")) {
                    if (numeric_count == 0) return .null;
                    return Value{ .real = sum / @as(f64, @floatFromInt(numeric_count)) };
                }
                if (ops.eqlIgnoreCase(call.name, "sum")) {
                    if (numeric_count == 0) return .null;
                    if (sum_all_integer and sum_overflow) return Error.AggregateEmpty;
                    if (sum < @as(f64, @floatFromInt(std.math.minInt(i64))) or sum > @as(f64, @floatFromInt(std.math.maxInt(i64)))) {
                        return Error.AggregateEmpty;
                    }
                    if (sum_all_integer) return Value{ .integer = int_sum };
                    if (@floor(sum) == sum) return Value{ .integer = @as(i64, @intFromFloat(sum)) };
                    return Value{ .real = sum };
                }
                if (ops.eqlIgnoreCase(call.name, "total")) {
                    return Value{ .real = total };
                }
                if (ops.eqlIgnoreCase(call.name, "min")) {
                    if (min_value == null) return .null;
                    if (@floor(min_value.?) == min_value.?) return Value{ .integer = @as(i64, @intFromFloat(min_value.?)) };
                    return Value{ .real = min_value.? };
                }
                if (ops.eqlIgnoreCase(call.name, "max")) {
                    if (max_value == null) return .null;
                    if (@floor(max_value.?) == max_value.?) return Value{ .integer = @as(i64, @intFromFloat(max_value.?)) };
                    return Value{ .real = max_value.? };
                }
                if (ops.eqlIgnoreCase(call.name, "group_concat")) {
                    if (concat_parts.items.len == 0) return .null;
                    const separator = if (call.args.len == 2) blk: {
                        const sep_value = try self.evalExpr(allocator, call.args[1], &EvalCtx{
                            .table = table,
                            .table_name = sel.from[0].table_name,
                            .alias = alias,
                            .row = null,
                            .parent = parent_ctx,
                        }, runtime);
                        break :blk try aggregateValueText(allocator, sep_value);
                    } else try allocator.dupe(u8, ",");
                    defer allocator.free(separator);

                    var joined = std.ArrayList(u8).empty;
                    defer joined.deinit(allocator);
                    for (concat_parts.items, 0..) |part, i| {
                        if (i != 0) try joined.appendSlice(allocator, separator);
                        try joined.appendSlice(allocator, part);
                    }
                    return Value{ .text = try joined.toOwnedSlice(allocator) };
                }
                return Error.UnsupportedSql;
            },
            else => {
                if (!seen_any_ptr.*) return .null;
                return Error.UnsupportedSql;
            },
        }
    }

    fn buildDepthFilterPlan(conjunct: *expr_mod.Expr, sources: []const SourceRef) Error!DepthFilterPlan {
        switch (conjunct.*) {
            .binary => |b| {
                if (b.op != .eq) return .{ .expr = conjunct };
                const left_id = switch (b.left.*) {
                    .ident => |id| id,
                    else => return .{ .expr = conjunct },
                };
                const right_id = switch (b.right.*) {
                    .ident => |id| id,
                    else => return .{ .expr = conjunct },
                };
                const left_src = try select_helpers.resolveIdentifierSourceIndex(sources, left_id.qualifier, left_id.name, ops.eqlIgnoreCase);
                const right_src = try select_helpers.resolveIdentifierSourceIndex(sources, right_id.qualifier, right_id.name, ops.eqlIgnoreCase);
                if (left_src == right_src) return .{ .expr = conjunct };
                const left_col = vm.columnIndex(sources[left_src].table.columns.items, left_id.name) orelse return Error.UnknownColumn;
                const right_col = vm.columnIndex(sources[right_src].table.columns.items, right_id.name) orelse return Error.UnknownColumn;
                return .{
                    .eq_columns = .{
                        .left_src = left_src,
                        .left_col = left_col,
                        .right_src = right_src,
                        .right_col = right_col,
                    },
                };
            },
            else => return .{ .expr = conjunct },
        }
    }

    fn evalDepthFilterPlan(
        self: *Engine,
        allocator: std.mem.Allocator,
        plan: DepthFilterPlan,
        ctx: *const EvalCtx,
        ctx_by_source: []const EvalCtx,
        runtime: *QueryRuntime,
    ) Error!bool {
        switch (plan) {
            .expr => |node| {
                const cond_val = try self.evalExpr(allocator, node, ctx, runtime);
                return ops.toSqlBool(cond_val);
            },
            .eq_columns => |eq| {
                if (self.metrics_enabled) self.metrics_data.eval_expr_calls += 1;
                const left_row = ctx_by_source[eq.left_src].row orelse return false;
                const right_row = ctx_by_source[eq.right_src].row orelse return false;
                const left = left_row[eq.left_col];
                const right = right_row[eq.right_col];
                if (left == .null or right == .null) return false;
                return ops.compareValues(left, right) == 0;
            },
        }
    }

    fn aggregateValueText(allocator: std.mem.Allocator, value: Value) Error![]const u8 {
        return switch (value) {
            .null => try allocator.dupe(u8, "NULL"),
            .integer => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
            .real => |v| try std.fmt.allocPrint(allocator, "{d:.3}", .{v}),
            .text => |v| try allocator.dupe(u8, v),
            .blob => try allocator.dupe(u8, "BLOB"),
        };
    }

    fn evalExpr(self: *Engine, allocator: std.mem.Allocator, node: *expr_mod.Expr, ctx: *const EvalCtx, runtime: *QueryRuntime) Error!Value {
        if (self.metrics_enabled) self.metrics_data.eval_expr_calls += 1;
        switch (node.*) {
            .literal => |v| return v,
            .ident => |id| return self.resolveIdentifier(ctx, id.qualifier, id.name),
            .unary => |u| {
                const inner = try self.evalExpr(allocator, u.expr, ctx, runtime);
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
                if (b.op == .and_op) {
                    const l = try self.evalExpr(allocator, b.left, ctx, runtime);
                    if (!ops.toSqlBool(l)) return Value{ .integer = 0 };
                    const r = try self.evalExpr(allocator, b.right, ctx, runtime);
                    return Value{ .integer = if (ops.toSqlBool(r)) 1 else 0 };
                }
                if (b.op == .or_op) {
                    const l = try self.evalExpr(allocator, b.left, ctx, runtime);
                    if (ops.toSqlBool(l)) return Value{ .integer = 1 };
                    const r = try self.evalExpr(allocator, b.right, ctx, runtime);
                    return Value{ .integer = if (ops.toSqlBool(r)) 1 else 0 };
                }
                const l = try self.evalExpr(allocator, b.left, ctx, runtime);
                const r = try self.evalExpr(allocator, b.right, ctx, runtime);
                return ops.evalBinary(b.op, l, r);
            },
            .between => |b| {
                const t = try self.evalExpr(allocator, b.target, ctx, runtime);
                const lo = try self.evalExpr(allocator, b.low, ctx, runtime);
                const hi = try self.evalExpr(allocator, b.high, ctx, runtime);
                if (t == .null or lo == .null or hi == .null) return .null;
                const left_cmp = ops.compareValues(t, lo);
                const right_cmp = ops.compareValues(t, hi);
                var ok = left_cmp >= 0 and right_cmp <= 0;
                if (b.not_between) ok = !ok;
                return Value{ .integer = if (ok) 1 else 0 };
            },
            .is_null => |n| {
                const v = try self.evalExpr(allocator, n.target, ctx, runtime);
                const is_null = v == .null;
                const out = if (n.not_null) !is_null else is_null;
                return Value{ .integer = if (out) 1 else 0 };
            },
            .in_list => |n| {
                const target = try self.evalExpr(allocator, n.target, ctx, runtime);
                if (n.subquery) |sql_text| {
                    var rs = try self.queryTextWithParent(allocator, sql_text, ctx, runtime);
                    defer rs.deinit();
                    if (rs.rows.items.len == 0) {
                        return Value{ .integer = if (n.not_in) 1 else 0 };
                    }
                    if (target == .null) return .null;

                    var has_null = false;
                    for (rs.rows.items) |row| {
                        if (row.len == 0) continue;
                        const rhs = row[0];
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
                }
                if (n.items.len == 0) {
                    return Value{ .integer = if (n.not_in) 1 else 0 };
                }
                if (target == .null) return .null;

                var has_null = false;
                for (n.items) |item| {
                    const rhs = try self.evalExpr(allocator, item, ctx, runtime);
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
                    const v = try self.evalExpr(allocator, call.args[0], ctx, runtime);
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
                        const v = try self.evalExpr(allocator, arg, ctx, runtime);
                        if (v != .null) return v;
                    }
                    return .null;
                }
                return Error.UnsupportedSql;
            },
            .case_expr => |c| {
                if (c.base) |base_expr| {
                    const base = try self.evalExpr(allocator, base_expr, ctx, runtime);
                    for (c.whens) |w| {
                        const when_val = try self.evalExpr(allocator, w.cond, ctx, runtime);
                        const cmp = ops.evalBinary(.eq, base, when_val);
                        if (ops.toSqlBool(cmp)) return self.evalExpr(allocator, w.value, ctx, runtime);
                    }
                } else {
                    for (c.whens) |w| {
                        const cond = try self.evalExpr(allocator, w.cond, ctx, runtime);
                        if (ops.toSqlBool(cond)) return self.evalExpr(allocator, w.value, ctx, runtime);
                    }
                }
                if (c.else_expr) |e| return self.evalExpr(allocator, e, ctx, runtime);
                return .null;
            },
            .subquery => |sql_text| {
                const parsed_stmt = try self.getParsedSubquery(runtime, sql_text);
                const mode_ptr = blk: {
                    if (runtime.scalar_mode.getPtr(sql_text)) |existing| break :blk existing;
                    const owned = try allocator.dupe(u8, sql_text);
                    const gop = try runtime.scalar_mode.getOrPut(owned);
                    if (gop.found_existing) {
                        allocator.free(owned);
                    } else {
                        gop.value_ptr.* = .unknown;
                    }
                    break :blk gop.value_ptr;
                };

                if (mode_ptr.* == .uncorrelated) {
                    if (runtime.scalar_cache.get(sql_text)) |cached| {
                        if (self.metrics_enabled) self.metrics_data.subquery_cache_hits += 1;
                        return cached;
                    }
                    return Error.InvalidSql;
                }

                if (mode_ptr.* == .unknown) {
                    var uncorr_result = self.queryParsedWithParent(allocator, parsed_stmt, null, runtime);
                    if (uncorr_result) |*rs_uncorr| {
                        defer rs_uncorr.deinit();
                        if (self.metrics_enabled) self.metrics_data.subquery_exec_calls += 1;
                        var cached_value: Value = .null;
                        if (rs_uncorr.rows.items.len != 0 and rs_uncorr.rows.items[0].len != 0) {
                            cached_value = try result_utils.cloneResultValue(allocator, rs_uncorr.rows.items[0][0]);
                        }
                        const owned = try allocator.dupe(u8, sql_text);
                        const cache_gop = try runtime.scalar_cache.getOrPut(owned);
                        if (cache_gop.found_existing) {
                            allocator.free(owned);
                            switch (cache_gop.value_ptr.*) {
                                .text => |t| allocator.free(t),
                                .blob => |b| allocator.free(b),
                                else => {},
                            }
                        }
                        cache_gop.value_ptr.* = cached_value;
                        mode_ptr.* = .uncorrelated;
                        return cached_value;
                    } else |uncorr_err| {
                        if (uncorr_err != Error.UnknownColumn) return uncorr_err;
                        mode_ptr.* = .correlated;
                    }
                }

                var rs = try self.queryParsedWithParent(allocator, parsed_stmt, ctx, runtime);
                defer rs.deinit();
                if (self.metrics_enabled) self.metrics_data.subquery_exec_calls += 1;
                if (rs.rows.items.len == 0 or rs.rows.items[0].len == 0) return .null;
                return rs.rows.items[0][0];
            },
            .exists_subquery => |sql_text| {
                const parsed_stmt = try self.getParsedSubquery(runtime, sql_text);
                const mode_ptr = blk: {
                    if (runtime.exists_mode.getPtr(sql_text)) |existing| break :blk existing;
                    const owned = try allocator.dupe(u8, sql_text);
                    const gop = try runtime.exists_mode.getOrPut(owned);
                    if (gop.found_existing) {
                        allocator.free(owned);
                    } else {
                        gop.value_ptr.* = .unknown;
                    }
                    break :blk gop.value_ptr;
                };

                if (mode_ptr.* == .uncorrelated) {
                    if (runtime.exists_cache.get(sql_text)) |cached| {
                        if (self.metrics_enabled) self.metrics_data.subquery_cache_hits += 1;
                        return cached;
                    }
                    return Error.InvalidSql;
                }

                if (mode_ptr.* == .unknown) {
                    var uncorr_result = self.queryParsedWithParent(allocator, parsed_stmt, null, runtime);
                    if (uncorr_result) |*rs_uncorr| {
                        defer rs_uncorr.deinit();
                        if (self.metrics_enabled) self.metrics_data.subquery_exec_calls += 1;
                        const cached_value = Value{ .integer = if (rs_uncorr.rows.items.len > 0) 1 else 0 };
                        const owned = try allocator.dupe(u8, sql_text);
                        const cache_gop = try runtime.exists_cache.getOrPut(owned);
                        if (cache_gop.found_existing) {
                            allocator.free(owned);
                            switch (cache_gop.value_ptr.*) {
                                .text => |t| allocator.free(t),
                                .blob => |b| allocator.free(b),
                                else => {},
                            }
                        }
                        cache_gop.value_ptr.* = cached_value;
                        mode_ptr.* = .uncorrelated;
                        return cached_value;
                    } else |uncorr_err| {
                        if (uncorr_err != Error.UnknownColumn) return uncorr_err;
                        mode_ptr.* = .correlated;
                    }
                }

                var rs = try self.queryParsedWithParent(allocator, parsed_stmt, ctx, runtime);
                defer rs.deinit();
                if (self.metrics_enabled) self.metrics_data.subquery_exec_calls += 1;
                return Value{ .integer = if (rs.rows.items.len > 0) 1 else 0 };
            },
        }
    }

    fn queryWithParent(self: *Engine, allocator: std.mem.Allocator, sql_text: []const u8, parent: *const EvalCtx) Error!RowSet {
        return self.queryTextWithParent(allocator, sql_text, parent, null);
    }

    fn queryTextWithParent(
        self: *Engine,
        allocator: std.mem.Allocator,
        sql_text: []const u8,
        parent: ?*const EvalCtx,
        runtime: ?*QueryRuntime,
    ) Error!RowSet {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);
        return self.queryParsedWithParent(allocator, stmt, parent, runtime);
    }

    fn queryParsedWithParent(
        self: *Engine,
        allocator: std.mem.Allocator,
        stmt: sql.Statement,
        parent: ?*const EvalCtx,
        runtime: ?*QueryRuntime,
    ) Error!RowSet {
        return switch (stmt) {
            .select => |sel| try self.executeSelect(allocator, sel, parent, runtime),
            .compound_select => |compound| try self.executeCompoundSelect(allocator, compound, parent),
            else => Error.InvalidSql,
        };
    }

    fn getParsedSubquery(_: *Engine, runtime: *QueryRuntime, sql_text: []const u8) Error!sql.Statement {
        if (runtime.parsed_subquery.get(sql_text)) |stmt| return stmt;

        const arena_allocator = runtime.parse_arena.allocator();
        const parsed = sql.parse(arena_allocator, sql_text) catch |err| return mapParseError(err);
        const owned_key = try arena_allocator.dupe(u8, sql_text);
        try runtime.parsed_subquery.put(owned_key, parsed);
        return parsed;
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
