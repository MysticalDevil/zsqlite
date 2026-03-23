const std = @import("std");
const Value = @import("../value.zig").Value;
const sql = @import("../sql.zig");
const vm = @import("../vm.zig");
const expr_mod = @import("../expr/mod.zig");
const result_utils = @import("result_utils.zig");
const select_helpers = @import("select_helpers.zig");
const shared = @import("shared.zig");

const Table = shared.Table;
const EvalCtx = shared.EvalCtx;
const ViewDef = shared.ViewDef;
const QueryRuntime = shared.QueryRuntime;
const Error = shared.Error;
const IndexDef = shared.IndexDef;

pub fn deinitEngine(self: anytype) void {
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
        var index_def = entry.value_ptr.*;
        index_def.deinit(self.allocator);
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

pub fn handleCreate(self: anytype, create: sql.CreateTable) Error!void {
    const key = try self.allocator.dupe(u8, create.table_name);
    errdefer self.allocator.free(key);
    const gop = try self.tables.getOrPut(key);
    if (gop.found_existing) return Error.TableAlreadyExists;

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

pub fn handleCreateIndex(self: anytype, create: sql.CreateIndex) Error!void {
    if (self.tables.getPtr(create.table_name) == null) return Error.UnknownTable;
    const key = try self.allocator.dupe(u8, create.index_name);
    errdefer self.allocator.free(key);
    const gop = try self.indexes.getOrPut(key);
    if (gop.found_existing) return Error.TableAlreadyExists;

    var columns = std.ArrayList(sql.IndexColumn).empty;
    errdefer {
        for (columns.items) |col| self.allocator.free(col.column_name);
        columns.deinit(self.allocator);
    }
    for (create.columns) |col| {
        try columns.append(self.allocator, .{
            .column_name = try self.allocator.dupe(u8, col.column_name),
            .descending = col.descending,
        });
    }

    gop.value_ptr.* = IndexDef{
        .name = try self.allocator.dupe(u8, create.index_name),
        .table_name = try self.allocator.dupe(u8, create.table_name),
        .unique = create.unique,
        .columns = columns,
    };
}

pub fn handleCreateView(self: anytype, create: sql.CreateView) Error!void {
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
                if (select_helpers.parseQualifiedStar(trimmed) != null) return Error.UnsupportedSql;
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

pub fn handleCreateTrigger(self: anytype, create: sql.CreateTrigger) Error!void {
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

pub fn handleInsert(self: anytype, ins: sql.Insert) Error!void {
    const table = self.tables.getPtr(ins.table_name) orelse return Error.UnknownTable;
    if (ins.values) |values| {
        const row = try self.allocator.alloc(Value, table.columns.items.len);
        errdefer self.allocator.free(row);
        for (row) |*cell| cell.* = .null;

        if (ins.columns) |cols| {
            if (values.len != cols.len) return Error.ColumnCountMismatch;
            for (cols, values) |col_name, v| {
                const idx = vm.columnIndex(table.columns.items, col_name) orelse return Error.UnknownColumn;
                row[idx] = try coerceValueForColumn(self, table, idx, v);
            }
        } else {
            if (values.len != table.columns.items.len) return Error.ColumnCountMismatch;
            for (values, 0..) |v, i| row[i] = try coerceValueForColumn(self, table, i, v);
        }
        try storeInsertedRow(self, table, row, ins.or_replace);
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
                row[idxs[i]] = try coerceValueForColumn(self, table, idxs[i], v);
            }
        } else {
            if (src_row.len != table.columns.items.len) return Error.ColumnCountMismatch;
            for (src_row, 0..) |v, i| row[i] = try coerceValueForColumn(self, table, i, v);
        }
        try storeInsertedRow(self, table, row, ins.or_replace);
    }
}

pub fn handleUpdate(self: anytype, upd: sql.Update) Error!usize {
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
            if (!@import("value_ops.zig").toSqlBool(cond)) continue;
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
            row[target_indexes[i]] = try coerceValueForColumn(self, table, target_indexes[i], value);
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

pub fn handleDelete(self: anytype, del: sql.Delete) Error!usize {
    const table = self.tables.getPtr(del.table_name) orelse return Error.UnknownTable;

    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const ea = arena.allocator();

    const where_expr = if (del.where_expr) |text|
        expr_mod.parse(ea, text) catch return Error.InvalidSql
    else
        null;

    var runtime = QueryRuntime.init(self.allocator);
    defer runtime.deinit(self.allocator);

    var rows_affected: usize = 0;
    var row_index: usize = 0;
    while (row_index < table.rows.items.len) {
        const row = table.rows.items[row_index];
        var ctx = EvalCtx{
            .table = table,
            .table_name = del.table_name,
            .alias = null,
            .row = row,
            .parent = null,
        };

        if (where_expr) |w| {
            const cond = try self.evalExpr(self.allocator, w, &ctx, &runtime);
            if (!@import("value_ops.zig").toSqlBool(cond)) {
                row_index += 1;
                continue;
            }
        }

        const removed = table.rows.orderedRemove(row_index);
        result_utils.freeOwnedRow(self.allocator, removed);
        rows_affected += 1;
    }

    return rows_affected;
}

pub fn handleDropTable(self: anytype, drop_table: sql.DropObject) Error!void {
    const removed = self.tables.fetchRemove(drop_table.object_name);
    if (removed) |entry| {
        self.allocator.free(entry.key);
        var table = entry.value;
        table.deinit(self.allocator);

        var index_names = std.ArrayList([]const u8).empty;
        defer index_names.deinit(self.allocator);
        var it = self.indexes.iterator();
        while (it.next()) |index_entry| {
            if (std.mem.eql(u8, index_entry.value_ptr.table_name, drop_table.object_name)) {
                try index_names.append(self.allocator, index_entry.key_ptr.*);
            }
        }
        for (index_names.items) |index_name| {
            try handleDropIndex(self, .{ .object_name = index_name, .if_exists = false });
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
            try handleDropTrigger(self, .{ .object_name = trigger_name, .if_exists = false });
        }
        return;
    }
    if (drop_table.if_exists) return;
    return Error.UnknownTable;
}

pub fn handleDropIndex(self: anytype, drop_index: sql.DropObject) Error!void {
    const removed = self.indexes.fetchRemove(drop_index.object_name);
    if (removed) |entry| {
        self.allocator.free(entry.key);
        var index_def = entry.value;
        index_def.deinit(self.allocator);
        return;
    }
    if (drop_index.if_exists) return;
    return Error.UnknownTable;
}

pub fn handleDropTrigger(self: anytype, drop_trigger: sql.DropObject) Error!void {
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

pub fn handleDropView(self: anytype, drop_view: sql.DropObject) Error!void {
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

pub fn handleReindex(self: anytype, reindex: sql.Reindex) Error!void {
    if (self.indexes.contains(reindex.target_name)) return;
    return Error.UnknownTable;
}

pub fn materializeView(
    self: anytype,
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

fn storeInsertedRow(self: anytype, table: *Table, row: []Value, or_replace: bool) Error!void {
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

fn coerceValueForColumn(self: anytype, table: *const Table, col_idx: usize, value: Value) Error!Value {
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
