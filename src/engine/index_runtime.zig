const std = @import("std");
const Value = @import("../value.zig").Value;
const shared = @import("shared.zig");
const expr_mod = @import("../expr/mod.zig");
const ops = @import("value_ops.zig");
const vm = @import("../vm.zig");

const Table = shared.Table;
const SourceRef = shared.SourceRef;
const IndexDef = shared.IndexDef;
const Error = shared.Error;

pub const IndexScanPlan = struct {
    row_ids: []usize,
};

pub fn initializeIndex(self: anytype, index_def: *IndexDef, table: *const Table) Error!void {
    if (index_def.columns.items.len != 1) {
        index_def.single_column_idx = null;
        return;
    }

    const column_name = index_def.columns.items[0].column_name;
    index_def.single_column_idx = vm.columnIndex(table.columns.items, column_name) orelse return Error.UnknownColumn;

    for (table.rows.items, 0..) |row, row_id| {
        if (!table.isRowLive(row_id)) continue;
        try addRowToIndex(self.allocator, index_def, row[index_def.single_column_idx.?], row_id);
    }
}

pub fn addRowToIndexes(self: anytype, table_name: []const u8, row: []const Value, row_id: usize) Error!void {
    var index_it = self.indexes.iterator();
    while (index_it.next()) |entry| {
        const index_def = entry.value_ptr;
        if (!std.mem.eql(u8, index_def.table_name, table_name)) continue;
        if (index_def.single_column_idx == null) continue;
        try addRowToIndex(self.allocator, index_def, row[index_def.single_column_idx.?], row_id);
    }
}

pub fn removeRowFromIndexes(self: anytype, table_name: []const u8, row: []const Value, row_id: usize) void {
    var index_it = self.indexes.iterator();
    while (index_it.next()) |entry| {
        const index_def = entry.value_ptr;
        if (!std.mem.eql(u8, index_def.table_name, table_name)) continue;
        if (index_def.single_column_idx == null) continue;
        removeRowFromIndex(self.allocator, index_def, row[index_def.single_column_idx.?], row_id);
    }
}

pub fn checkInsertConflicts(
    self: anytype,
    table_name: []const u8,
    table: *const Table,
    row: []const Value,
    target_row_id: ?usize,
    or_replace: bool,
) Error!void {
    var conflicts = std.ArrayList(usize).empty;
    defer conflicts.deinit(self.allocator);

    var index_it = self.indexes.iterator();
    while (index_it.next()) |entry| {
        const index_def = entry.value_ptr;
        if (!std.mem.eql(u8, index_def.table_name, table_name)) continue;
        if (!index_def.unique or index_def.single_column_idx == null) continue;

        const key_value = row[index_def.single_column_idx.?];
        if (key_value == .null) continue;

        const row_ids = try lookupRowIds(self.allocator, index_def, key_value);
        defer if (row_ids.owned) self.allocator.free(row_ids.items);

        for (row_ids.items) |row_id| {
            if (!table.isRowLive(row_id)) continue;
            if (target_row_id != null and row_id == target_row_id.?) continue;
            if (containsUsize(conflicts.items, row_id)) continue;
            if (!or_replace) return Error.TableAlreadyExists;
            try conflicts.append(self.allocator, row_id);
        }
    }

    for (conflicts.items) |row_id| {
        try tombstoneRow(self, table_name, row_id);
    }
}

pub fn tombstoneRow(self: anytype, table_name: []const u8, row_id: usize) Error!void {
    const table = self.tables.getPtr(table_name) orelse return Error.UnknownTable;
    if (!table.isRowLive(row_id)) return;

    removeRowFromIndexes(self, table_name, table.rows.items[row_id], row_id);
    table.row_states.items[row_id] = .tombstone;
}

pub fn tryPlanIndexScan(
    self: anytype,
    allocator: std.mem.Allocator,
    source: SourceRef,
    filters: []const *expr_mod.Expr,
) Error!?IndexScanPlan {
    switch (source.index_hint) {
        .not_indexed => return null,
        .indexed_by => |index_name| {
            const index_def = self.indexes.getPtr(index_name) orelse return Error.UnknownTable;
            if (!std.mem.eql(u8, index_def.table_name, source.table_name)) return Error.UnknownTable;
            var combined: ?[]usize = null;
            var used_index = false;

            for (filters) |filter_expr| {
                const plan_rows = try buildPlanForExpr(self, allocator, source, filter_expr, index_def);
                if (plan_rows) |rows| {
                    used_index = true;
                    if (combined == null) {
                        combined = rows;
                    } else {
                        const next = try intersectRowIds(allocator, combined.?, rows);
                        allocator.free(combined.?);
                        allocator.free(rows);
                        combined = next;
                    }
                }
            }

            if (!used_index) return Error.UnsupportedSql;
            return .{ .row_ids = combined orelse try allocator.alloc(usize, 0) };
        },
        .none => {},
    }

    var combined: ?[]usize = null;

    for (filters) |filter_expr| {
        const plan_rows = try buildPlanForExpr(self, allocator, source, filter_expr, null);
        if (plan_rows) |rows| {
            if (combined == null) {
                combined = rows;
            } else {
                const next = try intersectRowIds(allocator, combined.?, rows);
                allocator.free(combined.?);
                allocator.free(rows);
                combined = next;
            }
        }
    }

    if (combined) |rows| return .{ .row_ids = rows };
    return null;
}

fn buildPlanForExpr(
    self: anytype,
    allocator: std.mem.Allocator,
    source: SourceRef,
    node: *expr_mod.Expr,
    forced_index: ?*const IndexDef,
) Error!?[]usize {
    if (node.* == .binary and node.binary.op == .and_op) {
        const left = try buildPlanForExpr(self, allocator, source, node.binary.left, forced_index);
        const right = try buildPlanForExpr(self, allocator, source, node.binary.right, forced_index);
        if (left == null) return right;
        if (right == null) return left;
        const rows = try intersectRowIds(allocator, left.?, right.?);
        allocator.free(left.?);
        allocator.free(right.?);
        return rows;
    }
    if (node.* == .binary and node.binary.op == .or_op) {
        const left = try buildPlanForExpr(self, allocator, source, node.binary.left, forced_index);
        const right = try buildPlanForExpr(self, allocator, source, node.binary.right, forced_index);
        if (left == null) {
            if (right) |rows| allocator.free(rows);
            return null;
        }
        if (right == null) {
            allocator.free(left.?);
            return null;
        }
        const rows = try unionRowIds(allocator, left.?, right.?);
        allocator.free(left.?);
        allocator.free(right.?);
        return rows;
    }

    if (try extractEqPredicate(source, node)) |predicate| {
        return lookupBestIndexRows(self, allocator, source, predicate.column_name, &[_]Value{predicate.value}, forced_index);
    }
    if (try extractIsNullPredicate(source, node)) |column_name| {
        return lookupBestIndexRows(self, allocator, source, column_name, &[_]Value{.null}, forced_index);
    }
    if (try extractInPredicate(allocator, source, node)) |predicate| {
        defer allocator.free(predicate.values);
        return lookupBestIndexRows(self, allocator, source, predicate.column_name, predicate.values, forced_index);
    }
    return null;
}

fn lookupBestIndexRows(
    self: anytype,
    allocator: std.mem.Allocator,
    source: SourceRef,
    column_name: []const u8,
    values: []const Value,
    forced_index: ?*const IndexDef,
) Error!?[]usize {
    if (forced_index) |index_def| {
        if (!indexMatchesColumn(index_def, column_name)) return null;
        return try lookupRowsForValues(allocator, index_def, values);
    }

    var best_rows: ?[]usize = null;
    var best_len: usize = 0;
    var index_it = self.indexes.iterator();
    while (index_it.next()) |entry| {
        const index_def = entry.value_ptr;
        if (!std.mem.eql(u8, index_def.table_name, source.table_name)) continue;
        if (!indexMatchesColumn(index_def, column_name)) continue;

        const rows = try lookupRowsForValues(allocator, index_def, values);
        if (best_rows == null or rows.len < best_len) {
            if (best_rows != null) allocator.free(best_rows.?);
            best_rows = rows;
            best_len = rows.len;
        } else {
            allocator.free(rows);
        }
    }
    return best_rows;
}

fn lookupRowsForValues(allocator: std.mem.Allocator, index_def: *const IndexDef, values: []const Value) Error![]usize {
    var out = std.ArrayList(usize).empty;
    errdefer out.deinit(allocator);
    for (values) |value| {
        const row_ids = try lookupRowIds(allocator, index_def, value);
        defer if (row_ids.owned) allocator.free(row_ids.items);
        for (row_ids.items) |row_id| {
            if (containsUsize(out.items, row_id)) continue;
            try out.append(allocator, row_id);
        }
    }
    return out.toOwnedSlice(allocator);
}

fn indexMatchesColumn(index_def: *const IndexDef, column_name: []const u8) bool {
    return index_def.single_column_idx != null and
        index_def.columns.items.len == 1 and
        ops.eqlIgnoreCase(index_def.columns.items[0].column_name, column_name);
}

const LookupResult = struct {
    items: []const usize,
    owned: bool,
};

fn lookupRowIds(allocator: std.mem.Allocator, index_def: *const IndexDef, value: Value) Error!LookupResult {
    const key = try serializeIndexKeyOwned(allocator, value);
    defer allocator.free(key);

    if (index_def.entries.get(key)) |row_ids| {
        return .{ .items = row_ids.items, .owned = false };
    }
    return .{ .items = try allocator.alloc(usize, 0), .owned = true };
}

fn addRowToIndex(allocator: std.mem.Allocator, index_def: *IndexDef, value: Value, row_id: usize) Error!void {
    const key = try serializeIndexKeyOwned(allocator, value);
    const gop = try index_def.entries.getOrPut(key);
    if (!gop.found_existing) {
        gop.value_ptr.* = std.ArrayList(usize).empty;
    } else {
        allocator.free(key);
    }
    try gop.value_ptr.append(allocator, row_id);
}

fn removeRowFromIndex(allocator: std.mem.Allocator, index_def: *IndexDef, value: Value, row_id: usize) void {
    const key = serializeIndexKeyOwned(allocator, value) catch return;
    defer allocator.free(key);

    if (index_def.entries.getPtr(key)) |row_ids| {
        var i: usize = 0;
        while (i < row_ids.items.len) : (i += 1) {
            if (row_ids.items[i] == row_id) {
                std.debug.assert(row_ids.orderedRemove(i) == row_id);
                break;
            }
        }
        if (row_ids.items.len == 0) {
            const removed = index_def.entries.fetchRemove(key);
            if (removed) |entry| {
                allocator.free(entry.key);
                var list = entry.value;
                list.deinit(allocator);
            }
        }
    }
}

fn serializeIndexKeyOwned(allocator: std.mem.Allocator, value: Value) std.mem.Allocator.Error![]u8 {
    if (value == .null) return allocator.dupe(u8, "\x00");

    if (ops.toNumber(value)) |number| {
        return std.fmt.allocPrint(allocator, "n:{x}", .{@as(u64, @bitCast(number))});
    }

    return switch (value) {
        .text => |text| {
            var out = try allocator.alloc(u8, text.len + 2);
            out[0] = 't';
            out[1] = ':';
            @memcpy(out[2..], text);
            return out;
        },
        .blob => |blob| {
            var out = try allocator.alloc(u8, blob.len + 2);
            out[0] = 'b';
            out[1] = ':';
            @memcpy(out[2..], blob);
            return out;
        },
        else => unreachable,
    };
}

const EqPredicate = struct {
    column_name: []const u8,
    value: Value,
};

const InPredicate = struct {
    column_name: []const u8,
    values: []const Value,
};

fn extractEqPredicate(source: SourceRef, node: *expr_mod.Expr) Error!?EqPredicate {
    if (node.* != .binary or node.binary.op != .eq) return null;

    if (try extractIdentifierColumn(source, node.binary.left)) |column_name| {
        if (node.binary.right.* == .literal) {
            return .{ .column_name = column_name, .value = node.binary.right.literal };
        }
    }
    if (try extractIdentifierColumn(source, node.binary.right)) |column_name| {
        if (node.binary.left.* == .literal) {
            return .{ .column_name = column_name, .value = node.binary.left.literal };
        }
    }
    return null;
}

fn extractIsNullPredicate(source: SourceRef, node: *expr_mod.Expr) Error!?[]const u8 {
    if (node.* != .is_null or node.is_null.not_null) return null;
    return extractIdentifierColumn(source, node.is_null.target);
}

fn extractInPredicate(allocator: std.mem.Allocator, source: SourceRef, node: *expr_mod.Expr) Error!?InPredicate {
    if (node.* != .in_list) return null;
    if (node.in_list.not_in or node.in_list.subquery != null) return null;
    const column_name = try extractIdentifierColumn(source, node.in_list.target) orelse return null;

    const values = try allocator.alloc(Value, node.in_list.items.len);
    errdefer allocator.free(values);
    for (node.in_list.items, 0..) |item, i| {
        if (item.* != .literal) {
            allocator.free(values);
            return null;
        }
        values[i] = item.literal;
    }
    return .{ .column_name = column_name, .values = values };
}

fn extractIdentifierColumn(source: SourceRef, node: *expr_mod.Expr) Error!?[]const u8 {
    if (node.* != .ident) return null;
    if (node.ident.qualifier == null) return node.ident.name;
    const qualifier = node.ident.qualifier.?;
    if (ops.eqlIgnoreCase(qualifier, source.table_name)) return node.ident.name;
    if (source.alias != null and ops.eqlIgnoreCase(qualifier, source.alias.?)) return node.ident.name;
    return null;
}

fn intersectRowIds(allocator: std.mem.Allocator, left: []const usize, right: []const usize) std.mem.Allocator.Error![]usize {
    var out = std.ArrayList(usize).empty;
    errdefer out.deinit(allocator);
    for (left) |row_id| {
        if (!containsUsize(right, row_id)) continue;
        if (containsUsize(out.items, row_id)) continue;
        try out.append(allocator, row_id);
    }
    return out.toOwnedSlice(allocator);
}

fn unionRowIds(allocator: std.mem.Allocator, left: []const usize, right: []const usize) std.mem.Allocator.Error![]usize {
    var out = std.ArrayList(usize).empty;
    errdefer out.deinit(allocator);
    for (left) |row_id| {
        if (containsUsize(out.items, row_id)) continue;
        try out.append(allocator, row_id);
    }
    for (right) |row_id| {
        if (containsUsize(out.items, row_id)) continue;
        try out.append(allocator, row_id);
    }
    return out.toOwnedSlice(allocator);
}

fn containsUsize(items: []const usize, needle: usize) bool {
    for (items) |item| {
        if (item == needle) return true;
    }
    return false;
}
