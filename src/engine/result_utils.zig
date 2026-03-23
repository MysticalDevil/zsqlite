const std = @import("std");
const Value = @import("../value.zig").Value;
const ops = @import("value_ops.zig");
const types = @import("types.zig");

const RowSet = types.RowSet;
const SortRow = types.SortRow;

pub fn cloneResultValue(allocator: std.mem.Allocator, v: Value) std.mem.Allocator.Error!Value {
    return switch (v) {
        .text => |t| .{ .text = try allocator.dupe(u8, t) },
        .blob => |b| .{ .blob = try allocator.dupe(u8, b) },
        else => v,
    };
}

pub fn sortRowLessThan(_: void, a: SortRow, b: SortRow) bool {
    var i: usize = 0;
    while (i < a.keys.len and i < b.keys.len) : (i += 1) {
        const cmp = ops.compareValues(a.keys[i], b.keys[i]);
        if (cmp < 0) return true;
        if (cmp > 0) return false;
    }
    return false;
}

pub fn cloneRowSet(allocator: std.mem.Allocator, src: *const RowSet) std.mem.Allocator.Error!RowSet {
    var out = RowSet.init(allocator);
    errdefer out.deinit();
    for (src.rows.items) |row| {
        try appendClonedRow(allocator, &out, row);
    }
    return out;
}

pub fn rowSetIntersect(allocator: std.mem.Allocator, left: *const RowSet, right: *const RowSet) std.mem.Allocator.Error!RowSet {
    var out = RowSet.init(allocator);
    errdefer out.deinit();
    for (left.rows.items) |row| {
        if (containsRow(right.rows.items, row) and !containsRow(out.rows.items, row)) {
            try appendClonedRow(allocator, &out, row);
        }
    }
    return out;
}

pub fn rowSetExcept(allocator: std.mem.Allocator, left: *const RowSet, right: *const RowSet) std.mem.Allocator.Error!RowSet {
    var out = RowSet.init(allocator);
    errdefer out.deinit();
    for (left.rows.items) |row| {
        if (!containsRow(right.rows.items, row) and !containsRow(out.rows.items, row)) {
            try appendClonedRow(allocator, &out, row);
        }
    }
    return out;
}

pub fn dedupRowsInPlace(allocator: std.mem.Allocator, rs: *RowSet) void {
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

pub fn freeOwnedRow(allocator: std.mem.Allocator, row: []Value) void {
    for (row) |v| switch (v) {
        .text => |t| allocator.free(t),
        .blob => |b| allocator.free(b),
        else => {},
    };
    allocator.free(row);
}

pub fn appendClonedRow(allocator: std.mem.Allocator, rs: *RowSet, row: []const Value) std.mem.Allocator.Error!void {
    const copied = try allocator.alloc(Value, row.len);
    for (row, 0..) |v, i| {
        copied[i] = try cloneResultValue(allocator, v);
    }
    try rs.rows.append(allocator, copied);
}

pub fn appendDistinctRow(allocator: std.mem.Allocator, rs: *RowSet, row: []const Value) std.mem.Allocator.Error!void {
    if (containsRow(rs.rows.items, row)) return;
    try appendClonedRow(allocator, rs, row);
}

pub fn containsRow(rows: []const []Value, target: []const Value) bool {
    for (rows) |row| {
        if (rowsEqual(row, target)) return true;
    }
    return false;
}

pub fn rowsEqual(a: []const Value, b: []const Value) bool {
    if (a.len != b.len) return false;
    for (a, b) |av, bv| {
        if (ops.compareValues(av, bv) != 0) return false;
    }
    return true;
}
