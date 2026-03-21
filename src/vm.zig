const std = @import("std");
const Value = @import("value.zig").Value;
const sql = @import("sql.zig");

pub const Program = struct {
    table_name: []const u8,
    projections: []const []const u8,
    where_eq: ?sql.WhereEq,

    pub fn fromSelect(s: sql.Select) Program {
        return .{
            .table_name = s.table_name,
            .projections = s.projections,
            .where_eq = s.where_eq,
        };
    }
};

pub const ExecutionError = error{ UnknownColumn, OutOfMemory };

pub fn columnIndex(columns: []const []const u8, name: []const u8) ?usize {
    for (columns, 0..) |col, idx| {
        if (std.mem.eql(u8, col, name)) return idx;
    }
    return null;
}

pub fn matchesWhere(cols: []const []const u8, row: []const Value, where_eq: ?sql.WhereEq) ExecutionError!bool {
    if (where_eq) |w| {
        const idx = columnIndex(cols, w.column) orelse return ExecutionError.UnknownColumn;
        return row[idx].eql(w.value);
    }
    return true;
}

pub fn projectRow(allocator: std.mem.Allocator, cols: []const []const u8, row: []const Value, projections: []const []const u8) ExecutionError![]Value {
    if (projections.len == 1 and std.mem.eql(u8, projections[0], "*")) {
        var out = try allocator.alloc(Value, row.len);
        for (row, 0..) |v, i| {
            out[i] = v;
        }
        return out;
    }

    var out = try allocator.alloc(Value, projections.len);
    for (projections, 0..) |proj, i| {
        const idx = columnIndex(cols, proj) orelse return ExecutionError.UnknownColumn;
        out[i] = row[idx];
    }
    return out;
}
