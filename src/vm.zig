const std = @import("std");

pub fn columnIndex(columns: []const []const u8, name: []const u8) ?usize {
    for (columns, 0..) |col, idx| {
        if (std.mem.eql(u8, col, name)) return idx;
    }
    return null;
}
