const std = @import("std");
const zsqlite = @import("zsqlite");

pub fn valueToOwnedString(allocator: std.mem.Allocator, value: zsqlite.Value) ![]const u8 {
    return switch (value) {
        .null => try allocator.dupe(u8, "NULL"),
        .integer => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
        .real => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
        .text => |v| try allocator.dupe(u8, v),
        .blob => try allocator.dupe(u8, "BLOB"),
    };
}

pub fn appendValueString(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, value: zsqlite.Value, col_type: u8) !void {
    switch (value) {
        .null => try buf.appendSlice(allocator, "NULL"),
        .integer => |v| switch (col_type) {
            'R' => {
                var scratch: [64]u8 = undefined;
                const text = try std.fmt.bufPrint(&scratch, "{d:.3}", .{@as(f64, @floatFromInt(v))});
                try buf.appendSlice(allocator, text);
            },
            else => {
                var scratch: [64]u8 = undefined;
                const text = try std.fmt.bufPrint(&scratch, "{d}", .{v});
                try buf.appendSlice(allocator, text);
            },
        },
        .real => |v| switch (col_type) {
            'I' => {
                var scratch: [64]u8 = undefined;
                const text = try std.fmt.bufPrint(&scratch, "{d}", .{@as(i64, @intFromFloat(v))});
                try buf.appendSlice(allocator, text);
            },
            'R' => {
                var scratch: [64]u8 = undefined;
                const text = try std.fmt.bufPrint(&scratch, "{d:.3}", .{v});
                try buf.appendSlice(allocator, text);
            },
            else => {
                var scratch: [64]u8 = undefined;
                const text = try std.fmt.bufPrint(&scratch, "{d}", .{v});
                try buf.appendSlice(allocator, text);
            },
        },
        .text => |v| switch (col_type) {
            'I' => {
                const int_value = std.fmt.parseInt(i64, v, 10) catch 0;
                var scratch: [64]u8 = undefined;
                const text = try std.fmt.bufPrint(&scratch, "{d}", .{int_value});
                try buf.appendSlice(allocator, text);
            },
            'R' => {
                const real_value = std.fmt.parseFloat(f64, v) catch 0;
                var scratch: [64]u8 = undefined;
                const text = try std.fmt.bufPrint(&scratch, "{d:.3}", .{real_value});
                try buf.appendSlice(allocator, text);
            },
            else => try buf.appendSlice(allocator, v),
        },
        .blob => try buf.appendSlice(allocator, "BLOB"),
    }
}

pub fn columnTypeAt(column_types: []const u8, idx: usize) u8 {
    if (column_types.len == 0) return 0;
    return column_types[idx % column_types.len];
}

pub fn hashRowTokens(hasher: *std.crypto.hash.Md5, row_text: []const u8) usize {
    var token_count: usize = 0;
    var start: usize = 0;
    var idx: usize = 0;
    while (idx < row_text.len) : (idx += 1) {
        if (row_text[idx] != '\x1f') continue;
        hasher.update(row_text[start..idx]);
        hasher.update("\n");
        token_count += 1;
        start = idx + 1;
    }
    hasher.update(row_text[start..]);
    hasher.update("\n");
    token_count += 1;
    return token_count;
}

pub fn lessString(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.order(u8, a, b) == .lt;
}
