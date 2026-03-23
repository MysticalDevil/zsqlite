const std = @import("std");
const zsqlite = @import("zsqlite");
const types = @import("types.zig");
const format = @import("format.zig");

pub fn dumpMismatchDetails(details: types.CompareDetails, expected: []const []const u8) void {
    std.debug.print("actual token count: {d}\n", .{details.actual_count});
    if (details.expected_count) |n| {
        std.debug.print("expected token count: {d}\n", .{n});
    }
    if (details.actual_hash) |h| {
        const hex = std.fmt.bytesToHex(h, .lower);
        std.debug.print("actual hash: {s}\n", .{hex});
    }
    if (details.expected_hash) |h| {
        const hex = std.fmt.bytesToHex(h, .lower);
        std.debug.print("expected hash: {s}\n", .{hex});
    }
    if (expected.len > 0) {
        std.debug.print("expected first line: {s}\n", .{expected[0]});
    }
}

pub fn dumpActualSample(
    allocator: std.mem.Allocator,
    rs: *const zsqlite.RowSet,
    sort_mode: types.SortMode,
    max_rows: usize,
) !void {
    var row_strings = std.ArrayList([]const u8).empty;
    defer {
        for (row_strings.items) |row| allocator.free(row);
        row_strings.deinit(allocator);
    }

    for (rs.rows.items) |row| {
        var row_buf = std.ArrayList(u8).empty;
        defer row_buf.deinit(allocator);
        for (row, 0..) |v, i| {
            if (i != 0) try row_buf.append(allocator, '\x1f');
            try format.appendValueString(&row_buf, allocator, v, 0);
        }
        try row_strings.append(allocator, try row_buf.toOwnedSlice(allocator));
    }

    if (sort_mode == .rowsort) {
        std.sort.heap([]const u8, row_strings.items, {}, format.lessString);
    }

    const show = @min(max_rows, row_strings.items.len);
    var i: usize = 0;
    while (i < show) : (i += 1) {
        std.debug.print("actual[{d}]: {s}\n", .{ i, row_strings.items[i] });
    }
}
