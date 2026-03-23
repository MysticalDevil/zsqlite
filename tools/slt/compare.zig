const std = @import("std");
const zsqlite = @import("zsqlite");
const types = @import("types.zig");
const format = @import("format.zig");

pub fn compareResult(
    allocator: std.mem.Allocator,
    rs: *const zsqlite.RowSet,
    sort_mode: types.SortMode,
    column_types: []const u8,
    expected: []const []const u8,
) !types.CompareDetails {
    const hash_mode = expected.len == 1 and std.mem.indexOf(u8, expected[0], "values hashing to ") != null;
    if (hash_mode and sort_mode == .rowsort) {
        return compareRowsortHashFast(allocator, rs, column_types, expected[0]);
    }

    var tokens = std.ArrayList([]const u8).empty;
    defer {
        for (tokens.items) |token| allocator.free(token);
        tokens.deinit(allocator);
    }

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
            const start = row_buf.items.len;
            try format.appendValueString(&row_buf, allocator, v, format.columnTypeAt(column_types, i));
            try tokens.append(allocator, try allocator.dupe(u8, row_buf.items[start..]));
        }

        try row_strings.append(allocator, try row_buf.toOwnedSlice(allocator));
    }

    switch (sort_mode) {
        .rowsort => std.sort.heap([]const u8, row_strings.items, {}, format.lessString),
        .valuesort => std.sort.heap([]const u8, tokens.items, {}, format.lessString),
        .nosort => {},
    }

    if (sort_mode == .rowsort) {
        var row_tokens = std.ArrayList([]const u8).empty;
        defer {
            for (row_tokens.items) |tok| allocator.free(tok);
            row_tokens.deinit(allocator);
        }
        for (row_strings.items) |row| {
            var it = std.mem.splitScalar(u8, row, '\x1f');
            while (it.next()) |tok| {
                try row_tokens.append(allocator, try allocator.dupe(u8, tok));
            }
        }
        return compareExpected(row_tokens.items, column_types, expected);
    }

    return compareExpected(tokens.items, column_types, expected);
}

pub fn compareRowsortHashFast(
    allocator: std.mem.Allocator,
    rs: *const zsqlite.RowSet,
    column_types: []const u8,
    expected_line: []const u8,
) !types.CompareDetails {
    const parsed = parseExpectedHashLine(expected_line) orelse {
        return .{ .ok = false, .actual_count = 0, .actual_hash = null, .expected_count = null, .expected_hash = null };
    };

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
            try format.appendValueString(&row_buf, allocator, v, format.columnTypeAt(column_types, i));
        }
        try row_strings.append(allocator, try row_buf.toOwnedSlice(allocator));
    }

    std.sort.heap([]const u8, row_strings.items, {}, format.lessString);

    var hasher = std.crypto.hash.Md5.init(.{});
    var actual_count: usize = 0;
    for (row_strings.items) |row_text| {
        actual_count += format.hashRowTokens(&hasher, row_text);
    }
    var digest: [16]u8 = undefined;
    hasher.final(&digest);

    return .{
        .ok = actual_count == parsed.expected_count and std.mem.eql(u8, &digest, &parsed.expected_hash),
        .actual_count = actual_count,
        .actual_hash = digest,
        .expected_count = parsed.expected_count,
        .expected_hash = parsed.expected_hash,
    };
}

pub fn compareExpected(
    tokens: []const []const u8,
    column_types: []const u8,
    expected: []const []const u8,
) !types.CompareDetails {
    const hash_mode = expected.len == 1 and std.mem.indexOf(u8, expected[0], "values hashing to ") != null;
    if (hash_mode) {
        const parsed = parseExpectedHashLine(expected[0]) orelse {
            return .{
                .ok = false,
                .actual_count = tokens.len,
                .actual_hash = hashTokens(tokens),
                .expected_count = null,
                .expected_hash = null,
            };
        };
        const actual_hash = hashTokens(tokens);
        return .{
            .ok = parsed.expected_count == tokens.len and std.mem.eql(u8, &actual_hash, &parsed.expected_hash),
            .actual_count = tokens.len,
            .actual_hash = actual_hash,
            .expected_count = parsed.expected_count,
            .expected_hash = parsed.expected_hash,
        };
    }

    if (tokens.len != expected.len) {
        return .{
            .ok = false,
            .actual_count = tokens.len,
            .actual_hash = null,
            .expected_count = expected.len,
            .expected_hash = null,
        };
    }
    for (tokens, expected, 0..) |actual, exp, idx| {
        if (!tokensEqualForType(actual, exp, format.columnTypeAt(column_types, idx))) {
            return .{
                .ok = false,
                .actual_count = tokens.len,
                .actual_hash = null,
                .expected_count = expected.len,
                .expected_hash = null,
            };
        }
    }
    return .{
        .ok = true,
        .actual_count = tokens.len,
        .actual_hash = null,
        .expected_count = expected.len,
        .expected_hash = null,
    };
}

pub fn tokensEqualForType(actual: []const u8, expected: []const u8, col_type: u8) bool {
    if (col_type != 'R') return std.mem.eql(u8, actual, expected);
    const actual_num = std.fmt.parseFloat(f64, actual) catch return std.mem.eql(u8, actual, expected);
    const expected_num = std.fmt.parseFloat(f64, expected) catch return std.mem.eql(u8, actual, expected);
    const diff = @abs(actual_num - expected_num);
    const scale = @max(@abs(actual_num), @abs(expected_num));
    return diff <= 0.0005 or diff <= scale * 1e-12;
}

pub fn parseExpectedHashLine(line: []const u8) ?types.ExpectedHash {
    const marker = "values hashing to ";
    const idx = std.mem.indexOf(u8, line, marker) orelse return null;
    const count_str = std.mem.trim(u8, line[0..idx], " ");
    const hash_str = std.mem.trim(u8, line[idx + marker.len ..], " ");

    const expected_count = std.fmt.parseInt(usize, count_str, 10) catch return null;
    if (hash_str.len != 32) return null;
    var hash_buf: [16]u8 = undefined;
    const out = std.fmt.hexToBytes(&hash_buf, hash_str) catch return null;
    if (out.len != 16) return null;
    return .{ .expected_count = expected_count, .expected_hash = hash_buf };
}

pub fn hashTokens(tokens: []const []const u8) [16]u8 {
    var hasher = std.crypto.hash.Md5.init(.{});
    for (tokens) |tok| {
        hasher.update(tok);
        hasher.update("\n");
    }
    var digest: [16]u8 = undefined;
    hasher.final(&digest);
    return digest;
}
