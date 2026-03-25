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

    if (sort_mode == .rowsort) {
        if (rs.rows.items.len == 0) {
            return compareExpectedRows(&.{}, column_types, expected);
        }

        var row_strings = std.ArrayList([]const u8).empty;
        defer {
            for (row_strings.items) |row| allocator.free(row);
            row_strings.deinit(allocator);
        }

        try collectRows(allocator, rs, column_types, &row_strings);
        if (row_strings.items.len <= 1) {
            return compareExpectedRows(row_strings.items, column_types, expected);
        }
        std.sort.heap([]const u8, row_strings.items, {}, format.lessString);
        return compareExpectedRows(row_strings.items, column_types, expected);
    }

    var tokens = std.ArrayList([]const u8).empty;
    defer {
        for (tokens.items) |token| allocator.free(token);
        tokens.deinit(allocator);
    }

    try collectTokens(allocator, rs, column_types, &tokens);
    if (tokens.items.len > 1 and sort_mode == .valuesort) {
        std.sort.heap([]const u8, tokens.items, {}, format.lessString);
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

pub fn compareExpectedRows(
    row_strings: []const []const u8,
    column_types: []const u8,
    expected: []const []const u8,
) !types.CompareDetails {
    var actual_count: usize = 0;
    var expected_idx: usize = 0;

    for (row_strings) |row_text| {
        var col_idx: usize = 0;
        var start: usize = 0;
        var i: usize = 0;
        while (i <= row_text.len) : (i += 1) {
            if (i != row_text.len and row_text[i] != '\x1f') continue;
            const token = row_text[start..i];
            if (expected_idx >= expected.len) {
                return .{
                    .ok = false,
                    .actual_count = actual_count + 1,
                    .actual_hash = null,
                    .expected_count = expected.len,
                    .expected_hash = null,
                };
            }
            if (!tokensEqualForType(token, expected[expected_idx], format.columnTypeAt(column_types, col_idx))) {
                return .{
                    .ok = false,
                    .actual_count = actual_count + 1,
                    .actual_hash = null,
                    .expected_count = expected.len,
                    .expected_hash = null,
                };
            }
            actual_count += 1;
            expected_idx += 1;
            col_idx += 1;
            start = i + 1;
        }
    }

    if (actual_count != expected.len or expected_idx != expected.len) {
        return .{
            .ok = false,
            .actual_count = actual_count,
            .actual_hash = null,
            .expected_count = expected.len,
            .expected_hash = null,
        };
    }

    return .{
        .ok = true,
        .actual_count = actual_count,
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

fn collectRows(
    allocator: std.mem.Allocator,
    rs: *const zsqlite.RowSet,
    column_types: []const u8,
    row_strings: *std.ArrayList([]const u8),
) !void {
    for (rs.rows.items) |row| {
        var row_buf = std.ArrayList(u8).empty;
        defer row_buf.deinit(allocator);

        for (row, 0..) |v, i| {
            if (i != 0) try row_buf.append(allocator, '\x1f');
            try format.appendValueString(&row_buf, allocator, v, format.columnTypeAt(column_types, i));
        }

        try row_strings.append(allocator, try row_buf.toOwnedSlice(allocator));
    }
}

fn collectTokens(
    allocator: std.mem.Allocator,
    rs: *const zsqlite.RowSet,
    column_types: []const u8,
    tokens: *std.ArrayList([]const u8),
) !void {
    for (rs.rows.items) |row| {
        var row_buf = std.ArrayList(u8).empty;
        defer row_buf.deinit(allocator);

        for (row, 0..) |v, i| {
            const start = row_buf.items.len;
            try format.appendValueString(&row_buf, allocator, v, format.columnTypeAt(column_types, i));
            try tokens.append(allocator, try allocator.dupe(u8, row_buf.items[start..]));
        }
    }
}

test "rowsort comparison avoids token expansion and keeps typed equality" {
    const expected = [_][]const u8{ "1", "2.000", "3", "4.500" };
    const rows = [_][]const u8{ "3\x1f4.500", "1\x1f2.000" };

    const details = try compareExpectedRows(rows[0..], "IR", expected[0..]);
    try std.testing.expect(details.ok);
    try std.testing.expectEqual(@as(usize, 4), details.actual_count);
}
