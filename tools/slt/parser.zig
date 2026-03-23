const std = @import("std");
const types = @import("types.zig");

pub fn parseSortMode(header: []const u8) types.SortMode {
    var it = std.mem.tokenizeScalar(u8, header, ' ');
    if (it.next() == null) return .nosort;
    if (it.next() == null) return .nosort;
    const mode = it.next() orelse return .nosort;
    if (std.mem.eql(u8, mode, "rowsort")) return .rowsort;
    if (std.mem.eql(u8, mode, "valuesort")) return .valuesort;
    return .nosort;
}

pub fn parseStatementExpectation(header: []const u8) types.StatementExpectation {
    var it = std.mem.tokenizeScalar(u8, header, ' ');
    const first = it.next() orelse return .ok;
    if (!std.mem.eql(u8, first, "statement")) return .ok;
    const second = it.next() orelse return .ok;
    if (std.mem.eql(u8, second, "error")) return .err;
    return .ok;
}

pub fn parseQueryMeta(header: []const u8) types.QueryMeta {
    var it = std.mem.tokenizeScalar(u8, header, ' ');
    const first = it.next() orelse return .{ .label = null, .column_types = "" };
    if (!std.mem.eql(u8, first, "query")) return .{ .label = null, .column_types = "" };
    const column_types = it.next() orelse return .{ .label = null, .column_types = "" };
    if (it.next() == null) return .{ .label = null, .column_types = column_types };
    const label = it.next();
    return .{ .label = label, .column_types = column_types };
}

pub fn parseJoinKey(label: []const u8) ?types.JoinKey {
    if (!std.mem.startsWith(u8, label, "join")) return null;
    var suffix = label["join".len..];
    if (suffix.len == 0) return null;
    if (suffix[0] == '-') suffix = suffix[1..];
    if (suffix.len == 0) return null;

    var split = std.mem.splitScalar(u8, suffix, '-');
    const first = split.next() orelse return null;
    if (first.len == 0) return null;
    const primary = std.fmt.parseInt(usize, first, 10) catch return null;
    const second = split.next();
    const variant = if (second) |s|
        std.fmt.parseInt(usize, s, 10) catch return null
    else
        null;
    return .{ .primary = primary, .variant = variant };
}

pub fn readSqlBlock(allocator: std.mem.Allocator, lines: anytype) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);

    while (lines.next()) |line_raw| {
        const line = std.mem.trimEnd(u8, line_raw, "\r");
        if (std.mem.trim(u8, line, " \t").len == 0) break;
        try buf.appendSlice(allocator, line);
        try buf.append(allocator, '\n');
    }
    return buf.toOwnedSlice(allocator);
}

pub fn readQueryBlock(allocator: std.mem.Allocator, lines: anytype) !types.QueryBlock {
    var sql_buf = std.ArrayList(u8).empty;
    errdefer sql_buf.deinit(allocator);

    var expected = std.ArrayList([]const u8).empty;
    errdefer {
        for (expected.items) |line| allocator.free(line);
        expected.deinit(allocator);
    }

    var found_sep = false;
    while (lines.next()) |line_raw| {
        const line = std.mem.trimEnd(u8, line_raw, "\r");
        const trimmed = std.mem.trim(u8, line, " \t");
        if (std.mem.eql(u8, trimmed, "----")) {
            found_sep = true;
            break;
        }
        try sql_buf.appendSlice(allocator, line);
        try sql_buf.append(allocator, '\n');
    }

    if (!found_sep) return error.InvalidData;

    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0) break;
        try expected.append(allocator, try allocator.dupe(u8, line));
    }

    return .{
        .sql = try sql_buf.toOwnedSlice(allocator),
        .expected = expected,
    };
}

pub fn shouldSkipDirective(pending: types.PendingDirective) bool {
    return switch (pending) {
        .none => false,
        .skipif => |target| backendMatches(target),
        .onlyif => |target| !backendMatches(target),
    };
}

pub fn backendMatches(target: []const u8) bool {
    return std.mem.eql(u8, target, "sqlite") or std.mem.eql(u8, target, "zsqlite");
}
