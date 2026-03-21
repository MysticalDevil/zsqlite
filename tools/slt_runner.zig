const std = @import("std");
const zsqlite = @import("zsqlite");

const SortMode = enum { nosort, rowsort, valuesort };

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = std.process.Args.Iterator.init(init.minimal.args);
    if (!args.skip()) {
        std.debug.print("usage: zsqlite-slt <sqllogictest-file>\n", .{});
        return;
    }
    const path = args.next() orelse {
        std.debug.print("usage: zsqlite-slt <sqllogictest-file>\n", .{});
        return;
    };

    const content = try std.Io.Dir.cwd().readFileAlloc(init.io, path, allocator, .limited(1024 * 1024 * 128));
    defer allocator.free(content);

    var db = zsqlite.Engine.init(allocator);
    defer db.deinit();

    var pass: usize = 0;
    var fail: usize = 0;

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0 or line[0] == '#') continue;

        if (std.mem.startsWith(u8, line, "statement")) {
            const sql = try readSqlBlock(allocator, &lines);
            defer allocator.free(sql);
            const result = db.exec(sql);
            if (result) |_| {
                pass += 1;
            } else |_| {
                fail += 1;
            }
            continue;
        }

        if (std.mem.startsWith(u8, line, "query")) {
            const sort_mode = parseSortMode(line);

            const sql = try readSqlBlock(allocator, &lines);
            defer allocator.free(sql);

            var expected = try readExpectedBlock(allocator, &lines);
            defer {
                for (expected.items) |item| allocator.free(item);
                expected.deinit(allocator);
            }

            var rows = db.query(allocator, sql);
            if (rows) |*rs| {
                defer rs.deinit();
                const ok = try compareResult(allocator, rs, sort_mode, expected.items);
                if (ok) {
                    pass += 1;
                } else {
                    fail += 1;
                }
            } else |_| {
                fail += 1;
            }
        }
    }

    std.debug.print("slt done: pass={d} fail={d}\n", .{ pass, fail });
    if (fail != 0) return error.TestFailed;
}

fn parseSortMode(header: []const u8) SortMode {
    var it = std.mem.tokenizeScalar(u8, header, ' ');
    if (it.next() == null) return .nosort;
    if (it.next() == null) return .nosort;
    const mode = it.next() orelse return .nosort;
    if (std.mem.eql(u8, mode, "rowsort")) return .rowsort;
    if (std.mem.eql(u8, mode, "valuesort")) return .valuesort;
    return .nosort;
}

fn readSqlBlock(allocator: std.mem.Allocator, lines: anytype) ![]u8 {
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

fn readExpectedBlock(allocator: std.mem.Allocator, lines: anytype) !std.ArrayList([]const u8) {
    var out = std.ArrayList([]const u8).empty;
    errdefer {
        for (out.items) |line| allocator.free(line);
        out.deinit(allocator);
    }

    var found_sep = false;
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0) continue;
        if (std.mem.eql(u8, line, "----")) {
            found_sep = true;
            break;
        }
    }

    if (!found_sep) return out;

    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0) break;
        try out.append(allocator, try allocator.dupe(u8, line));
    }

    return out;
}

fn compareResult(
    allocator: std.mem.Allocator,
    rs: *const zsqlite.RowSet,
    sort_mode: SortMode,
    expected: []const []const u8,
) !bool {
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
            const tok = try valueToOwnedString(allocator, v);
            try tokens.append(allocator, tok);

            if (i != 0) try row_buf.append(allocator, '\x1f');
            try row_buf.appendSlice(allocator, tok);
        }

        try row_strings.append(allocator, try row_buf.toOwnedSlice(allocator));
    }

    switch (sort_mode) {
        .rowsort => std.sort.heap([]const u8, row_strings.items, {}, lessString),
        .valuesort => std.sort.heap([]const u8, tokens.items, {}, lessString),
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
        return compareExpected(allocator, row_tokens.items, expected);
    }

    return compareExpected(allocator, tokens.items, expected);
}

fn compareExpected(
    _: std.mem.Allocator,
    tokens: []const []const u8,
    expected: []const []const u8,
) !bool {
    const hash_mode = expected.len == 1 and std.mem.indexOf(u8, expected[0], "values hashing to ") != null;
    if (hash_mode) {
        return compareHashed(tokens, expected[0]);
    }

    if (tokens.len != expected.len) return false;
    for (tokens, expected) |actual, exp| {
        if (!std.mem.eql(u8, actual, exp)) return false;
    }
    return true;
}

fn compareHashed(tokens: []const []const u8, line: []const u8) bool {
    const marker = "values hashing to ";
    const idx = std.mem.indexOf(u8, line, marker) orelse return false;
    const count_str = std.mem.trim(u8, line[0..idx], " ");
    const hash_str = std.mem.trim(u8, line[idx + marker.len ..], " ");

    const expected_count = std.fmt.parseInt(usize, count_str, 10) catch return false;
    if (expected_count != tokens.len) return false;
    if (hash_str.len != 32) return false;

    var hasher = std.crypto.hash.Md5.init(.{});
    for (tokens) |tok| {
        hasher.update(tok);
        hasher.update("\n");
    }
    var digest: [16]u8 = undefined;
    hasher.final(&digest);

    const hex = std.fmt.bytesToHex(digest, .lower);
    return std.mem.eql(u8, &hex, hash_str);
}

fn valueToOwnedString(allocator: std.mem.Allocator, value: zsqlite.Value) ![]const u8 {
    return switch (value) {
        .null => try allocator.dupe(u8, "NULL"),
        .integer => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
        .real => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
        .text => |v| try allocator.dupe(u8, v),
        .blob => try allocator.dupe(u8, "BLOB"),
    };
}

fn lessString(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.order(u8, a, b) == .lt;
}
