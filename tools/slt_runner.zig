const std = @import("std");
const zsqlite = @import("zsqlite");

const SortMode = enum { nosort, rowsort, valuesort };
const DebugOptions = struct {
    enabled: bool,
    label_filter: ?[]const u8,
    no_label_only: bool,
    join_min: ?usize,
    join_max: ?usize,
    trace_query: bool,
    stop_on_fail: bool,
};
const QueryMeta = struct {
    label: ?[]const u8,
};
const StatementExpectation = enum { ok, err };
const CompareDetails = struct {
    ok: bool,
    actual_count: usize,
    actual_hash: ?[16]u8,
    expected_count: ?usize,
    expected_hash: ?[16]u8,
};

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
    var opts = DebugOptions{
        .enabled = false,
        .label_filter = null,
        .no_label_only = false,
        .join_min = null,
        .join_max = null,
        .trace_query = false,
        .stop_on_fail = false,
    };
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--")) continue;
        if (std.mem.eql(u8, arg, "--debug-mismatch")) {
            opts.enabled = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--label")) {
            const value = args.next() orelse {
                std.debug.print("missing value for --label\n", .{});
                return;
            };
            opts.label_filter = value;
            continue;
        }
        if (std.mem.eql(u8, arg, "--no-label")) {
            opts.no_label_only = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--join-min")) {
            const value = args.next() orelse {
                std.debug.print("missing value for --join-min\n", .{});
                return;
            };
            opts.join_min = std.fmt.parseInt(usize, value, 10) catch {
                std.debug.print("invalid --join-min: {s}\n", .{value});
                return;
            };
            continue;
        }
        if (std.mem.eql(u8, arg, "--join-max")) {
            const value = args.next() orelse {
                std.debug.print("missing value for --join-max\n", .{});
                return;
            };
            opts.join_max = std.fmt.parseInt(usize, value, 10) catch {
                std.debug.print("invalid --join-max: {s}\n", .{value});
                return;
            };
            continue;
        }
        if (std.mem.eql(u8, arg, "--stop-on-fail")) {
            opts.stop_on_fail = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--trace-query")) {
            opts.trace_query = true;
            continue;
        }
        std.debug.print("unknown argument: {s}\n", .{arg});
        return;
    }
    const ignore_statement_failures = opts.label_filter != null or opts.no_label_only or opts.join_min != null or opts.join_max != null;
    const content = try std.Io.Dir.cwd().readFileAlloc(init.io, path, allocator, .limited(1024 * 1024 * 128));
    defer allocator.free(content);

    var db = zsqlite.Engine.init(allocator);
    defer db.deinit();

    var pass: usize = 0;
    var fail: usize = 0;
    var fail_dump_count: usize = 0;
    var query_index: usize = 0;

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0 or line[0] == '#') continue;

        if (std.mem.startsWith(u8, line, "statement")) {
            const statement_expect = parseStatementExpectation(line);
            const sql = try readSqlBlock(allocator, &lines);
            defer allocator.free(sql);
            const result = db.exec(sql);
            if (result) |_| {
                if (statement_expect == .ok) {
                    pass += 1;
                } else if (!ignore_statement_failures) {
                    fail += 1;
                    if (opts.enabled and fail_dump_count < 5) {
                        std.debug.print("statement expected error but succeeded:\n{s}\n", .{sql});
                        fail_dump_count += 1;
                    }
                }
            } else |_| {
                if (statement_expect == .err) {
                    pass += 1;
                } else if (!ignore_statement_failures) {
                    fail += 1;
                    if (opts.enabled and fail_dump_count < 5) {
                        std.debug.print("statement expected ok but failed:\n{s}\n", .{sql});
                        fail_dump_count += 1;
                    }
                }
            }
            continue;
        }

        if (std.mem.startsWith(u8, line, "query")) {
            const meta = parseQueryMeta(line);
            query_index += 1;
            if (opts.join_min != null or opts.join_max != null) {
                const join_num = if (meta.label) |label| parseJoinNumber(label) else null;
                if (join_num == null) {
                    var skipped_joinless = try readQueryBlock(allocator, &lines);
                    defer {
                        allocator.free(skipped_joinless.sql);
                        for (skipped_joinless.expected.items) |item| allocator.free(item);
                        skipped_joinless.expected.deinit(allocator);
                    }
                    continue;
                }
                if (opts.join_min) |min_n| {
                    if (join_num.? < min_n) {
                        var skipped_join_min = try readQueryBlock(allocator, &lines);
                        defer {
                            allocator.free(skipped_join_min.sql);
                            for (skipped_join_min.expected.items) |item| allocator.free(item);
                            skipped_join_min.expected.deinit(allocator);
                        }
                        continue;
                    }
                }
                if (opts.join_max) |max_n| {
                    if (join_num.? > max_n) {
                        var skipped_join_max = try readQueryBlock(allocator, &lines);
                        defer {
                            allocator.free(skipped_join_max.sql);
                            for (skipped_join_max.expected.items) |item| allocator.free(item);
                            skipped_join_max.expected.deinit(allocator);
                        }
                        continue;
                    }
                }
            }
            if (opts.no_label_only and meta.label != null) {
                var skipped_no_label = try readQueryBlock(allocator, &lines);
                defer {
                    allocator.free(skipped_no_label.sql);
                    for (skipped_no_label.expected.items) |item| allocator.free(item);
                    skipped_no_label.expected.deinit(allocator);
                }
                continue;
            }
            if (opts.label_filter) |label| {
                if (meta.label == null or !std.mem.eql(u8, meta.label.?, label)) {
                    var skipped = try readQueryBlock(allocator, &lines);
                    defer {
                        allocator.free(skipped.sql);
                        for (skipped.expected.items) |item| allocator.free(item);
                        skipped.expected.deinit(allocator);
                    }
                    continue;
                }
            }
            const sort_mode = parseSortMode(line);

            var query_block = try readQueryBlock(allocator, &lines);
            defer {
                for (query_block.expected.items) |item| allocator.free(item);
                query_block.expected.deinit(allocator);
            }
            if (opts.trace_query) {
                std.debug.print("query#{d} label={any}\n", .{ query_index, meta.label });
            }

            const sql = query_block.sql;
            defer allocator.free(sql);

            var rows = db.query(allocator, sql);
            if (rows) |*rs| {
                defer rs.deinit();
                const details = try compareResult(allocator, rs, sort_mode, query_block.expected.items);
                if (details.ok) {
                    pass += 1;
                } else {
                    fail += 1;
                    if (fail_dump_count < 5) {
                        std.debug.print("query mismatch:\n{s}\n", .{sql});
                        if (opts.enabled) {
                            dumpMismatchDetails(details, query_block.expected.items);
                            try dumpActualSample(allocator, rs, sort_mode, 12);
                        }
                        fail_dump_count += 1;
                    }
                    if (opts.stop_on_fail) {
                        std.debug.print("stopping on first mismatch\n", .{});
                        return error.TestFailed;
                    }
                }
            } else |err| {
                fail += 1;
                if (fail_dump_count < 5) {
                    std.debug.print("query execution error ({s}):\n{s}\n", .{ @errorName(err), sql });
                    fail_dump_count += 1;
                }
                if (opts.stop_on_fail) {
                    std.debug.print("stopping on first execution error\n", .{});
                    return error.TestFailed;
                }
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

fn parseStatementExpectation(header: []const u8) StatementExpectation {
    var it = std.mem.tokenizeScalar(u8, header, ' ');
    const first = it.next() orelse return .ok;
    if (!std.mem.eql(u8, first, "statement")) return .ok;
    const second = it.next() orelse return .ok;
    if (std.mem.eql(u8, second, "error")) return .err;
    return .ok;
}

fn parseQueryMeta(header: []const u8) QueryMeta {
    var it = std.mem.tokenizeScalar(u8, header, ' ');
    const first = it.next() orelse return .{ .label = null };
    if (!std.mem.eql(u8, first, "query")) return .{ .label = null };
    if (it.next() == null) return .{ .label = null };
    if (it.next() == null) return .{ .label = null };
    const label = it.next();
    return .{ .label = label };
}

fn parseJoinNumber(label: []const u8) ?usize {
    if (!std.mem.startsWith(u8, label, "join")) return null;
    const suffix = label["join".len..];
    if (suffix.len == 0) return null;
    return std.fmt.parseInt(usize, suffix, 10) catch null;
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

const QueryBlock = struct {
    sql: []u8,
    expected: std.ArrayList([]const u8),
};

fn readQueryBlock(allocator: std.mem.Allocator, lines: anytype) !QueryBlock {
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

fn compareResult(
    allocator: std.mem.Allocator,
    rs: *const zsqlite.RowSet,
    sort_mode: SortMode,
    expected: []const []const u8,
) !CompareDetails {
    const hash_mode = expected.len == 1 and std.mem.indexOf(u8, expected[0], "values hashing to ") != null;
    if (hash_mode and sort_mode == .rowsort) {
        return compareRowsortHashFast(allocator, rs, expected[0]);
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

fn compareRowsortHashFast(
    allocator: std.mem.Allocator,
    rs: *const zsqlite.RowSet,
    expected_line: []const u8,
) !CompareDetails {
    const parsed = parseExpectedHashLine(expected_line) orelse {
        return .{
            .ok = false,
            .actual_count = 0,
            .actual_hash = null,
            .expected_count = null,
            .expected_hash = null,
        };
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
            const tok = try valueToOwnedString(allocator, v);
            defer allocator.free(tok);
            if (i != 0) try row_buf.append(allocator, '\x1f');
            try row_buf.appendSlice(allocator, tok);
        }
        try row_strings.append(allocator, try row_buf.toOwnedSlice(allocator));
    }

    std.sort.heap([]const u8, row_strings.items, {}, lessString);

    var hasher = std.crypto.hash.Md5.init(.{});
    var actual_count: usize = 0;
    for (row_strings.items) |row_text| {
        var it = std.mem.splitScalar(u8, row_text, '\x1f');
        while (it.next()) |tok| {
            hasher.update(tok);
            hasher.update("\n");
            actual_count += 1;
        }
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

fn compareExpected(
    _: std.mem.Allocator,
    tokens: []const []const u8,
    expected: []const []const u8,
) !CompareDetails {
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
    for (tokens, expected) |actual, exp| {
        if (!std.mem.eql(u8, actual, exp)) {
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

const ExpectedHash = struct {
    expected_count: usize,
    expected_hash: [16]u8,
};

fn parseExpectedHashLine(line: []const u8) ?ExpectedHash {
    const marker = "values hashing to ";
    const idx = std.mem.indexOf(u8, line, marker) orelse return null;
    const count_str = std.mem.trim(u8, line[0..idx], " ");
    const hash_str = std.mem.trim(u8, line[idx + marker.len ..], " ");

    const expected_count = std.fmt.parseInt(usize, count_str, 10) catch return null;
    if (hash_str.len != 32) return null;
    var hash_buf: [16]u8 = undefined;
    const out = std.fmt.hexToBytes(&hash_buf, hash_str) catch return null;
    if (out.len != 16) return null;
    return .{
        .expected_count = expected_count,
        .expected_hash = hash_buf,
    };
}

fn hashTokens(tokens: []const []const u8) [16]u8 {
    var hasher = std.crypto.hash.Md5.init(.{});
    for (tokens) |tok| {
        hasher.update(tok);
        hasher.update("\n");
    }
    var digest: [16]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn dumpMismatchDetails(details: CompareDetails, expected: []const []const u8) void {
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

fn dumpActualSample(
    allocator: std.mem.Allocator,
    rs: *const zsqlite.RowSet,
    sort_mode: SortMode,
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
            const tok = try valueToOwnedString(allocator, v);
            defer allocator.free(tok);
            if (i != 0) try row_buf.append(allocator, '\x1f');
            try row_buf.appendSlice(allocator, tok);
        }
        try row_strings.append(allocator, try row_buf.toOwnedSlice(allocator));
    }

    if (sort_mode == .rowsort) {
        std.sort.heap([]const u8, row_strings.items, {}, lessString);
    }

    const show = @min(max_rows, row_strings.items.len);
    var i: usize = 0;
    while (i < show) : (i += 1) {
        std.debug.print("actual[{d}]: {s}\n", .{ i, row_strings.items[i] });
    }
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
