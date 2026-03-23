const std = @import("std");
const zsqlite = @import("zsqlite");

const SortMode = enum { nosort, rowsort, valuesort };
const DebugOptions = struct {
    enabled: bool,
    label_filter: ?[]const u8,
    no_label_only: bool,
    join_min: ?usize,
    join_max: ?usize,
    join_variant_min: ?usize,
    join_variant_max: ?usize,
    trace_query: bool,
    stop_on_fail: bool,
};
const QueryMeta = struct {
    label: ?[]const u8,
    column_types: []const u8,
};
const StatementExpectation = enum { ok, err };
const CompareDetails = struct {
    ok: bool,
    actual_count: usize,
    actual_hash: ?[16]u8,
    expected_count: ?usize,
    expected_hash: ?[16]u8,
};
const PendingDirective = union(enum) {
    none,
    skipif: []const u8,
    onlyif: []const u8,
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
        .join_variant_min = null,
        .join_variant_max = null,
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
        if (std.mem.eql(u8, arg, "--join-variant-min")) {
            const value = args.next() orelse {
                std.debug.print("missing value for --join-variant-min\n", .{});
                return;
            };
            opts.join_variant_min = std.fmt.parseInt(usize, value, 10) catch {
                std.debug.print("invalid --join-variant-min: {s}\n", .{value});
                return;
            };
            continue;
        }
        if (std.mem.eql(u8, arg, "--join-variant-max")) {
            const value = args.next() orelse {
                std.debug.print("missing value for --join-variant-max\n", .{});
                return;
            };
            opts.join_variant_max = std.fmt.parseInt(usize, value, 10) catch {
                std.debug.print("invalid --join-variant-max: {s}\n", .{value});
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
    const ignore_statement_failures = opts.label_filter != null or
        opts.no_label_only or
        opts.join_min != null or
        opts.join_max != null or
        opts.join_variant_min != null or
        opts.join_variant_max != null;
    const content = try std.Io.Dir.cwd().readFileAlloc(init.io, path, allocator, .limited(1024 * 1024 * 128));
    defer allocator.free(content);

    var db = zsqlite.Engine.init(allocator);
    defer db.deinit();

    var pass: usize = 0;
    var fail: usize = 0;
    var fail_dump_count: usize = 0;
    var query_index: usize = 0;
    var pending_directive: PendingDirective = .none;

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0 or line[0] == '#') continue;
        if (std.mem.startsWith(u8, line, "hash-threshold ")) continue;
        if (std.mem.startsWith(u8, line, "skipif ")) {
            pending_directive = .{ .skipif = std.mem.trim(u8, line["skipif ".len..], " \t\r") };
            continue;
        }
        if (std.mem.startsWith(u8, line, "onlyif ")) {
            pending_directive = .{ .onlyif = std.mem.trim(u8, line["onlyif ".len..], " \t\r") };
            continue;
        }
        if (std.mem.eql(u8, line, "halt")) {
            const should_skip = shouldSkipDirective(pending_directive);
            pending_directive = .none;
            if (should_skip) continue;
            break;
        }

        if (std.mem.startsWith(u8, line, "statement")) {
            const statement_expect = parseStatementExpectation(line);
            const sql = try readSqlBlock(allocator, &lines);
            defer allocator.free(sql);
            const should_skip = shouldSkipDirective(pending_directive);
            pending_directive = .none;
            if (should_skip) continue;
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
            const should_skip = shouldSkipDirective(pending_directive);
            pending_directive = .none;
            if (opts.join_min != null or opts.join_max != null or opts.join_variant_min != null or opts.join_variant_max != null) {
                const join_key = if (meta.label) |label| parseJoinKey(label) else null;
                if (join_key == null) {
                    var skipped_joinless = try readQueryBlock(allocator, &lines);
                    defer {
                        allocator.free(skipped_joinless.sql);
                        for (skipped_joinless.expected.items) |item| allocator.free(item);
                        skipped_joinless.expected.deinit(allocator);
                    }
                    continue;
                }
                if (opts.join_min) |min_n| {
                    if (join_key.?.primary < min_n) {
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
                    if (join_key.?.primary > max_n) {
                        var skipped_join_max = try readQueryBlock(allocator, &lines);
                        defer {
                            allocator.free(skipped_join_max.sql);
                            for (skipped_join_max.expected.items) |item| allocator.free(item);
                            skipped_join_max.expected.deinit(allocator);
                        }
                        continue;
                    }
                }
                if (opts.join_variant_min) |min_v| {
                    const variant = join_key.?.variant orelse 0;
                    if (variant < min_v) {
                        var skipped_join_variant_min = try readQueryBlock(allocator, &lines);
                        defer {
                            allocator.free(skipped_join_variant_min.sql);
                            for (skipped_join_variant_min.expected.items) |item| allocator.free(item);
                            skipped_join_variant_min.expected.deinit(allocator);
                        }
                        continue;
                    }
                }
                if (opts.join_variant_max) |max_v| {
                    const variant = join_key.?.variant orelse 0;
                    if (variant > max_v) {
                        var skipped_join_variant_max = try readQueryBlock(allocator, &lines);
                        defer {
                            allocator.free(skipped_join_variant_max.sql);
                            for (skipped_join_variant_max.expected.items) |item| allocator.free(item);
                            skipped_join_variant_max.expected.deinit(allocator);
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
            var query_block = try readQueryBlock(allocator, &lines);
            defer {
                for (query_block.expected.items) |item| allocator.free(item);
                query_block.expected.deinit(allocator);
            }
            if (should_skip) {
                allocator.free(query_block.sql);
                continue;
            }
            const sort_mode = parseSortMode(line);
            if (opts.trace_query) {
                if (meta.label) |label| {
                    std.debug.print("query#{d} label={s}\n", .{ query_index, label });
                } else {
                    std.debug.print("query#{d} label=<none>\n", .{query_index});
                }
            }

            const sql = query_block.sql;
            defer allocator.free(sql);

            var rows = db.query(allocator, sql);
            if (rows) |*rs| {
                defer rs.deinit();
                const details = try compareResult(allocator, rs, sort_mode, meta.column_types, query_block.expected.items);
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
    const first = it.next() orelse return .{ .label = null, .column_types = "" };
    if (!std.mem.eql(u8, first, "query")) return .{ .label = null, .column_types = "" };
    const column_types = it.next() orelse return .{ .label = null, .column_types = "" };
    if (it.next() == null) return .{ .label = null, .column_types = column_types };
    const label = it.next();
    return .{ .label = label, .column_types = column_types };
}

const JoinKey = struct {
    primary: usize,
    variant: ?usize,
};

fn parseJoinKey(label: []const u8) ?JoinKey {
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
    return .{
        .primary = primary,
        .variant = variant,
    };
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
    column_types: []const u8,
    expected: []const []const u8,
) !CompareDetails {
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
            try appendValueString(&row_buf, allocator, v, columnTypeAt(column_types, i));
            try tokens.append(allocator, try allocator.dupe(u8, row_buf.items[start..]));
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
        return compareExpected(allocator, row_tokens.items, column_types, expected);
    }

    return compareExpected(allocator, tokens.items, column_types, expected);
}

fn compareRowsortHashFast(
    allocator: std.mem.Allocator,
    rs: *const zsqlite.RowSet,
    column_types: []const u8,
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
            if (i != 0) try row_buf.append(allocator, '\x1f');
            try appendValueString(&row_buf, allocator, v, columnTypeAt(column_types, i));
        }
        try row_strings.append(allocator, try row_buf.toOwnedSlice(allocator));
    }

    std.sort.heap([]const u8, row_strings.items, {}, lessString);

    var hasher = std.crypto.hash.Md5.init(.{});
    var actual_count: usize = 0;
    for (row_strings.items) |row_text| {
        actual_count += hashRowTokens(&hasher, row_text);
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
    column_types: []const u8,
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
    for (tokens, expected, 0..) |actual, exp, idx| {
        if (!tokensEqualForType(actual, exp, columnTypeAt(column_types, idx))) {
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

fn tokensEqualForType(actual: []const u8, expected: []const u8, col_type: u8) bool {
    if (col_type != 'R') return std.mem.eql(u8, actual, expected);
    const actual_num = std.fmt.parseFloat(f64, actual) catch return std.mem.eql(u8, actual, expected);
    const expected_num = std.fmt.parseFloat(f64, expected) catch return std.mem.eql(u8, actual, expected);
    const diff = @abs(actual_num - expected_num);
    const scale = @max(@abs(actual_num), @abs(expected_num));
    return diff <= 0.0005 or diff <= scale * 1e-12;
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
            if (i != 0) try row_buf.append(allocator, '\x1f');
            try appendValueString(&row_buf, allocator, v, 0);
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

fn appendValueString(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, value: zsqlite.Value, col_type: u8) !void {
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

fn columnTypeAt(column_types: []const u8, idx: usize) u8 {
    if (column_types.len == 0) return 0;
    return column_types[idx % column_types.len];
}

fn hashRowTokens(hasher: *std.crypto.hash.Md5, row_text: []const u8) usize {
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

fn lessString(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.order(u8, a, b) == .lt;
}

fn shouldSkipDirective(pending: PendingDirective) bool {
    return switch (pending) {
        .none => false,
        .skipif => |target| backendMatches(target),
        .onlyif => |target| !backendMatches(target),
    };
}

fn backendMatches(target: []const u8) bool {
    return std.mem.eql(u8, target, "sqlite") or std.mem.eql(u8, target, "zsqlite");
}
