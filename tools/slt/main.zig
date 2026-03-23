const std = @import("std");
const zsqlite = @import("zsqlite");
const types = @import("types.zig");
const parser = @import("parser.zig");
const compare = @import("compare.zig");
const debug = @import("debug.zig");

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
    var opts = types.DebugOptions{
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
    var pending_directive: types.PendingDirective = .none;

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
            const should_skip = parser.shouldSkipDirective(pending_directive);
            pending_directive = .none;
            if (should_skip) continue;
            break;
        }

        if (std.mem.startsWith(u8, line, "statement")) {
            const statement_expect = parser.parseStatementExpectation(line);
            const sql_text = try parser.readSqlBlock(allocator, &lines);
            defer allocator.free(sql_text);
            const should_skip = parser.shouldSkipDirective(pending_directive);
            pending_directive = .none;
            if (should_skip) continue;
            const result = db.exec(sql_text);
            if (result) |_| {
                if (statement_expect == .ok) {
                    pass += 1;
                } else if (!ignore_statement_failures) {
                    fail += 1;
                    if (opts.enabled and fail_dump_count < 5) {
                        std.debug.print("statement expected error but succeeded:\n{s}\n", .{sql_text});
                        fail_dump_count += 1;
                    }
                }
            } else |_| {
                if (statement_expect == .err) {
                    pass += 1;
                } else if (!ignore_statement_failures) {
                    fail += 1;
                    if (opts.enabled and fail_dump_count < 5) {
                        std.debug.print("statement expected ok but failed:\n{s}\n", .{sql_text});
                        fail_dump_count += 1;
                    }
                }
            }
            continue;
        }

        if (!std.mem.startsWith(u8, line, "query")) continue;

        const meta = parser.parseQueryMeta(line);
        query_index += 1;
        const should_skip = parser.shouldSkipDirective(pending_directive);
        pending_directive = .none;
        if (opts.join_min != null or opts.join_max != null or opts.join_variant_min != null or opts.join_variant_max != null) {
            const join_key = if (meta.label) |label| parser.parseJoinKey(label) else null;
            if (join_key == null) {
                var skipped_joinless = try parser.readQueryBlock(allocator, &lines);
                defer freeQueryBlock(allocator, &skipped_joinless);
                continue;
            }
            if (joinMinFiltered(opts, join_key.?) or joinMaxFiltered(opts, join_key.?) or joinVariantMinFiltered(opts, join_key.?) or joinVariantMaxFiltered(opts, join_key.?)) {
                var skipped_filtered = try parser.readQueryBlock(allocator, &lines);
                defer freeQueryBlock(allocator, &skipped_filtered);
                continue;
            }
        }
        if (opts.no_label_only and meta.label != null) {
            var skipped_no_label = try parser.readQueryBlock(allocator, &lines);
            defer freeQueryBlock(allocator, &skipped_no_label);
            continue;
        }
        if (opts.label_filter) |label| {
            if (meta.label == null or !std.mem.eql(u8, meta.label.?, label)) {
                var skipped = try parser.readQueryBlock(allocator, &lines);
                defer freeQueryBlock(allocator, &skipped);
                continue;
            }
        }

        var query_block = try parser.readQueryBlock(allocator, &lines);
        defer freeQueryBlock(allocator, &query_block);
        if (should_skip) continue;

        const sort_mode = parser.parseSortMode(line);
        if (opts.trace_query) {
            if (meta.label) |label| {
                std.debug.print("query#{d} label={s}\n", .{ query_index, label });
            } else {
                std.debug.print("query#{d} label=<none>\n", .{query_index});
            }
        }

        const sql_text = query_block.sql;
        var rows = db.query(allocator, sql_text);
        if (rows) |*rs| {
            defer rs.deinit();
            const details = try compare.compareResult(allocator, rs, sort_mode, meta.column_types, query_block.expected.items);
            if (details.ok) {
                pass += 1;
            } else {
                fail += 1;
                if (fail_dump_count < 5) {
                    std.debug.print("query mismatch:\n{s}\n", .{sql_text});
                    if (opts.enabled) {
                        debug.dumpMismatchDetails(details, query_block.expected.items);
                        try debug.dumpActualSample(allocator, rs, sort_mode, 12);
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
                std.debug.print("query execution error ({s}):\n{s}\n", .{ @errorName(err), sql_text });
                fail_dump_count += 1;
            }
            if (opts.stop_on_fail) {
                std.debug.print("stopping on first execution error\n", .{});
                return error.TestFailed;
            }
        }
    }

    std.debug.print("slt done: pass={d} fail={d}\n", .{ pass, fail });
    if (fail != 0) return error.TestFailed;
}

fn freeQueryBlock(allocator: std.mem.Allocator, block: *types.QueryBlock) void {
    allocator.free(block.sql);
    for (block.expected.items) |item| allocator.free(item);
    block.expected.deinit(allocator);
}

fn joinPrimary(key: types.JoinKey) usize {
    return key.primary;
}

fn joinVariant(key: types.JoinKey) usize {
    return key.variant orelse 0;
}

fn joinMinFiltered(opts: types.DebugOptions, key: types.JoinKey) bool {
    if (opts.join_min) |min_n| return joinPrimary(key) < min_n;
    return false;
}

fn joinMaxFiltered(opts: types.DebugOptions, key: types.JoinKey) bool {
    if (opts.join_max) |max_n| return joinPrimary(key) > max_n;
    return false;
}

fn joinVariantMinFiltered(opts: types.DebugOptions, key: types.JoinKey) bool {
    if (opts.join_variant_min) |min_v| return joinVariant(key) < min_v;
    return false;
}

fn joinVariantMaxFiltered(opts: types.DebugOptions, key: types.JoinKey) bool {
    if (opts.join_variant_max) |max_v| return joinVariant(key) > max_v;
    return false;
}
