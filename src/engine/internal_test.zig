const std = @import("std");
const engine_mod = @import("../engine.zig");
const sql = @import("../sql.zig");

test "correlated subquery respects inner alias scope" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(a, b)");
    try db.exec("INSERT INTO t1 VALUES (1, 10)");
    try db.exec("INSERT INTO t1 VALUES (2, 20)");
    try db.exec("INSERT INTO t1 VALUES (3, 30)");

    var rows = try db.query(
        allocator,
        "SELECT (SELECT count(*) FROM t1 AS x WHERE x.b < t1.b) FROM t1 ORDER BY 1",
    );
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 3), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 0 }));
    try std.testing.expect(rows.rows.items[1][0].eql(.{ .integer = 1 }));
    try std.testing.expect(rows.rows.items[2][0].eql(.{ .integer = 2 }));
}

test "exists correlated subquery works with outer row reference" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(a, b)");
    try db.exec("INSERT INTO t1 VALUES (1, 10)");
    try db.exec("INSERT INTO t1 VALUES (2, 20)");
    try db.exec("INSERT INTO t1 VALUES (3, 30)");

    var rows = try db.query(
        allocator,
        "SELECT a FROM t1 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b < t1.b) ORDER BY 1",
    );
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 2), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 2 }));
    try std.testing.expect(rows.rows.items[1][0].eql(.{ .integer = 3 }));
}

test "parser handles multiline compound select" {
    const query =
        \\  SELECT a FROM t1
        \\EXCEPT
        \\  SELECT b FROM t2
        \\UNION ALL
        \\  SELECT c FROM t3
    ;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const stmt = try sql.parse(arena.allocator(), query);
    try std.testing.expect(stmt == .compound_select);
    try std.testing.expectEqual(@as(usize, 3), stmt.compound_select.arms.len);
    try std.testing.expectEqual(@as(usize, 2), stmt.compound_select.ops.len);
}

test "parser splits complex compound arms" {
    const query =
        \\  SELECT e1 FROM t1
        \\   WHERE a1 in (767,433,637,363,776,109,451)
        \\      OR c1 in (683,531,654,246,3,876,309,284)
        \\      OR (b1=738)
        \\EXCEPT
        \\  SELECT b8 FROM t8
        \\   WHERE NOT ((761=d8 AND b8=259 AND e8=44 AND 762=c8 AND 563=a8)
        \\           OR e8 in (866,579,106,933))
        \\UNION ALL
        \\  SELECT a9 FROM t9
        \\   WHERE (e9=195)
        \\      OR (c9=98 OR d9=145)
    ;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const stmt = try sql.parse(arena.allocator(), query);
    try std.testing.expect(stmt == .compound_select);
    try std.testing.expectEqual(@as(usize, 3), stmt.compound_select.arms.len);
    try std.testing.expect(std.mem.indexOf(u8, stmt.compound_select.arms[0], "EXCEPT") == null);
    try std.testing.expect(std.mem.indexOf(u8, stmt.compound_select.arms[1], "UNION") == null);
}

test "compound query with in lists resolves columns per arm" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(a1, b1, c1, d1, e1)");
    try db.exec("CREATE TABLE t8(a8, b8, c8, d8, e8)");
    try db.exec("CREATE TABLE t9(a9, b9, c9, d9, e9)");
    try db.exec("INSERT INTO t1 VALUES (1,2,3,4,5)");
    try db.exec("INSERT INTO t8 VALUES (1,2,3,4,5)");
    try db.exec("INSERT INTO t9 VALUES (1,2,3,4,5)");

    var rows = try db.query(
        allocator,
        \\  SELECT e1 FROM t1
        \\   WHERE a1 in (767,433,637,363,776,109,451)
        \\      OR c1 in (683,531,654,246,3,876,309,284)
        \\      OR (b1=738)
        \\EXCEPT
        \\  SELECT b8 FROM t8
        \\   WHERE NOT ((761=d8 AND b8=259 AND e8=44 AND 762=c8 AND 563=a8)
        \\           OR e8 in (866,579,106,933))
        \\UNION ALL
        \\  SELECT a9 FROM t9
        \\   WHERE (e9=195)
        \\      OR (c9=98 OR d9=145)
    );
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 0), rows.rows.items.len);
}
