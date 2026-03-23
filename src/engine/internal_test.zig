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

test "uncorrelated scalar subquery executes once per statement" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(a, b)");
    var i: i64 = 0;
    while (i < 50) : (i += 1) {
        try db.exec("INSERT INTO t1 VALUES (1, 2)");
    }

    db.resetMetrics();
    var rows = try db.query(
        allocator,
        "SELECT (SELECT avg(b) FROM t1) FROM t1",
    );
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 50), rows.rows.items.len);
    const m = db.metrics();
    try std.testing.expectEqual(@as(usize, 1), m.subquery_exec_calls);
    try std.testing.expect(m.subquery_cache_hits >= 49);
}

test "uncorrelated exists subquery executes once per statement" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(a, b)");
    var i: i64 = 1;
    while (i <= 30) : (i += 1) {
        const stmt = try std.fmt.allocPrint(allocator, "INSERT INTO t1 VALUES ({d}, {d})", .{ i, i });
        defer allocator.free(stmt);
        try db.exec(stmt);
    }

    db.resetMetrics();
    var rows = try db.query(
        allocator,
        "SELECT a FROM t1 WHERE EXISTS(SELECT 1 FROM t1 WHERE b > 0)",
    );
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 30), rows.rows.items.len);
    const m = db.metrics();
    try std.testing.expectEqual(@as(usize, 1), m.subquery_exec_calls);
    try std.testing.expect(m.subquery_cache_hits >= 29);
}

test "where OR short-circuits subquery evaluation" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(a, b)");
    try db.exec("INSERT INTO t1 VALUES (1, 10)");
    try db.exec("INSERT INTO t1 VALUES (2, 20)");

    db.resetMetrics();
    var rows = try db.query(
        allocator,
        "SELECT a FROM t1 WHERE 1=1 OR EXISTS(SELECT 1 FROM t1 WHERE b > 100)",
    );
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 2), rows.rows.items.len);
    const m = db.metrics();
    try std.testing.expectEqual(@as(usize, 0), m.subquery_exec_calls);
}

test "empty IN list follows SQLite truth table" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(a)");
    try db.exec("INSERT INTO t1 VALUES (1)");
    try db.exec("INSERT INTO t1 VALUES (NULL)");

    var rows = try db.query(
        allocator,
        "SELECT a IN (), a NOT IN () FROM t1 ORDER BY 1",
    );
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 2), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 0 }));
    try std.testing.expect(rows.rows.items[0][1].eql(.{ .integer = 1 }));
    try std.testing.expect(rows.rows.items[1][0].eql(.{ .integer = 0 }));
    try std.testing.expect(rows.rows.items[1][1].eql(.{ .integer = 1 }));
}

test "IN supports subquery and table-name shorthand" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(x)");
    try db.exec("INSERT INTO t1 VALUES (1)");
    try db.exec("INSERT INTO t1 VALUES (2)");

    var rows = try db.query(
        allocator,
        "SELECT 1 IN t1, 3 IN (SELECT x FROM t1), NULL IN (SELECT x FROM t1)",
    );
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 1), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 1 }));
    try std.testing.expect(rows.rows.items[0][1].eql(.{ .integer = 0 }));
    try std.testing.expect(rows.rows.items[0][2] == .null);
}

test "insert into select copies source rows" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE src(a)");
    try db.exec("CREATE TABLE dst(a)");
    try db.exec("INSERT INTO src VALUES (2)");
    try db.exec("INSERT INTO src VALUES (3)");
    try db.exec("INSERT INTO dst SELECT * FROM src");

    var rows = try db.query(allocator, "SELECT a FROM dst ORDER BY 1");
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 2), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 2 }));
    try std.testing.expect(rows.rows.items[1][0].eql(.{ .integer = 3 }));
}

test "blob literal parses in expressions" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(x)");
    try db.exec("INSERT INTO t1 VALUES (1)");

    var rows = try db.query(allocator, "SELECT x'303132' IN ()");
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 1), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 0 }));
}

test "update uses old row values and rightmost assignment wins" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(x, y)");
    try db.exec("INSERT INTO t1 VALUES (1, 'a')");
    try db.exec("UPDATE t1 SET x=10, x=x+2, y='b' WHERE x=1");

    var rows = try db.query(allocator, "SELECT x, y FROM t1");
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 1), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 3 }));
    try std.testing.expect(rows.rows.items[0][1].eql(.{ .text = "b" }));
}

test "update without where touches all rows" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(x)");
    try db.exec("INSERT INTO t1 VALUES (1)");
    try db.exec("INSERT INTO t1 VALUES (2)");
    try db.exec("UPDATE t1 SET x=7");

    var rows = try db.query(allocator, "SELECT count(*) FROM t1 WHERE x=7");
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 1), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 2 }));
}

test "replace overwrites existing primary key row" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(x INTEGER PRIMARY KEY, y)");
    try db.exec("INSERT INTO t1 VALUES (2, 'insert')");
    try db.exec("REPLACE INTO t1 VALUES (2, 'replace')");

    var rows = try db.query(allocator, "SELECT x, y FROM t1");
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 1), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 2 }));
    try std.testing.expect(rows.rows.items[0][1].eql(.{ .text = "replace" }));
}
