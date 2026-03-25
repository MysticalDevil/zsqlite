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

test "parser handles trigger timing and event variants" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const before_insert = try sql.parse(arena.allocator(), "CREATE TRIGGER tr_before BEFORE INSERT ON t1 BEGIN SELECT 1; END");
    try std.testing.expect(before_insert == .create_trigger);
    try std.testing.expectEqual(sql.TriggerTiming.before, before_insert.create_trigger.timing);
    try std.testing.expectEqual(sql.TriggerEvent.insert, before_insert.create_trigger.event);

    const after_delete = try sql.parse(arena.allocator(), "CREATE TRIGGER tr_after AFTER DELETE ON t1 BEGIN SELECT 1; END");
    try std.testing.expect(after_delete == .create_trigger);
    try std.testing.expectEqual(sql.TriggerTiming.after, after_delete.create_trigger.timing);
    try std.testing.expectEqual(sql.TriggerEvent.delete, after_delete.create_trigger.event);

    const plain_update = try sql.parse(arena.allocator(), "CREATE TRIGGER tr_plain UPDATE ON t1 BEGIN SELECT 1; END");
    try std.testing.expect(plain_update == .create_trigger);
    try std.testing.expectEqual(sql.TriggerTiming.none, plain_update.create_trigger.timing);
    try std.testing.expectEqual(sql.TriggerEvent.update, plain_update.create_trigger.event);
}

test "parser handles unique index columns select all and index hints" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const create_index = try sql.parse(arena.allocator(), "CREATE UNIQUE INDEX idx1 ON t1 (a DESC, b)");
    try std.testing.expect(create_index == .create_index);
    try std.testing.expect(create_index.create_index.unique);
    try std.testing.expectEqual(@as(usize, 2), create_index.create_index.columns.len);
    try std.testing.expectEqualStrings("a", create_index.create_index.columns[0].column_name);
    try std.testing.expect(create_index.create_index.columns[0].descending);
    try std.testing.expectEqualStrings("b", create_index.create_index.columns[1].column_name);
    try std.testing.expect(!create_index.create_index.columns[1].descending);

    const select_all = try sql.parse(arena.allocator(), "SELECT ALL * FROM t1 INDEXED BY idx1");
    try std.testing.expect(select_all == .select);
    try std.testing.expect(!select_all.select.distinct);
    try std.testing.expectEqualStrings("idx1", select_all.select.from[0].index_hint.indexed_by);

    const select_not_indexed = try sql.parse(arena.allocator(), "SELECT * FROM t1 NOT INDEXED");
    try std.testing.expect(select_not_indexed == .select);
    try std.testing.expect(select_not_indexed.select.from[0].index_hint == .not_indexed);
}

test "parser and engine handle cast to integer" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const stmt = try sql.parse(arena.allocator(), "SELECT CAST(x AS INTEGER) FROM t1");
    try std.testing.expect(stmt == .select);

    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(x)");
    try db.exec("INSERT INTO t1 VALUES ('39.8')");
    var rows = try db.query(allocator, "SELECT CAST(x AS INTEGER), CAST(7.9 AS INTEGER), CAST(NULL AS INTEGER) FROM t1");
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 1), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 39 }));
    try std.testing.expect(rows.rows.items[0][1].eql(.{ .integer = 7 }));
    try std.testing.expect(rows.rows.items[0][2] == .null);
}

test "derived table in FROM can be queried by alias" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const stmt = try sql.parse(arena.allocator(), "SELECT pk FROM (SELECT pk, col0 FROM tab0) AS d0");
    try std.testing.expect(stmt == .select);
    try std.testing.expectEqualStrings("d0", stmt.select.from[0].table_name);
    try std.testing.expect(stmt.select.from[0].subquery_sql != null);

    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE tab0(pk, col0)");
    try db.exec("INSERT INTO tab0 VALUES (1, 10)");
    try db.exec("INSERT INTO tab0 VALUES (2, 20)");

    var rows = try db.query(allocator, "SELECT pk FROM (SELECT pk, col0 FROM tab0) AS d0 ORDER BY 1");
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 2), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 1 }));
    try std.testing.expect(rows.rows.items[1][0].eql(.{ .integer = 2 }));
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

test "aggregate count works across multiple FROM sources" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(a, b)");
    try db.exec("INSERT INTO t1 VALUES (1, 10)");
    try db.exec("INSERT INTO t1 VALUES (2, 20)");

    var rows = try db.query(
        allocator,
        "SELECT COUNT(*) FROM t1, t1 AS x WHERE t1.a < x.a",
    );
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 1), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 1 }));
}

test "aggregate expressions work across multiple FROM sources" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(a, b)");
    try db.exec("INSERT INTO t1 VALUES (1, 3)");
    try db.exec("INSERT INTO t1 VALUES (2, 7)");

    var rows = try db.query(
        allocator,
        "SELECT 14, -COUNT(*), SUM(t1.b) + COUNT(*) FROM t1, t1 AS x WHERE t1.a < x.a",
    );
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 1), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 14 }));
    try std.testing.expect(rows.rows.items[0][1].eql(.{ .integer = -1 }));
    try std.testing.expect(rows.rows.items[0][2].eql(.{ .integer = 4 }));
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

test "delete with where removes matching rows only" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(x, y)");
    try db.exec("INSERT INTO t1 VALUES (1, 'keep')");
    try db.exec("INSERT INTO t1 VALUES (2, 'drop')");
    try db.exec("INSERT INTO t1 VALUES (3, 'keep')");

    const result = try db.exec("DELETE FROM t1 WHERE x=2");
    try std.testing.expectEqual(@as(usize, 1), result.rows_affected);

    var rows = try db.query(allocator, "SELECT x, y FROM t1 ORDER BY 1");
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 2), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 1 }));
    try std.testing.expect(rows.rows.items[0][1].eql(.{ .text = "keep" }));
    try std.testing.expect(rows.rows.items[1][0].eql(.{ .integer = 3 }));
    try std.testing.expect(rows.rows.items[1][1].eql(.{ .text = "keep" }));
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

test "create and drop trigger schema objects" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(x)");
    try db.exec("CREATE TRIGGER tr1 UPDATE ON t1 BEGIN SELECT 1; END");
    try std.testing.expectError(engine_mod.Error.TableAlreadyExists, db.exec("CREATE TRIGGER tr1 UPDATE ON t1 BEGIN SELECT 1; END"));
    try db.exec("DROP TRIGGER tr1");
    try std.testing.expectError(engine_mod.Error.UnknownTable, db.exec("DROP TRIGGER tr1"));
}

test "drop table removes attached triggers" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(x)");
    try db.exec("CREATE TRIGGER tr_before BEFORE INSERT ON t1 BEGIN SELECT 1; END");
    try db.exec("CREATE TRIGGER tr_after AFTER DELETE ON t1 BEGIN SELECT 1; END");
    try db.exec("CREATE TRIGGER tr_plain UPDATE ON t1 BEGIN SELECT 1; END");
    try db.exec("DROP TABLE t1");
    try std.testing.expectError(engine_mod.Error.UnknownTable, db.exec("DROP TRIGGER tr_before"));
    try std.testing.expectError(engine_mod.Error.UnknownTable, db.exec("DROP TRIGGER tr_after"));
    try std.testing.expectError(engine_mod.Error.UnknownTable, db.exec("DROP TRIGGER tr_plain"));
}

test "unique index rejects duplicate insert" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(x)");
    try db.exec("CREATE UNIQUE INDEX idx_x ON t1(x)");
    try db.exec("INSERT INTO t1 VALUES (1)");
    try std.testing.expectError(engine_mod.Error.TableAlreadyExists, db.exec("INSERT INTO t1 VALUES (1)"));
}

test "replace resolves unique index conflicts" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(id INTEGER PRIMARY KEY, x)");
    try db.exec("CREATE UNIQUE INDEX idx_x ON t1(x)");
    try db.exec("INSERT INTO t1 VALUES (1, 'a')");
    try db.exec("INSERT INTO t1 VALUES (2, 'b')");
    try db.exec("REPLACE INTO t1 VALUES (3, 'a')");

    var rows = try db.query(allocator, "SELECT id, x FROM t1 ORDER BY 1");
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 2), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 2 }));
    try std.testing.expect(rows.rows.items[0][1].eql(.{ .text = "b" }));
    try std.testing.expect(rows.rows.items[1][0].eql(.{ .integer = 3 }));
    try std.testing.expect(rows.rows.items[1][1].eql(.{ .text = "a" }));
}

test "indexed by requires an indexable predicate" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(x)");
    try db.exec("CREATE INDEX idx_x ON t1(x)");
    try db.exec("INSERT INTO t1 VALUES (1)");

    try std.testing.expectError(
        engine_mod.Error.UnsupportedSql,
        db.query(allocator, "SELECT * FROM t1 INDEXED BY idx_x"),
    );
}

test "delete removes row from index-backed candidates" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = engine_mod.Engine.init(allocator);
    defer db.deinit();

    try db.exec("CREATE TABLE t1(x)");
    try db.exec("CREATE INDEX idx_x ON t1(x)");
    try db.exec("INSERT INTO t1 VALUES (1)");
    try db.exec("INSERT INTO t1 VALUES (2)");
    try db.exec("DELETE FROM t1 WHERE x=1");

    var rows = try db.query(allocator, "SELECT x FROM t1 INDEXED BY idx_x WHERE x IN (1, 2) ORDER BY 1");
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 1), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 2 }));
}
