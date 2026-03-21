const std = @import("std");
const engine_mod = @import("../engine.zig");

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
