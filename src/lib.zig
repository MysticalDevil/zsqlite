pub const Engine = @import("engine.zig").Engine;
pub const ExecResult = @import("engine.zig").ExecResult;
pub const RowState = @import("engine.zig").RowState;
pub const RowSet = @import("engine.zig").RowSet;
pub const Stmt = @import("engine.zig").Stmt;
pub const Value = @import("value.zig").Value;

const std = @import("std");
const engine_mod = @import("engine.zig");

test "create insert select" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.testing.expect(gpa.deinit() == .ok) catch @panic("leak");
    const allocator = gpa.allocator();

    var db = Engine.init(allocator);
    defer db.deinit();

    const create_result = try db.exec("CREATE TABLE t(a, b)");
    try std.testing.expectEqual(@as(usize, 0), create_result.rows_affected);
    const insert_result = try db.exec("INSERT INTO t VALUES (1, 'x')");
    try std.testing.expectEqual(@as(usize, 1), insert_result.rows_affected);

    var rows = try db.query(allocator, "SELECT a FROM t WHERE b = 'x'");
    defer rows.deinit();

    try std.testing.expectEqual(@as(usize, 1), rows.rows.items.len);
    try std.testing.expect(rows.rows.items[0][0].eql(.{ .integer = 1 }));
}
