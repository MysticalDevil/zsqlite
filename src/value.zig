const std = @import("std");

pub const Value = union(enum) {
    null,
    integer: i64,
    real: f64,
    text: []const u8,
    blob: []const u8,

    pub fn clone(self: Value, allocator: std.mem.Allocator) !Value {
        return switch (self) {
            .null => .null,
            .integer => |v| .{ .integer = v },
            .real => |v| .{ .real = v },
            .text => |v| .{ .text = try allocator.dupe(u8, v) },
            .blob => |v| .{ .blob = try allocator.dupe(u8, v) },
        };
    }

    pub fn eql(a: Value, b: Value) bool {
        return switch (a) {
            .null => b == .null,
            .integer => |av| switch (b) {
                .integer => |bv| av == bv,
                .real => |bv| @as(f64, @floatFromInt(av)) == bv,
                else => false,
            },
            .real => |av| switch (b) {
                .integer => |bv| av == @as(f64, @floatFromInt(bv)),
                .real => |bv| av == bv,
                else => false,
            },
            .text => |av| switch (b) {
                .text => |bv| std.mem.eql(u8, av, bv),
                else => false,
            },
            .blob => |av| switch (b) {
                .blob => |bv| std.mem.eql(u8, av, bv),
                else => false,
            },
        };
    }

    pub fn write(self: Value, writer: anytype) !void {
        switch (self) {
            .null => try writer.writeAll("NULL"),
            .integer => |v| try writer.print("{d}", .{v}),
            .real => |v| try writer.print("{d}", .{v}),
            .text => |v| try writer.writeAll(v),
            .blob => |v| try writer.print("0x{x}", .{std.fmt.fmtSliceHexLower(v)}),
        }
    }
};
