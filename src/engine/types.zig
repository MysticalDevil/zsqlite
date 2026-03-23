const std = @import("std");
const Value = @import("../value.zig").Value;

pub const RowSet = struct {
    allocator: std.mem.Allocator,
    rows: std.ArrayList([]Value),

    pub fn init(allocator: std.mem.Allocator) RowSet {
        return .{ .allocator = allocator, .rows = std.ArrayList([]Value).empty };
    }

    pub fn deinit(self: *RowSet) void {
        for (self.rows.items) |r| {
            for (r) |v| switch (v) {
                .text => |t| self.allocator.free(t),
                .blob => |b| self.allocator.free(b),
                else => {},
            };
            self.allocator.free(r);
        }
        self.rows.deinit(self.allocator);
    }
};

pub const Table = struct {
    name: []const u8,
    columns: std.ArrayList([]const u8),
    integer_affinity: std.ArrayList(bool),
    rows: std.ArrayList([]Value),
    primary_key_col: ?usize,

    pub fn deinit(self: *Table, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        for (self.columns.items) |c| allocator.free(c);
        self.columns.deinit(allocator);
        self.integer_affinity.deinit(allocator);
        for (self.rows.items) |row| {
            for (row) |v| switch (v) {
                .text => |t| allocator.free(t),
                .blob => |b| allocator.free(b),
                else => {},
            };
            allocator.free(row);
        }
        self.rows.deinit(allocator);
    }
};

pub const EvalCtx = struct {
    table: *const Table,
    table_name: []const u8,
    alias: ?[]const u8,
    row: ?[]const Value,
    parent: ?*const EvalCtx,
};

pub const SortRow = struct {
    values: []Value,
    keys: []Value,
};
