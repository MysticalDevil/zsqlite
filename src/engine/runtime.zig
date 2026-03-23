const std = @import("std");
const sql = @import("../sql.zig");
const Value = @import("../value.zig").Value;

pub const QueryMetrics = struct {
    eval_expr_calls: usize = 0,
    subquery_exec_calls: usize = 0,
    subquery_cache_hits: usize = 0,
};

pub const QueryRuntime = struct {
    pub const SubqueryMode = enum { unknown, uncorrelated, correlated };

    scalar_cache: std.StringHashMap(Value),
    scalar_mode: std.StringHashMap(SubqueryMode),
    exists_cache: std.StringHashMap(Value),
    exists_mode: std.StringHashMap(SubqueryMode),
    parsed_subquery: std.StringHashMap(sql.Statement),
    parse_arena: std.heap.ArenaAllocator,

    pub fn init(allocator: std.mem.Allocator) QueryRuntime {
        return .{
            .scalar_cache = std.StringHashMap(Value).init(allocator),
            .scalar_mode = std.StringHashMap(SubqueryMode).init(allocator),
            .exists_cache = std.StringHashMap(Value).init(allocator),
            .exists_mode = std.StringHashMap(SubqueryMode).init(allocator),
            .parsed_subquery = std.StringHashMap(sql.Statement).init(allocator),
            .parse_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    pub fn deinit(self: *QueryRuntime, allocator: std.mem.Allocator) void {
        var scalar_it = self.scalar_cache.iterator();
        while (scalar_it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            switch (entry.value_ptr.*) {
                .text => |t| allocator.free(t),
                .blob => |b| allocator.free(b),
                else => {},
            }
        }
        self.scalar_cache.deinit();

        var scalar_mode_it = self.scalar_mode.iterator();
        while (scalar_mode_it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        self.scalar_mode.deinit();

        var exists_it = self.exists_cache.iterator();
        while (exists_it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            switch (entry.value_ptr.*) {
                .text => |t| allocator.free(t),
                .blob => |b| allocator.free(b),
                else => {},
            }
        }
        self.exists_cache.deinit();

        var exists_mode_it = self.exists_mode.iterator();
        while (exists_mode_it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        self.exists_mode.deinit();

        self.parsed_subquery.deinit();
        self.parse_arena.deinit();
    }
};
