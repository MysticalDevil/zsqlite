const std = @import("std");
const Value = @import("../value.zig").Value;
const sql = @import("../sql.zig");
const expr_mod = @import("../expr/mod.zig");
const types = @import("types.zig");
const runtime_mod = @import("runtime.zig");

pub const RowSet = types.RowSet;
pub const Table = types.Table;
pub const EvalCtx = types.EvalCtx;
pub const SortRow = types.SortRow;
pub const QueryMetrics = runtime_mod.QueryMetrics;
pub const QueryRuntime = runtime_mod.QueryRuntime;

pub const DepthFilterPlan = union(enum) {
    expr: *expr_mod.Expr,
    eq_columns: struct {
        left_src: usize,
        left_col: usize,
        right_src: usize,
        right_col: usize,
    },
};

pub const SourceRef = struct {
    table: *const Table,
    table_name: []const u8,
    alias: ?[]const u8,
};

pub const ViewDef = struct {
    name: []const u8,
    select_sql: []const u8,
    columns: std.ArrayList([]const u8),

    pub fn deinit(self: *ViewDef, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.select_sql);
        for (self.columns.items) |col| allocator.free(col);
        self.columns.deinit(allocator);
    }
};

pub const TriggerDef = struct {
    name: []const u8,
    table_name: []const u8,
    timing: sql.TriggerTiming,
    event: sql.TriggerEvent,
    body_sql: []const u8,

    pub fn deinit(self: *TriggerDef, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.table_name);
        allocator.free(self.body_sql);
    }
};

pub const IndexDef = struct {
    name: []const u8,
    table_name: []const u8,
    unique: bool,
    columns: std.ArrayList(sql.IndexColumn),

    pub fn deinit(self: *IndexDef, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.table_name);
        for (self.columns.items) |col| {
            allocator.free(col.column_name);
        }
        self.columns.deinit(allocator);
    }
};

pub const Error = error{
    OutOfMemory,
    TableAlreadyExists,
    UnknownTable,
    ColumnCountMismatch,
    UnknownColumn,
    InvalidSql,
    UnsupportedSql,
    InvalidLiteral,
    AggregateEmpty,
};

pub const ExecResult = struct {
    rows_affected: usize = 0,
};

pub const RowState = enum { row, done };
