const std = @import("std");
const sql = @import("sql.zig");
const vm = @import("vm.zig");
const ops = @import("engine/value_ops.zig");
const shared = @import("engine/shared.zig");
const schema_ops = @import("engine/schema_ops.zig");
const select_exec = @import("engine/select_exec.zig");
const compound_select = @import("engine/compound_select.zig");
const aggregate = @import("engine/aggregate.zig");
const expr_eval = @import("engine/expr_eval.zig");
const stmt_mod = @import("engine/stmt.zig");

pub const RowSet = shared.RowSet;
pub const QueryMetrics = shared.QueryMetrics;
pub const Error = shared.Error;
pub const ExecResult = shared.ExecResult;
pub const RowState = shared.RowState;
pub const Stmt = stmt_mod.Stmt;

const Table = shared.Table;
const EvalCtx = shared.EvalCtx;
const ViewDef = shared.ViewDef;
const TriggerDef = shared.TriggerDef;
const IndexDef = shared.IndexDef;
const QueryRuntime = shared.QueryRuntime;

pub const Engine = struct {
    allocator: std.mem.Allocator,
    tables: std.StringHashMap(Table),
    views: std.StringHashMap(ViewDef),
    indexes: std.StringHashMap(IndexDef),
    triggers: std.StringHashMap(TriggerDef),
    metrics_data: QueryMetrics,
    metrics_enabled: bool,

    pub fn init(allocator: std.mem.Allocator) Engine {
        return .{
            .allocator = allocator,
            .tables = std.StringHashMap(Table).init(allocator),
            .views = std.StringHashMap(ViewDef).init(allocator),
            .indexes = std.StringHashMap(IndexDef).init(allocator),
            .triggers = std.StringHashMap(TriggerDef).init(allocator),
            .metrics_data = .{},
            .metrics_enabled = false,
        };
    }

    pub fn deinit(self: *Engine) void {
        schema_ops.deinitEngine(self);
    }

    pub fn exec(self: *Engine, sql_text: []const u8) Error!ExecResult {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);
        switch (stmt) {
            .create_table => |create| {
                try self.handleCreate(create);
                return .{ .rows_affected = 0 };
            },
            .create_index => |create_index| {
                try self.handleCreateIndex(create_index);
                return .{ .rows_affected = 0 };
            },
            .create_view => |create_view| {
                try self.handleCreateView(create_view);
                return .{ .rows_affected = 0 };
            },
            .create_trigger => |create_trigger| {
                try self.handleCreateTrigger(create_trigger);
                return .{ .rows_affected = 0 };
            },
            .insert => |ins| {
                try self.handleInsert(ins);
                return .{ .rows_affected = 1 };
            },
            .update => |upd| return .{ .rows_affected = try self.handleUpdate(upd) },
            .delete => |del| return .{ .rows_affected = try self.handleDelete(del) },
            .drop_table => |drop_table| {
                try self.handleDropTable(drop_table);
                return .{ .rows_affected = 0 };
            },
            .drop_index => |drop_index| {
                try self.handleDropIndex(drop_index);
                return .{ .rows_affected = 0 };
            },
            .drop_trigger => |drop_trigger| {
                try self.handleDropTrigger(drop_trigger);
                return .{ .rows_affected = 0 };
            },
            .drop_view => |drop_view| {
                try self.handleDropView(drop_view);
                return .{ .rows_affected = 0 };
            },
            .reindex => |reindex| {
                try self.handleReindex(reindex);
                return .{ .rows_affected = 0 };
            },
            .select => return Error.UnsupportedSql,
            .compound_select => return Error.UnsupportedSql,
        }
    }

    pub fn query(self: *Engine, allocator: std.mem.Allocator, sql_text: []const u8) Error!RowSet {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);
        return self.queryParsedWithParent(allocator, stmt, null, null);
    }

    pub fn resetMetrics(self: *Engine) void {
        self.metrics_data = .{};
        self.metrics_enabled = true;
    }

    pub fn metrics(self: *const Engine) QueryMetrics {
        return self.metrics_data;
    }

    fn handleCreate(self: *Engine, create: sql.CreateTable) Error!void {
        return schema_ops.handleCreate(self, create);
    }

    fn handleCreateIndex(self: *Engine, create: sql.CreateIndex) Error!void {
        return schema_ops.handleCreateIndex(self, create);
    }

    fn handleCreateView(self: *Engine, create: sql.CreateView) Error!void {
        return schema_ops.handleCreateView(self, create);
    }

    fn handleCreateTrigger(self: *Engine, create: sql.CreateTrigger) Error!void {
        return schema_ops.handleCreateTrigger(self, create);
    }

    fn handleInsert(self: *Engine, ins: sql.Insert) Error!void {
        return schema_ops.handleInsert(self, ins);
    }

    fn handleUpdate(self: *Engine, upd: sql.Update) Error!usize {
        return schema_ops.handleUpdate(self, upd);
    }

    fn handleDelete(self: *Engine, del: sql.Delete) Error!usize {
        return schema_ops.handleDelete(self, del);
    }

    fn handleDropTable(self: *Engine, drop_table: sql.DropObject) Error!void {
        return schema_ops.handleDropTable(self, drop_table);
    }

    fn handleDropIndex(self: *Engine, drop_index: sql.DropObject) Error!void {
        return schema_ops.handleDropIndex(self, drop_index);
    }

    fn handleDropTrigger(self: *Engine, drop_trigger: sql.DropObject) Error!void {
        return schema_ops.handleDropTrigger(self, drop_trigger);
    }

    fn handleDropView(self: *Engine, drop_view: sql.DropObject) Error!void {
        return schema_ops.handleDropView(self, drop_view);
    }

    fn handleReindex(self: *Engine, reindex: sql.Reindex) Error!void {
        return schema_ops.handleReindex(self, reindex);
    }

    pub fn materializeView(
        self: *Engine,
        allocator: std.mem.Allocator,
        view: *const ViewDef,
        parent_ctx: ?*const EvalCtx,
        runtime: *QueryRuntime,
        temp_tables: *std.ArrayList(*Table),
    ) Error!*Table {
        return schema_ops.materializeView(self, allocator, view, parent_ctx, runtime, temp_tables);
    }

    fn executeSelect(
        self: *Engine,
        allocator: std.mem.Allocator,
        sel: sql.Select,
        parent_ctx: ?*const EvalCtx,
        runtime: ?*QueryRuntime,
    ) Error!RowSet {
        return select_exec.executeSelect(self, allocator, sel, parent_ctx, runtime);
    }

    fn executeCompoundSelect(
        self: *Engine,
        allocator: std.mem.Allocator,
        compound: sql.CompoundSelect,
        parent_ctx: ?*const EvalCtx,
    ) Error!RowSet {
        return compound_select.executeCompoundSelect(self, allocator, compound, parent_ctx);
    }

    pub fn evalAggregateRow(
        self: *Engine,
        allocator: std.mem.Allocator,
        table: *const Table,
        sel: sql.Select,
        projections: []const *@import("expr/mod.zig").Expr,
        where_expr: ?*@import("expr/mod.zig").Expr,
        parent_ctx: ?*const EvalCtx,
        alias: []const u8,
        runtime: *QueryRuntime,
    ) Error![]@import("value.zig").Value {
        return aggregate.evalAggregateRow(self, allocator, table, sel, projections, where_expr, parent_ctx, alias, runtime);
    }

    pub fn evalExpr(
        self: *Engine,
        allocator: std.mem.Allocator,
        node: *@import("expr/mod.zig").Expr,
        ctx: *const EvalCtx,
        runtime: *QueryRuntime,
    ) Error!@import("value.zig").Value {
        return expr_eval.evalExpr(self, allocator, node, ctx, runtime);
    }

    fn queryWithParent(self: *Engine, allocator: std.mem.Allocator, sql_text: []const u8, parent: *const EvalCtx) Error!RowSet {
        return self.queryTextWithParent(allocator, sql_text, parent, null);
    }

    pub fn queryTextWithParent(
        self: *Engine,
        allocator: std.mem.Allocator,
        sql_text: []const u8,
        parent: ?*const EvalCtx,
        runtime: ?*QueryRuntime,
    ) Error!RowSet {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);
        return self.queryParsedWithParent(allocator, stmt, parent, runtime);
    }

    pub fn queryParsedWithParent(
        self: *Engine,
        allocator: std.mem.Allocator,
        stmt: sql.Statement,
        parent: ?*const EvalCtx,
        runtime: ?*QueryRuntime,
    ) Error!RowSet {
        return switch (stmt) {
            .select => |sel| try self.executeSelect(allocator, sel, parent, runtime),
            .compound_select => |compound| try self.executeCompoundSelect(allocator, compound, parent),
            else => Error.InvalidSql,
        };
    }

    pub fn getParsedSubquery(_: *Engine, runtime: *QueryRuntime, sql_text: []const u8) Error!sql.Statement {
        if (runtime.parsed_subquery.get(sql_text)) |stmt| return stmt;

        const arena_allocator = runtime.parse_arena.allocator();
        const parsed = sql.parse(arena_allocator, sql_text) catch |err| return mapParseError(err);
        const owned_key = try arena_allocator.dupe(u8, sql_text);
        try runtime.parsed_subquery.put(owned_key, parsed);
        return parsed;
    }

    pub fn resolveIdentifier(_: *Engine, ctx: *const EvalCtx, qualifier: ?[]const u8, name: []const u8) Error!@import("value.zig").Value {
        var cur: ?*const EvalCtx = ctx;
        while (cur) |c| {
            const matches_qualifier = if (qualifier) |q|
                if (c.alias) |a| ops.eqlIgnoreCase(q, a) else ops.eqlIgnoreCase(q, c.table_name)
            else
                true;
            if (matches_qualifier and c.row != null) {
                if (vm.columnIndex(c.table.columns.items, name)) |idx| return c.row.?[idx];
            }
            cur = c.parent;
        }
        return Error.UnknownColumn;
    }

    pub fn prepare(self: *Engine, allocator: std.mem.Allocator, sql_text: []const u8) Error!Stmt {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);

        return switch (stmt) {
            .select, .compound_select => .{ .row_set = try self.query(allocator, sql_text), .cursor = 0 },
            else => blk: {
                try self.exec(sql_text);
                break :blk .{ .row_set = null, .cursor = 0 };
            },
        };
    }
};

fn mapParseError(err: anyerror) Error {
    return switch (err) {
        sql.ParseError.InvalidSql => Error.InvalidSql,
        sql.ParseError.UnsupportedSql => Error.UnsupportedSql,
        sql.ParseError.InvalidLiteral => Error.InvalidLiteral,
        sql.ParseError.OutOfMemory => Error.OutOfMemory,
        else => Error.InvalidSql,
    };
}
