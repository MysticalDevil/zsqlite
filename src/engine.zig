const std = @import("std");
const Value = @import("value.zig").Value;
const sql = @import("sql.zig");
const vm = @import("vm.zig");
const expr_mod = @import("expr/mod.zig");
const ops = @import("engine/value_ops.zig");

pub const Error = error{
    OutOfMemory,
    TableAlreadyExists,
    UnknownTable,
    ColumnCountMismatch,
    UnknownColumn,
    InvalidSql,
    UnsupportedSql,
    InvalidLiteral,
};

pub const ExecResult = struct {
    rows_affected: usize = 0,
};

pub const RowState = enum { row, done };

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

const Table = struct {
    name: []const u8,
    columns: std.ArrayList([]const u8),
    rows: std.ArrayList([]Value),

    fn deinit(self: *Table, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        for (self.columns.items) |c| allocator.free(c);
        self.columns.deinit(allocator);
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

const EvalCtx = struct {
    table: *const Table,
    table_name: []const u8,
    alias: ?[]const u8,
    row: ?[]const Value,
    parent: ?*const EvalCtx,
};

const SortRow = struct {
    values: []Value,
    keys: []Value,
};

pub const Engine = struct {
    allocator: std.mem.Allocator,
    tables: std.StringHashMap(Table),

    pub fn init(allocator: std.mem.Allocator) Engine {
        return .{ .allocator = allocator, .tables = std.StringHashMap(Table).init(allocator) };
    }

    pub fn deinit(self: *Engine) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var table = entry.value_ptr.*;
            table.deinit(self.allocator);
        }
        self.tables.deinit();
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
            .insert => |ins| {
                try self.handleInsert(ins);
                return .{ .rows_affected = 1 };
            },
            .select => return Error.UnsupportedSql,
        }
    }

    pub fn query(self: *Engine, allocator: std.mem.Allocator, sql_text: []const u8) Error!RowSet {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);
        return switch (stmt) {
            .select => |sel| try self.executeSelect(allocator, sel, null),
            else => Error.UnsupportedSql,
        };
    }

    fn handleCreate(self: *Engine, create: sql.CreateTable) Error!void {
        const key = try self.allocator.dupe(u8, create.table_name);
        errdefer self.allocator.free(key);
        const gop = try self.tables.getOrPut(key);
        if (gop.found_existing) {
            return Error.TableAlreadyExists;
        }

        var table = Table{
            .name = try self.allocator.dupe(u8, create.table_name),
            .columns = std.ArrayList([]const u8).empty,
            .rows = std.ArrayList([]Value).empty,
        };
        for (create.columns) |col| {
            try table.columns.append(self.allocator, try self.allocator.dupe(u8, col));
        }
        gop.value_ptr.* = table;
    }

    fn handleInsert(self: *Engine, ins: sql.Insert) Error!void {
        const table = self.tables.getPtr(ins.table_name) orelse return Error.UnknownTable;
        const row = try self.allocator.alloc(Value, table.columns.items.len);
        for (row) |*cell| cell.* = .null;

        if (ins.columns) |cols| {
            if (ins.values.len != cols.len) return Error.ColumnCountMismatch;
            for (cols, ins.values) |col_name, v| {
                const idx = vm.columnIndex(table.columns.items, col_name) orelse return Error.UnknownColumn;
                row[idx] = try v.clone(self.allocator);
            }
        } else {
            if (ins.values.len != table.columns.items.len) return Error.ColumnCountMismatch;
            for (ins.values, 0..) |v, i| row[i] = try v.clone(self.allocator);
        }
        try table.rows.append(self.allocator, row);
    }

    fn executeSelect(
        self: *Engine,
        allocator: std.mem.Allocator,
        sel: sql.Select,
        parent_ctx: ?*const EvalCtx,
    ) Error!RowSet {
        var result = RowSet.init(allocator);
        errdefer result.deinit();

        const table = self.tables.getPtr(sel.table_name) orelse return Error.UnknownTable;
        const alias = if (sel.table_alias) |a| a else sel.table_name;

        var expr_arena = std.heap.ArenaAllocator.init(allocator);
        defer expr_arena.deinit();
        const ea = expr_arena.allocator();

        var projections = std.ArrayList(*expr_mod.Expr).empty;
        defer projections.deinit(ea);
        for (sel.projections) |text| {
            const parsed = expr_mod.parse(ea, text) catch return Error.InvalidSql;
            try projections.append(ea, parsed);
        }

        const where_expr = if (sel.where_expr) |w|
            expr_mod.parse(ea, w) catch return Error.InvalidSql
        else
            null;

        var order_exprs = std.ArrayList(?*expr_mod.Expr).empty;
        defer order_exprs.deinit(ea);
        for (sel.order_by) |term| {
            if (term.is_ordinal) {
                try order_exprs.append(ea, null);
            } else {
                const oe = expr_mod.parse(ea, term.expr) catch return Error.InvalidSql;
                try order_exprs.append(ea, oe);
            }
        }

        if (isAggregateProjectionList(projections.items)) {
            const row = try self.evalAggregateRow(allocator, table, sel, projections.items, where_expr, parent_ctx, alias);
            try result.rows.append(allocator, row);
            return result;
        }

        var rows = std.ArrayList(SortRow).empty;
        defer {
            for (rows.items) |entry| {
                allocator.free(entry.keys);
            }
            rows.deinit(allocator);
        }

        for (table.rows.items) |source_row| {
            var ctx = EvalCtx{
                .table = table,
                .table_name = sel.table_name,
                .alias = alias,
                .row = source_row,
                .parent = parent_ctx,
            };

            if (where_expr) |w| {
                const cond_val = try self.evalExpr(allocator, w, &ctx);
                const cond = toSqlBool(cond_val);
                if (!cond) continue;
            }

            const out_row = try allocator.alloc(Value, projections.items.len);
            for (projections.items, 0..) |pexpr, i| {
                const val = try self.evalExpr(allocator, pexpr, &ctx);
                out_row[i] = try cloneResultValue(allocator, val);
            }

            const keys = try allocator.alloc(Value, sel.order_by.len);
            for (sel.order_by, 0..) |term, i| {
                if (term.is_ordinal) {
                    if (term.ordinal == 0 or term.ordinal > out_row.len) return Error.InvalidSql;
                    keys[i] = try cloneResultValue(allocator, out_row[term.ordinal - 1]);
                } else {
                    const oexpr = order_exprs.items[i] orelse return Error.InvalidSql;
                    const kv = try self.evalExpr(allocator, oexpr, &ctx);
                    keys[i] = try cloneResultValue(allocator, kv);
                }
            }

            try rows.append(allocator, .{ .values = out_row, .keys = keys });
        }

        if (sel.order_by.len > 0) {
            std.sort.insertion(SortRow, rows.items, {}, sortRowLessThan);
        }

        for (rows.items) |entry| {
            try result.rows.append(allocator, entry.values);
        }
        return result;
    }

    fn evalAggregateRow(
        self: *Engine,
        allocator: std.mem.Allocator,
        table: *const Table,
        sel: sql.Select,
        projections: []const *expr_mod.Expr,
        where_expr: ?*expr_mod.Expr,
        parent_ctx: ?*const EvalCtx,
        alias: []const u8,
    ) Error![]Value {
        const row = try allocator.alloc(Value, projections.len);
        var seen_any = false;

        for (projections, 0..) |pexpr, i| {
            row[i] = try self.evalAggregateExpr(allocator, pexpr, table, sel, where_expr, parent_ctx, alias, &seen_any);
        }
        return row;
    }

    fn evalAggregateExpr(
        self: *Engine,
        allocator: std.mem.Allocator,
        pexpr: *expr_mod.Expr,
        table: *const Table,
        sel: sql.Select,
        where_expr: ?*expr_mod.Expr,
        parent_ctx: ?*const EvalCtx,
        alias: []const u8,
        seen_any_ptr: *bool,
    ) Error!Value {
        switch (pexpr.*) {
            .call => |call| {
                if (eqlIgnoreCase(call.name, "count")) {
                    var count: i64 = 0;
                    for (table.rows.items) |source_row| {
                        var ctx = EvalCtx{
                            .table = table,
                            .table_name = sel.table_name,
                            .alias = alias,
                            .row = source_row,
                            .parent = parent_ctx,
                        };
                        if (where_expr) |w| {
                            const cond = try self.evalExpr(allocator, w, &ctx);
                            if (!toSqlBool(cond)) continue;
                        }
                        seen_any_ptr.* = true;
                        if (call.star_arg) {
                            count += 1;
                        } else {
                            if (call.args.len != 1) return Error.InvalidSql;
                            const v = try self.evalExpr(allocator, call.args[0], &ctx);
                            if (v != .null) count += 1;
                        }
                    }
                    return Value{ .integer = count };
                }
                if (eqlIgnoreCase(call.name, "avg")) {
                    if (call.args.len != 1) return Error.InvalidSql;
                    var sum: f64 = 0;
                    var cnt: usize = 0;
                    for (table.rows.items) |source_row| {
                        var ctx = EvalCtx{
                            .table = table,
                            .table_name = sel.table_name,
                            .alias = alias,
                            .row = source_row,
                            .parent = parent_ctx,
                        };
                        if (where_expr) |w| {
                            const cond = try self.evalExpr(allocator, w, &ctx);
                            if (!toSqlBool(cond)) continue;
                        }
                        const v = try self.evalExpr(allocator, call.args[0], &ctx);
                        if (toNumber(v)) |num| {
                            sum += num;
                            cnt += 1;
                            seen_any_ptr.* = true;
                        }
                    }
                    if (cnt == 0) return .null;
                    return Value{ .real = sum / @as(f64, @floatFromInt(cnt)) };
                }
                return Error.UnsupportedSql;
            },
            else => {
                if (!seen_any_ptr.*) return .null;
                return Error.UnsupportedSql;
            },
        }
    }

    fn evalExpr(self: *Engine, allocator: std.mem.Allocator, node: *expr_mod.Expr, ctx: *const EvalCtx) Error!Value {
        switch (node.*) {
            .literal => |v| return v,
            .ident => |id| return self.resolveIdentifier(ctx, id.qualifier, id.name),
            .unary => |u| {
                const inner = try self.evalExpr(allocator, u.expr, ctx);
                switch (u.op) {
                    .neg => {
                        if (inner == .integer) {
                            return Value{ .integer = -inner.integer };
                        }
                        if (inner == .real) {
                            return Value{ .real = -inner.real };
                        }
                        if (ops.toNumber(inner)) |num| {
                            return Value{ .real = -num };
                        }
                        return .null;
                    },
                    .not_op => {
                        const b = toSqlBool(inner);
                        return Value{ .integer = if (b) 0 else 1 };
                    },
                }
            },
            .binary => |b| {
                const l = try self.evalExpr(allocator, b.left, ctx);
                const r = try self.evalExpr(allocator, b.right, ctx);
                return evalBinary(b.op, l, r);
            },
            .between => |b| {
                const t = try self.evalExpr(allocator, b.target, ctx);
                const lo = try self.evalExpr(allocator, b.low, ctx);
                const hi = try self.evalExpr(allocator, b.high, ctx);
                if (t == .null or lo == .null or hi == .null) return .null;
                const left_cmp = compareValues(t, lo);
                const right_cmp = compareValues(t, hi);
                var ok = left_cmp >= 0 and right_cmp <= 0;
                if (b.not_between) ok = !ok;
                return Value{ .integer = if (ok) 1 else 0 };
            },
            .call => |call| {
                if (eqlIgnoreCase(call.name, "abs")) {
                    if (call.args.len != 1) return Error.InvalidSql;
                    const v = try self.evalExpr(allocator, call.args[0], ctx);
                    if (v == .integer) {
                        const i = v.integer;
                        if (i == std.math.minInt(i64)) return Error.InvalidSql;
                        return Value{ .integer = if (i < 0) -i else i };
                    }
                    if (v == .real) return Value{ .real = @abs(v.real) };
                    if (toNumber(v)) |n| return Value{ .real = @abs(n) };
                    return .null;
                }
                return Error.UnsupportedSql;
            },
            .case_expr => |c| {
                if (c.base) |base_expr| {
                    const base = try self.evalExpr(allocator, base_expr, ctx);
                    for (c.whens) |w| {
                        const when_val = try self.evalExpr(allocator, w.cond, ctx);
                        const cmp = evalBinary(.eq, base, when_val);
                        if (toSqlBool(cmp)) return self.evalExpr(allocator, w.value, ctx);
                    }
                } else {
                    for (c.whens) |w| {
                        const cond = try self.evalExpr(allocator, w.cond, ctx);
                        if (toSqlBool(cond)) return self.evalExpr(allocator, w.value, ctx);
                    }
                }
                if (c.else_expr) |e| return self.evalExpr(allocator, e, ctx);
                return .null;
            },
            .subquery => |sql_text| {
                var rs = try self.queryWithParent(allocator, sql_text, ctx);
                defer rs.deinit();
                if (rs.rows.items.len == 0) return .null;
                if (rs.rows.items[0].len == 0) return .null;
                return rs.rows.items[0][0];
            },
            .exists_subquery => |sql_text| {
                var rs = try self.queryWithParent(allocator, sql_text, ctx);
                defer rs.deinit();
                return Value{ .integer = if (rs.rows.items.len > 0) 1 else 0 };
            },
        }
    }

    fn queryWithParent(self: *Engine, allocator: std.mem.Allocator, sql_text: []const u8, parent: *const EvalCtx) Error!RowSet {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);
        return switch (stmt) {
            .select => |sel| try self.executeSelect(allocator, sel, parent),
            else => Error.InvalidSql,
        };
    }

    fn resolveIdentifier(_: *Engine, ctx: *const EvalCtx, qualifier: ?[]const u8, name: []const u8) Error!Value {
        var cur: ?*const EvalCtx = ctx;
        while (cur) |c| {
            const matches_qualifier = if (qualifier) |q|
                if (c.alias) |a|
                    eqlIgnoreCase(q, a)
                else
                    eqlIgnoreCase(q, c.table_name)
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
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return Engine.mapParseError(err);

        return switch (stmt) {
            .select => .{ .row_set = try self.query(allocator, sql_text), .cursor = 0 },
            else => blk: {
                try self.exec(sql_text);
                break :blk .{ .row_set = null, .cursor = 0 };
            },
        };
    }

    fn mapParseError(err: anyerror) Error {
        return switch (err) {
            sql.ParseError.InvalidSql => Error.InvalidSql,
            sql.ParseError.UnsupportedSql => Error.UnsupportedSql,
            sql.ParseError.InvalidLiteral => Error.InvalidLiteral,
            sql.ParseError.OutOfMemory => Error.OutOfMemory,
            else => Error.InvalidSql,
        };
    }
};

pub const Stmt = struct {
    row_set: ?RowSet,
    cursor: usize,

    pub fn deinit(self: *Stmt) void {
        if (self.row_set) |*rs| rs.deinit();
    }

    pub fn step(self: *Stmt) RowState {
        const rs = &(self.row_set orelse return .done);
        if (self.cursor >= rs.rows.items.len) return .done;
        self.cursor += 1;
        return .row;
    }

    pub fn column(self: *Stmt, idx: usize) Value {
        const rs = &(self.row_set orelse unreachable);
        const row_idx = self.cursor - 1;
        return rs.rows.items[row_idx][idx];
    }

    pub fn bind(_: *Stmt, _: usize, _: Value) Error!void {
        return Error.UnsupportedSql;
    }
};

fn cloneResultValue(allocator: std.mem.Allocator, v: Value) Error!Value {
    return switch (v) {
        .text => |t| .{ .text = try allocator.dupe(u8, t) },
        .blob => |b| .{ .blob = try allocator.dupe(u8, b) },
        else => v,
    };
}

fn isAggregateProjectionList(nodes: []const *expr_mod.Expr) bool {
    for (nodes) |n| {
        if (containsAggregateCall(n)) return true;
    }
    return false;
}

fn containsAggregateCall(node: *expr_mod.Expr) bool {
    return switch (node.*) {
        .call => |c| eqlIgnoreCase(c.name, "count") or eqlIgnoreCase(c.name, "avg"),
        .unary => |u| containsAggregateCall(u.expr),
        .binary => |b| containsAggregateCall(b.left) or containsAggregateCall(b.right),
        .between => |b| containsAggregateCall(b.target) or containsAggregateCall(b.low) or containsAggregateCall(b.high),
        .case_expr => |c| blk: {
            if (c.base != null and containsAggregateCall(c.base.?)) break :blk true;
            for (c.whens) |w| {
                if (containsAggregateCall(w.cond) or containsAggregateCall(w.value)) break :blk true;
            }
            if (c.else_expr != null and containsAggregateCall(c.else_expr.?)) break :blk true;
            break :blk false;
        },
        .subquery, .exists_subquery, .literal, .ident => false,
    };
}

fn toNumber(v: Value) ?f64 {
    return switch (v) {
        .integer => |i| @as(f64, @floatFromInt(i)),
        .real => |f| f,
        .text => |t| std.fmt.parseFloat(f64, t) catch null,
        else => null,
    };
}

fn toSqlBool(v: Value) bool {
    return switch (v) {
        .null => false,
        .integer => |i| i != 0,
        .real => |f| f != 0,
        .text => |t| {
            if (std.fmt.parseFloat(f64, t)) |n| return n != 0 else |_| return t.len != 0;
        },
        .blob => |b| b.len != 0,
    };
}

fn evalBinary(op: expr_mod.BinaryOp, l: Value, r: Value) Value {
    switch (op) {
        .add, .sub, .mul, .div => {
            if (l == .null or r == .null) return .null;
            const ln = toNumber(l) orelse return .null;
            const rn = toNumber(r) orelse return .null;
            switch (op) {
                .add => return .{ .real = ln + rn },
                .sub => return .{ .real = ln - rn },
                .mul => return .{ .real = ln * rn },
                .div => return .{ .real = if (rn == 0) 0 else ln / rn },
                else => unreachable,
            }
        },
        .eq => {
            if (l == .null or r == .null) return .null;
            return .{ .integer = if (l.eql(r)) 1 else 0 };
        },
        .ne => {
            if (l == .null or r == .null) return .null;
            return .{ .integer = if (l.eql(r)) 0 else 1 };
        },
        .lt, .le, .gt, .ge => {
            if (l == .null or r == .null) return .null;
            const cmp = compareValues(l, r);
            return switch (op) {
                .lt => .{ .integer = if (cmp < 0) 1 else 0 },
                .le => .{ .integer = if (cmp <= 0) 1 else 0 },
                .gt => .{ .integer = if (cmp > 0) 1 else 0 },
                .ge => .{ .integer = if (cmp >= 0) 1 else 0 },
                else => unreachable,
            };
        },
        .and_op => {
            const lb = toSqlBool(l);
            const rb = toSqlBool(r);
            return .{ .integer = if (lb and rb) 1 else 0 };
        },
        .or_op => {
            const lb = toSqlBool(l);
            const rb = toSqlBool(r);
            return .{ .integer = if (lb or rb) 1 else 0 };
        },
    }
}

fn compareValues(a: Value, b: Value) i8 {
    if (a == .null and b == .null) return 0;
    if (a == .null) return -1;
    if (b == .null) return 1;

    if (toNumber(a)) |an| {
        if (toNumber(b)) |bn| {
            if (an < bn) return -1;
            if (an > bn) return 1;
            return 0;
        }
    }

    if (a == .text and b == .text) {
        const o = std.mem.order(u8, a.text, b.text);
        return switch (o) {
            .lt => -1,
            .eq => 0,
            .gt => 1,
        };
    }

    return 0;
}

fn sortRowLessThan(_: void, a: SortRow, b: SortRow) bool {
    var i: usize = 0;
    while (i < a.keys.len and i < b.keys.len) : (i += 1) {
        const cmp = compareValues(a.keys[i], b.keys[i]);
        if (cmp < 0) return true;
        if (cmp > 0) return false;
    }
    return false;
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (std.ascii.toLower(ca) != std.ascii.toLower(cb)) return false;
    }
    return true;
}
