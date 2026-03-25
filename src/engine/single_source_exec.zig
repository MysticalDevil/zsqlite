const std = @import("std");
const result_utils = @import("result_utils.zig");
const shared = @import("shared.zig");
const select_helpers = @import("select_helpers.zig");
const ops = @import("value_ops.zig");
const expr_mod = @import("../expr/mod.zig");
const vm = @import("../vm.zig");

const RowSet = shared.RowSet;
const EvalCtx = shared.EvalCtx;
const QueryRuntime = shared.QueryRuntime;
const SourceRef = shared.SourceRef;
const Error = shared.Error;
const Value = @import("../value.zig").Value;

pub fn executeSingleSourceNoOrderNoDistinct(
    self: anytype,
    allocator: std.mem.Allocator,
    source: SourceRef,
    projections: anytype,
    where_expr: ?*@import("../expr/mod.zig").Expr,
    parent_ctx: ?*const EvalCtx,
    runtime: *QueryRuntime,
) Error!RowSet {
    var result = RowSet.init(allocator);
    errdefer result.deinit();

    var bound_arena: std.heap.ArenaAllocator = undefined;
    var bound_alloc: std.mem.Allocator = undefined;
    var arena_used = false;

    var active_where_expr = where_expr;
    var bound_where_expr: ?*BoundExpr = null;

    if (where_expr) |w| {
        if (maybeFoldWhereConst(w, source.table)) |folded| {
            if (!folded) return result;
            active_where_expr = null;
        } else {
            const only_source = [_]SourceRef{source};
            const dep = select_helpers.exprDependency(w, only_source[0..], ops.eqlIgnoreCase) catch null;
            if (dep != null and dep.?.supported and dep.?.mask == 0) {
                var ctx = EvalCtx{
                    .table = source.table,
                    .table_name = source.table_name,
                    .alias = source.alias,
                    .row = null,
                    .parent = parent_ctx,
                };
                const cond = self.evalExpr(allocator, w, &ctx, runtime) catch .null;
                if (!ops.toSqlBool(cond)) return result;
                active_where_expr = null;
            } else {
                bound_arena = std.heap.ArenaAllocator.init(allocator);
                bound_alloc = bound_arena.allocator();
                arena_used = true;
                bound_where_expr = try bindWhereExpr(bound_alloc, w, source);
            }
        }
    }

    var out_count: usize = 0;
    var qualified_matches = try allocator.alloc(bool, projections.len);
    defer allocator.free(qualified_matches);

    for (projections, 0..) |p, i| {
        switch (p.kind) {
            .expr => {
                qualified_matches[i] = false;
                out_count += 1;
            },
            .star_all => {
                qualified_matches[i] = false;
                out_count += source.table.columns.items.len;
            },
            .star_qualifier => {
                const matches = select_helpers.sourceMatchesQualifier(source, p.qualifier.?, ops.eqlIgnoreCase);
                if (!matches) return Error.UnknownColumn;
                qualified_matches[i] = true;
                out_count += source.table.columns.items.len;
            },
        }
    }

    for (source.table.rows.items, 0..) |row, row_idx| {
        if (!source.table.isRowLive(row_idx)) continue;

        var ctx = EvalCtx{
            .table = source.table,
            .table_name = source.table_name,
            .alias = source.alias,
            .row = row,
            .parent = parent_ctx,
        };

        if (bound_where_expr) |bound| {
            const cond = evalBoundExpr(bound, row);
            if (!ops.toSqlBool(cond)) continue;
        } else if (active_where_expr) |w| {
            const cond = try self.evalExpr(allocator, w, &ctx, runtime);
            if (!ops.toSqlBool(cond)) continue;
        }

        const out_row = try allocator.alloc(Value, out_count);
        var out_idx: usize = 0;
        for (projections, 0..) |p, i| {
            switch (p.kind) {
                .expr => {
                    const value = try self.evalExpr(allocator, p.expr.?, &ctx, runtime);
                    out_row[out_idx] = try result_utils.cloneResultValue(allocator, value);
                    out_idx += 1;
                },
                .star_all => {
                    for (row) |v| {
                        out_row[out_idx] = try result_utils.cloneResultValue(allocator, v);
                        out_idx += 1;
                    }
                },
                .star_qualifier => {
                    if (!qualified_matches[i]) return Error.UnknownColumn;
                    for (row) |v| {
                        out_row[out_idx] = try result_utils.cloneResultValue(allocator, v);
                        out_idx += 1;
                    }
                },
            }
        }
        try result.rows.append(allocator, out_row);
    }

    if (arena_used) bound_arena.deinit();
    return result;
}

fn maybeFoldWhereConst(node: *@import("../expr/mod.zig").Expr, table: *const shared.Table) ?bool {
    return switch (node.*) {
        .literal => |v| ops.toSqlBool(v),
        .cast_expr => |c| maybeFoldWhereConst(c.expr, table),
        .unary => |u| switch (u.op) {
            .not_op => if (maybeFoldWhereConst(u.expr, table)) |inner| !inner else null,
            .neg => null,
        },
        .binary => |b| blk: {
            const left = maybeFoldWhereConst(b.left, table);
            const right = maybeFoldWhereConst(b.right, table);
            switch (b.op) {
                .and_op => if (left != null and right != null) break :blk left.? and right.? else break :blk null,
                .or_op => if (left != null and right != null) break :blk left.? or right.? else break :blk null,
                else => break :blk null,
            }
        },
        .is_null => |n| blk: {
            if (exprGuaranteedNonNull(n.target, table)) break :blk n.not_null;
            break :blk switch (n.target.*) {
                .literal => |v| if (v == .null) !n.not_null else n.not_null,
                else => null,
            };
        },
        else => null,
    };
}

fn exprGuaranteedNonNull(node: *@import("../expr/mod.zig").Expr, table: *const shared.Table) bool {
    return switch (node.*) {
        .literal => |v| v != .null,
        .ident => |id| blk: {
            if (id.qualifier != null) break :blk false;
            const idx = columnIndex(table.columns.items, id.name) orelse break :blk false;
            break :blk idx < table.column_has_null.items.len and !table.column_has_null.items[idx];
        },
        .cast_expr => |c| exprGuaranteedNonNull(c.expr, table),
        .unary => |u| exprGuaranteedNonNull(u.expr, table),
        .binary => |b| switch (b.op) {
            .add, .sub, .mul => exprGuaranteedNonNull(b.left, table) and exprGuaranteedNonNull(b.right, table),
            .eq, .ne, .lt, .le, .gt, .ge => exprGuaranteedNonNull(b.left, table) and exprGuaranteedNonNull(b.right, table),
            else => false,
        },
        .between => |b| exprGuaranteedNonNull(b.target, table) and exprGuaranteedNonNull(b.low, table) and exprGuaranteedNonNull(b.high, table),
        .is_null => true,
        else => false,
    };
}

fn columnIndex(columns: []const []const u8, name: []const u8) ?usize {
    for (columns, 0..) |column_name, idx| {
        if (std.mem.eql(u8, column_name, name)) return idx;
    }
    return null;
}

const BoundExpr = union(enum) {
    literal: @import("../value.zig").Value,
    column: usize,
    cast_expr: struct {
        expr: *BoundExpr,
        target_type: []const u8,
    },
    unary: struct {
        op: expr_mod.UnaryOp,
        expr: *BoundExpr,
    },
    binary: struct {
        op: expr_mod.BinaryOp,
        left: *BoundExpr,
        right: *BoundExpr,
    },
    between: struct {
        target: *BoundExpr,
        low: *BoundExpr,
        high: *BoundExpr,
        not_between: bool,
    },
    is_null: struct {
        target: *BoundExpr,
        not_null: bool,
    },
    in_list: struct {
        target: *BoundExpr,
        items: []const *BoundExpr,
        not_in: bool,
    },
};

fn bindWhereExpr(allocator: std.mem.Allocator, node: *expr_mod.Expr, source: SourceRef) Error!?*BoundExpr {
    const out = try allocator.create(BoundExpr);
    switch (node.*) {
        .literal => |v| {
            out.* = .{ .literal = v };
            return out;
        },
        .ident => |id| {
            if (id.qualifier) |qualifier| {
                if (!select_helpers.sourceMatchesQualifier(source, qualifier, ops.eqlIgnoreCase)) return null;
            }
            const idx = vm.columnIndex(source.table.columns.items, id.name) orelse return null;
            out.* = .{ .column = idx };
            return out;
        },
        .cast_expr => |c| {
            const inner = try bindWhereExpr(allocator, c.expr, source) orelse return null;
            out.* = .{ .cast_expr = .{
                .expr = inner,
                .target_type = c.target_type,
            } };
            return out;
        },
        .unary => |u| {
            const inner = try bindWhereExpr(allocator, u.expr, source) orelse return null;
            out.* = .{ .unary = .{ .op = u.op, .expr = inner } };
            return out;
        },
        .binary => |b| {
            const left = try bindWhereExpr(allocator, b.left, source) orelse return null;
            const right = try bindWhereExpr(allocator, b.right, source) orelse return null;
            out.* = .{ .binary = .{ .op = b.op, .left = left, .right = right } };
            return out;
        },
        .between => |b| {
            const target = try bindWhereExpr(allocator, b.target, source) orelse return null;
            const low = try bindWhereExpr(allocator, b.low, source) orelse return null;
            const high = try bindWhereExpr(allocator, b.high, source) orelse return null;
            out.* = .{ .between = .{
                .target = target,
                .low = low,
                .high = high,
                .not_between = b.not_between,
            } };
            return out;
        },
        .is_null => |n| {
            const target = try bindWhereExpr(allocator, n.target, source) orelse return null;
            out.* = .{ .is_null = .{ .target = target, .not_null = n.not_null } };
            return out;
        },
        .in_list => |n| {
            if (n.subquery != null) return null;
            const target = try bindWhereExpr(allocator, n.target, source) orelse return null;
            const items = try allocator.alloc(*BoundExpr, n.items.len);
            for (n.items, 0..) |item, i| {
                items[i] = try bindWhereExpr(allocator, item, source) orelse return null;
            }
            out.* = .{ .in_list = .{ .target = target, .items = items, .not_in = n.not_in } };
            return out;
        },
        else => return null,
    }
}

fn evalBoundExpr(node: *const BoundExpr, row: []const Value) Value {
    return switch (node.*) {
        .literal => |v| v,
        .column => |idx| if (idx < row.len) row[idx] else .null,
        .cast_expr => |c| evalBoundCast(c, row),
        .unary => |u| evalBoundUnary(u, row),
        .binary => |b| evalBoundBinary(b, row),
        .between => |b| evalBoundBetween(b, row),
        .is_null => |n| evalBoundIsNull(n, row),
        .in_list => |n| evalBoundInList(n, row),
    };
}

fn evalBoundCast(c: anytype, row: []const Value) Value {
    const value = evalBoundExpr(c.expr, row);
    if (value == .null) return .null;
    if (ops.eqlIgnoreCase(c.target_type, "INTEGER")) {
        return switch (value) {
            .integer => value,
            .real => |v| .{ .integer = @as(i64, @intFromFloat(v)) },
            .text => |v| blk: {
                const parsed_int = std.fmt.parseInt(i64, v, 10) catch {
                    const parsed_real = std.fmt.parseFloat(f64, v) catch 0;
                    break :blk .{ .integer = @as(i64, @intFromFloat(parsed_real)) };
                };
                break :blk .{ .integer = parsed_int };
            },
            .blob => .{ .integer = 0 },
            .null => .null,
        };
    }
    if (ops.eqlIgnoreCase(c.target_type, "REAL")) {
        if (ops.toNumber(value)) |num| return .{ .real = num };
        return .{ .real = 0 };
    }
    return value;
}

fn evalBoundUnary(u: anytype, row: []const Value) Value {
    const inner = evalBoundExpr(u.expr, row);
    return switch (u.op) {
        .neg => blk: {
            if (inner == .integer) break :blk .{ .integer = -inner.integer };
            if (inner == .real) break :blk .{ .real = -inner.real };
            if (ops.toNumber(inner)) |num| break :blk .{ .real = -num };
            break :blk .null;
        },
        .not_op => blk: {
            if (inner == .null) break :blk .null;
            break :blk .{ .integer = if (ops.toSqlBool(inner)) 0 else 1 };
        },
    };
}

fn evalBoundBinary(b: anytype, row: []const Value) Value {
    if (b.op == .and_op) {
        const left = evalBoundExpr(b.left, row);
        if (left != .null and !ops.toSqlBool(left)) return .{ .integer = 0 };
        const right = evalBoundExpr(b.right, row);
        if (right != .null and !ops.toSqlBool(right)) return .{ .integer = 0 };
        if (left == .null or right == .null) return .null;
        return .{ .integer = 1 };
    }
    if (b.op == .or_op) {
        const left = evalBoundExpr(b.left, row);
        if (left != .null and ops.toSqlBool(left)) return .{ .integer = 1 };
        const right = evalBoundExpr(b.right, row);
        if (right != .null and ops.toSqlBool(right)) return .{ .integer = 1 };
        if (left == .null or right == .null) return .null;
        return .{ .integer = 0 };
    }
    return ops.evalBinary(b.op, evalBoundExpr(b.left, row), evalBoundExpr(b.right, row));
}

fn evalBoundBetween(b: anytype, row: []const Value) Value {
    const target = evalBoundExpr(b.target, row);
    const low = evalBoundExpr(b.low, row);
    const high = evalBoundExpr(b.high, row);
    const ge = ops.evalBinary(.ge, target, low);
    const le = ops.evalBinary(.le, target, high);
    const between_value = sqlAnd(ge, le);
    if (b.not_between) return sqlNot(between_value);
    return between_value;
}

fn evalBoundIsNull(n: anytype, row: []const Value) Value {
    const value = evalBoundExpr(n.target, row);
    const is_null = value == .null;
    return .{ .integer = if (if (n.not_null) !is_null else is_null) 1 else 0 };
}

fn evalBoundInList(n: anytype, row: []const Value) Value {
    const target = evalBoundExpr(n.target, row);
    if (n.items.len == 0) return .{ .integer = if (n.not_in) 1 else 0 };
    if (target == .null) return .null;

    var has_null = false;
    for (n.items) |item| {
        const rhs = evalBoundExpr(item, row);
        if (rhs == .null) {
            has_null = true;
            continue;
        }
        if (ops.compareValues(target, rhs) == 0) {
            return .{ .integer = if (n.not_in) 0 else 1 };
        }
    }
    if (has_null) return .null;
    return .{ .integer = if (n.not_in) 1 else 0 };
}

fn sqlNot(value: Value) Value {
    if (value == .null) return .null;
    return .{ .integer = if (ops.toSqlBool(value)) 0 else 1 };
}

fn sqlAnd(left: Value, right: Value) Value {
    if (left != .null and !ops.toSqlBool(left)) return .{ .integer = 0 };
    if (right != .null and !ops.toSqlBool(right)) return .{ .integer = 0 };
    if (left == .null or right == .null) return .null;
    return .{ .integer = 1 };
}
