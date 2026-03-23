const std = @import("std");
const Value = @import("../value.zig").Value;
const expr_mod = @import("../expr/mod.zig");

pub fn toNumber(v: Value) ?f64 {
    return switch (v) {
        .integer => |i| @as(f64, @floatFromInt(i)),
        .real => |f| f,
        .text => |t| std.fmt.parseFloat(f64, t) catch null,
        else => null,
    };
}

pub fn toSqlBool(v: Value) bool {
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

pub fn evalBinary(op: expr_mod.BinaryOp, l: Value, r: Value) Value {
    switch (op) {
        .add, .sub, .mul, .div => {
            if (l == .null or r == .null) return .null;
            if (l == .integer and r == .integer) {
                const li = l.integer;
                const ri = r.integer;
                switch (op) {
                    .add => return .{ .integer = li + ri },
                    .sub => return .{ .integer = li - ri },
                    .mul => return .{ .integer = li * ri },
                    .div => {
                        if (ri == 0) return .null;
                        return .{ .integer = @divTrunc(li, ri) };
                    },
                    else => unreachable,
                }
            }

            const ln = toNumber(l) orelse return .null;
            const rn = toNumber(r) orelse return .null;
            if (op == .div and rn == 0) return .null;
            switch (op) {
                .add => return .{ .real = ln + rn },
                .sub => return .{ .real = ln - rn },
                .mul => return .{ .real = ln * rn },
                .div => return .{ .real = ln / rn },
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

pub fn compareValues(a: Value, b: Value) i8 {
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

pub fn containsAggregateCall(node: *expr_mod.Expr) bool {
    return switch (node.*) {
        .call => |c| eqlIgnoreCase(c.name, "count") or
            eqlIgnoreCase(c.name, "avg") or
            eqlIgnoreCase(c.name, "sum") or
            eqlIgnoreCase(c.name, "total") or
            eqlIgnoreCase(c.name, "min") or
            eqlIgnoreCase(c.name, "max") or
            eqlIgnoreCase(c.name, "group_concat"),
        .cast_expr => |c| containsAggregateCall(c.expr),
        .unary => |u| containsAggregateCall(u.expr),
        .binary => |b| containsAggregateCall(b.left) or containsAggregateCall(b.right),
        .between => |b| containsAggregateCall(b.target) or containsAggregateCall(b.low) or containsAggregateCall(b.high),
        .is_null => |n| containsAggregateCall(n.target),
        .in_list => |n| blk: {
            if (containsAggregateCall(n.target)) break :blk true;
            if (n.subquery != null) break :blk false;
            for (n.items) |item| {
                if (containsAggregateCall(item)) break :blk true;
            }
            break :blk false;
        },
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

pub fn isAggregateProjectionList(nodes: []const *expr_mod.Expr) bool {
    for (nodes) |n| {
        if (containsAggregateCall(n)) return true;
    }
    return false;
}

pub fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (std.ascii.toLower(ca) != std.ascii.toLower(cb)) return false;
    }
    return true;
}
