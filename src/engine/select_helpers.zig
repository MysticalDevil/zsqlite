const std = @import("std");
const vm = @import("../vm.zig");
const expr_mod = @import("../expr/mod.zig");

pub const SelectHelperError = error{ UnknownColumn, OutOfMemory };

pub fn parseQualifiedStar(text: []const u8) ?[]const u8 {
    if (text.len < 3) return null;
    if (text[text.len - 2] != '.' or text[text.len - 1] != '*') return null;
    const qualifier = std.mem.trim(u8, text[0 .. text.len - 2], " \t\r\n");
    if (qualifier.len == 0) return null;
    return qualifier;
}

pub fn findQualifiedSourceIndex(sources: anytype, qualifier: []const u8, eqlIgnoreCase: fn ([]const u8, []const u8) bool) ?usize {
    var found: ?usize = null;
    for (sources, 0..) |source, i| {
        const matches = sourceMatchesQualifier(source, qualifier, eqlIgnoreCase);
        if (!matches) continue;
        if (found != null) return null;
        found = i;
    }
    return found;
}

pub fn sourceMatchesQualifier(source: anytype, qualifier: []const u8, eqlIgnoreCase: fn ([]const u8, []const u8) bool) bool {
    if (eqlIgnoreCase(qualifier, source.table_name)) return true;
    if (source.alias) |alias| {
        return eqlIgnoreCase(qualifier, alias);
    }
    return false;
}

pub const ExprDependency = struct {
    supported: bool,
    mask: u64,
};

pub fn collectAndConjuncts(
    allocator: std.mem.Allocator,
    node: *expr_mod.Expr,
    out: *std.ArrayList(*expr_mod.Expr),
) SelectHelperError!void {
    switch (node.*) {
        .binary => |b| {
            if (b.op == .and_op) {
                try collectAndConjuncts(allocator, b.left, out);
                try collectAndConjuncts(allocator, b.right, out);
                return;
            }
        },
        else => {},
    }
    try out.append(allocator, node);
}

pub fn exprDependency(
    node: *expr_mod.Expr,
    sources: anytype,
    eqlIgnoreCase: fn ([]const u8, []const u8) bool,
) SelectHelperError!ExprDependency {
    return switch (node.*) {
        .literal => .{ .supported = true, .mask = 0 },
        .ident => |id| .{ .supported = true, .mask = @as(u64, 1) << @intCast(try resolveIdentifierSourceIndex(sources, id.qualifier, id.name, eqlIgnoreCase)) },
        .cast_expr => |c| try exprDependency(c.expr, sources, eqlIgnoreCase),
        .unary => |u| try exprDependency(u.expr, sources, eqlIgnoreCase),
        .binary => |b| combineDependency(try exprDependency(b.left, sources, eqlIgnoreCase), try exprDependency(b.right, sources, eqlIgnoreCase)),
        .between => |b| combineDependency(
            combineDependency(try exprDependency(b.target, sources, eqlIgnoreCase), try exprDependency(b.low, sources, eqlIgnoreCase)),
            try exprDependency(b.high, sources, eqlIgnoreCase),
        ),
        .is_null => |n| try exprDependency(n.target, sources, eqlIgnoreCase),
        .in_list => |n| blk: {
            if (n.subquery != null) break :blk .{ .supported = false, .mask = 0 };
            var dep = try exprDependency(n.target, sources, eqlIgnoreCase);
            for (n.items) |item| {
                dep = combineDependency(dep, try exprDependency(item, sources, eqlIgnoreCase));
            }
            break :blk dep;
        },
        .call => |c| blk: {
            var dep = ExprDependency{ .supported = true, .mask = 0 };
            for (c.args) |arg| {
                dep = combineDependency(dep, try exprDependency(arg, sources, eqlIgnoreCase));
            }
            break :blk dep;
        },
        .case_expr => |c| blk: {
            var dep = ExprDependency{ .supported = true, .mask = 0 };
            if (c.base) |base| dep = combineDependency(dep, try exprDependency(base, sources, eqlIgnoreCase));
            for (c.whens) |w| {
                dep = combineDependency(dep, try exprDependency(w.cond, sources, eqlIgnoreCase));
                dep = combineDependency(dep, try exprDependency(w.value, sources, eqlIgnoreCase));
            }
            if (c.else_expr) |e| dep = combineDependency(dep, try exprDependency(e, sources, eqlIgnoreCase));
            break :blk dep;
        },
        .subquery, .exists_subquery => .{ .supported = false, .mask = 0 },
    };
}

pub fn combineDependency(a: ExprDependency, b: ExprDependency) ExprDependency {
    if (!a.supported or !b.supported) return .{ .supported = false, .mask = 0 };
    return .{ .supported = true, .mask = a.mask | b.mask };
}

pub fn resolveIdentifierSourceIndex(
    sources: anytype,
    qualifier: ?[]const u8,
    name: []const u8,
    eqlIgnoreCase: fn ([]const u8, []const u8) bool,
) SelectHelperError!usize {
    if (qualifier) |q| {
        const idx = findQualifiedSourceIndex(sources, q, eqlIgnoreCase) orelse return SelectHelperError.UnknownColumn;
        if (vm.columnIndex(sources[idx].table.columns.items, name) == null) return SelectHelperError.UnknownColumn;
        return idx;
    }

    var found: ?usize = null;
    for (sources, 0..) |source, i| {
        if (vm.columnIndex(source.table.columns.items, name) != null) {
            if (found != null) return SelectHelperError.UnknownColumn;
            found = i;
        }
    }
    return found orelse SelectHelperError.UnknownColumn;
}
