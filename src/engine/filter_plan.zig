const expr_mod = @import("../expr/mod.zig");
const vm = @import("../vm.zig");
const ops = @import("value_ops.zig");
const select_helpers = @import("select_helpers.zig");
const shared = @import("shared.zig");
const std = @import("std");

const DepthFilterPlan = shared.DepthFilterPlan;
const SourceRef = shared.SourceRef;
const EvalCtx = shared.EvalCtx;
const QueryRuntime = shared.QueryRuntime;
const Error = shared.Error;

pub fn buildDepthFilterPlan(conjunct: *expr_mod.Expr, sources: []const SourceRef) Error!DepthFilterPlan {
    switch (conjunct.*) {
        .binary => |b| {
            if (b.op != .eq) return .{ .expr = conjunct };
            const left_id = switch (b.left.*) {
                .ident => |id| id,
                else => return .{ .expr = conjunct },
            };
            const right_id = switch (b.right.*) {
                .ident => |id| id,
                else => return .{ .expr = conjunct },
            };
            const left_src = try select_helpers.resolveIdentifierSourceIndex(sources, left_id.qualifier, left_id.name, ops.eqlIgnoreCase);
            const right_src = try select_helpers.resolveIdentifierSourceIndex(sources, right_id.qualifier, right_id.name, ops.eqlIgnoreCase);
            if (left_src == right_src) return .{ .expr = conjunct };
            const left_col = vm.columnIndex(sources[left_src].table.columns.items, left_id.name) orelse return Error.UnknownColumn;
            const right_col = vm.columnIndex(sources[right_src].table.columns.items, right_id.name) orelse return Error.UnknownColumn;
            return .{
                .eq_columns = .{
                    .left_src = left_src,
                    .left_col = left_col,
                    .right_src = right_src,
                    .right_col = right_col,
                },
            };
        },
        else => return .{ .expr = conjunct },
    }
}

pub fn evalDepthFilterPlan(
    self: anytype,
    allocator: std.mem.Allocator,
    plan: DepthFilterPlan,
    ctx: *const EvalCtx,
    ctx_by_source: []const EvalCtx,
    runtime: *QueryRuntime,
) Error!bool {
    switch (plan) {
        .expr => |node| {
            const cond_val = try self.evalExpr(allocator, node, ctx, runtime);
            return ops.toSqlBool(cond_val);
        },
        .eq_columns => |eq| {
            if (self.metrics_enabled) self.metrics_data.eval_expr_calls += 1;
            const left_row = ctx_by_source[eq.left_src].row orelse return false;
            const right_row = ctx_by_source[eq.right_src].row orelse return false;
            const left = left_row[eq.left_col];
            const right = right_row[eq.right_col];
            if (left == .null or right == .null) return false;
            return ops.compareValues(left, right) == 0;
        },
    }
}
