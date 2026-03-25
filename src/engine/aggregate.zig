const std = @import("std");
const Value = @import("../value.zig").Value;
const sql = @import("../sql.zig");
const ops = @import("value_ops.zig");
const result_utils = @import("result_utils.zig");
const shared = @import("shared.zig");

const Table = shared.Table;
const EvalCtx = shared.EvalCtx;
const QueryRuntime = shared.QueryRuntime;
const SourceRef = shared.SourceRef;
const Error = shared.Error;

pub fn evalAggregateRow(
    self: anytype,
    allocator: std.mem.Allocator,
    table: *const Table,
    sel: sql.Select,
    projections: []const *@import("../expr/mod.zig").Expr,
    where_expr: ?*@import("../expr/mod.zig").Expr,
    parent_ctx: ?*const EvalCtx,
    alias: []const u8,
    runtime: *QueryRuntime,
) Error![]Value {
    const row = try allocator.alloc(Value, projections.len);
    var seen_any = false;

    for (projections, 0..) |pexpr, i| {
        row[i] = try evalAggregateExpr(self, allocator, pexpr, table, sel, where_expr, parent_ctx, alias, runtime, &seen_any);
    }
    return row;
}

pub fn evalAggregateExpr(
    self: anytype,
    allocator: std.mem.Allocator,
    pexpr: *@import("../expr/mod.zig").Expr,
    table: *const Table,
    sel: sql.Select,
    where_expr: ?*@import("../expr/mod.zig").Expr,
    parent_ctx: ?*const EvalCtx,
    alias: []const u8,
    runtime: *QueryRuntime,
    seen_any_ptr: *bool,
) Error!Value {
    switch (pexpr.*) {
        .literal => |v| return v,
        .cast_expr => |cast_expr| {
            const value = try evalAggregateExpr(self, allocator, cast_expr.expr, table, sel, where_expr, parent_ctx, alias, runtime, seen_any_ptr);
            if (value == .null) return .null;
            if (ops.eqlIgnoreCase(cast_expr.target_type, "INTEGER")) {
                return switch (value) {
                    .integer => value,
                    .real => |v| Value{ .integer = @as(i64, @intFromFloat(v)) },
                    .text => |v| blk: {
                        const parsed_int = std.fmt.parseInt(i64, v, 10) catch {
                            const parsed_real = std.fmt.parseFloat(f64, v) catch 0;
                            break :blk Value{ .integer = @as(i64, @intFromFloat(parsed_real)) };
                        };
                        break :blk Value{ .integer = parsed_int };
                    },
                    .blob => Value{ .integer = 0 },
                    .null => .null,
                };
            }
            return value;
        },
        .unary => |u| {
            const inner = try evalAggregateExpr(self, allocator, u.expr, table, sel, where_expr, parent_ctx, alias, runtime, seen_any_ptr);
            switch (u.op) {
                .neg => {
                    if (inner == .null) return .null;
                    if (inner == .integer) return Value{ .integer = -inner.integer };
                    if (inner == .real) return Value{ .real = -inner.real };
                    if (ops.toNumber(inner)) |num| return Value{ .real = -num };
                    return .null;
                },
                .not_op => {
                    if (inner == .null) return .null;
                    return Value{ .integer = if (ops.toSqlBool(inner)) 0 else 1 };
                },
            }
        },
        .binary => |b| {
            const left = try evalAggregateExpr(self, allocator, b.left, table, sel, where_expr, parent_ctx, alias, runtime, seen_any_ptr);
            const right = try evalAggregateExpr(self, allocator, b.right, table, sel, where_expr, parent_ctx, alias, runtime, seen_any_ptr);
            if (b.op == .and_op) {
                if (left != .null and !ops.toSqlBool(left)) return Value{ .integer = 0 };
                if (right != .null and !ops.toSqlBool(right)) return Value{ .integer = 0 };
                if (left == .null or right == .null) return .null;
                return Value{ .integer = 1 };
            }
            if (b.op == .or_op) {
                if (left != .null and ops.toSqlBool(left)) return Value{ .integer = 1 };
                if (right != .null and ops.toSqlBool(right)) return Value{ .integer = 1 };
                if (left == .null or right == .null) return .null;
                return Value{ .integer = 0 };
            }
            return ops.evalBinary(b.op, left, right);
        },
        .between => |b| {
            const target = try evalAggregateExpr(self, allocator, b.target, table, sel, where_expr, parent_ctx, alias, runtime, seen_any_ptr);
            const low = try evalAggregateExpr(self, allocator, b.low, table, sel, where_expr, parent_ctx, alias, runtime, seen_any_ptr);
            const high = try evalAggregateExpr(self, allocator, b.high, table, sel, where_expr, parent_ctx, alias, runtime, seen_any_ptr);
            const ge = ops.evalBinary(.ge, target, low);
            const le = ops.evalBinary(.le, target, high);
            const between_value = sqlAnd(ge, le);
            if (b.not_between) return sqlNot(between_value);
            return between_value;
        },
        .is_null => |n| {
            const value = try evalAggregateExpr(self, allocator, n.target, table, sel, where_expr, parent_ctx, alias, runtime, seen_any_ptr);
            const is_null = value == .null;
            return Value{ .integer = if (if (n.not_null) !is_null else is_null) 1 else 0 };
        },
        .call => |call| {
            if (call.star_arg and call.distinct) return Error.InvalidSql;
            if (call.star_arg and !ops.eqlIgnoreCase(call.name, "count")) return Error.InvalidSql;

            var distinct_values = std.ArrayList(Value).empty;
            defer {
                for (distinct_values.items) |value| switch (value) {
                    .text => |t| allocator.free(t),
                    .blob => |b| allocator.free(b),
                    else => {},
                };
                distinct_values.deinit(allocator);
            }

            var count: i64 = 0;
            var sum: f64 = 0;
            var total: f64 = 0;
            var numeric_count: usize = 0;
            var int_sum: i64 = 0;
            var sum_all_integer = true;
            var sum_overflow = false;
            var concat_parts = std.ArrayList([]const u8).empty;
            defer {
                for (concat_parts.items) |part| allocator.free(part);
                concat_parts.deinit(allocator);
            }
            var min_value: ?f64 = null;
            var max_value: ?f64 = null;

            for (table.rows.items, 0..) |source_row, row_id| {
                if (!table.isRowLive(row_id)) continue;
                var ctx = EvalCtx{
                    .table = table,
                    .table_name = sel.from[0].table_name,
                    .alias = alias,
                    .row = source_row,
                    .parent = parent_ctx,
                };
                if (where_expr) |w| {
                    const cond = try self.evalExpr(allocator, w, &ctx, runtime);
                    if (!ops.toSqlBool(cond)) continue;
                }

                if (call.star_arg) {
                    count += 1;
                    seen_any_ptr.* = true;
                    continue;
                }
                if (call.args.len == 0 or call.args.len > 2) return Error.InvalidSql;

                const value = try self.evalExpr(allocator, call.args[0], &ctx, runtime);
                if (value == .null) continue;

                if (call.distinct) {
                    var found = false;
                    for (distinct_values.items) |existing| {
                        if (existing.eql(value)) {
                            found = true;
                            break;
                        }
                    }
                    if (found) continue;
                    try distinct_values.append(allocator, try result_utils.cloneResultValue(allocator, value));
                }

                if (ops.eqlIgnoreCase(call.name, "count")) {
                    count += 1;
                    seen_any_ptr.* = true;
                    continue;
                }

                if (ops.eqlIgnoreCase(call.name, "group_concat")) {
                    if (call.distinct and call.args.len != 1) return Error.InvalidSql;
                    const text = try aggregateValueText(allocator, value);
                    try concat_parts.append(allocator, text);
                    seen_any_ptr.* = true;
                    continue;
                }

                const number = ops.toNumber(value) orelse 0;
                if (ops.eqlIgnoreCase(call.name, "avg")) {
                    sum += number;
                    numeric_count += 1;
                    seen_any_ptr.* = true;
                    continue;
                }
                if (ops.eqlIgnoreCase(call.name, "sum")) {
                    sum += number;
                    numeric_count += 1;
                    if ((value == .integer or (value == .real and @floor(value.real) == value.real)) and sum_all_integer and !sum_overflow) {
                        const int_value = if (value == .integer) value.integer else @as(i64, @intFromFloat(value.real));
                        int_sum = std.math.add(i64, int_sum, int_value) catch blk: {
                            sum_overflow = true;
                            break :blk int_sum;
                        };
                    } else {
                        sum_all_integer = false;
                    }
                    seen_any_ptr.* = true;
                    continue;
                }
                if (ops.eqlIgnoreCase(call.name, "total")) {
                    total += number;
                    seen_any_ptr.* = true;
                    continue;
                }
                if (ops.eqlIgnoreCase(call.name, "min")) {
                    if (min_value == null or number < min_value.?) min_value = number;
                    seen_any_ptr.* = true;
                    continue;
                }
                if (ops.eqlIgnoreCase(call.name, "max")) {
                    if (max_value == null or number > max_value.?) max_value = number;
                    seen_any_ptr.* = true;
                    continue;
                }
                return Error.UnsupportedSql;
            }

            if (ops.eqlIgnoreCase(call.name, "count")) return Value{ .integer = count };
            if (ops.eqlIgnoreCase(call.name, "avg")) {
                if (numeric_count == 0) return .null;
                return Value{ .real = sum / @as(f64, @floatFromInt(numeric_count)) };
            }
            if (ops.eqlIgnoreCase(call.name, "sum")) {
                if (numeric_count == 0) return .null;
                if (sum_all_integer and sum_overflow) return Error.AggregateEmpty;
                if (sum < @as(f64, @floatFromInt(std.math.minInt(i64))) or sum > @as(f64, @floatFromInt(std.math.maxInt(i64)))) {
                    return Error.AggregateEmpty;
                }
                if (sum_all_integer) return Value{ .integer = int_sum };
                if (@floor(sum) == sum) return Value{ .integer = @as(i64, @intFromFloat(sum)) };
                return Value{ .real = sum };
            }
            if (ops.eqlIgnoreCase(call.name, "total")) return Value{ .real = total };
            if (ops.eqlIgnoreCase(call.name, "min")) {
                if (min_value == null) return .null;
                if (@floor(min_value.?) == min_value.?) return Value{ .integer = @as(i64, @intFromFloat(min_value.?)) };
                return Value{ .real = min_value.? };
            }
            if (ops.eqlIgnoreCase(call.name, "max")) {
                if (max_value == null) return .null;
                if (@floor(max_value.?) == max_value.?) return Value{ .integer = @as(i64, @intFromFloat(max_value.?)) };
                return Value{ .real = max_value.? };
            }
            if (ops.eqlIgnoreCase(call.name, "group_concat")) {
                if (concat_parts.items.len == 0) return .null;
                const separator = if (call.args.len == 2) blk: {
                    const sep_value = try self.evalExpr(allocator, call.args[1], &EvalCtx{
                        .table = table,
                        .table_name = sel.from[0].table_name,
                        .alias = alias,
                        .row = null,
                        .parent = parent_ctx,
                    }, runtime);
                    break :blk try aggregateValueText(allocator, sep_value);
                } else try allocator.dupe(u8, ",");
                defer allocator.free(separator);

                var joined = std.ArrayList(u8).empty;
                defer joined.deinit(allocator);
                for (concat_parts.items, 0..) |part, i| {
                    if (i != 0) try joined.appendSlice(allocator, separator);
                    try joined.appendSlice(allocator, part);
                }
                return Value{ .text = try joined.toOwnedSlice(allocator) };
            }
            return Error.UnsupportedSql;
        },
        else => return Error.UnsupportedSql,
    }
}

pub fn evalAggregateRowForMatches(
    self: anytype,
    allocator: std.mem.Allocator,
    projections: []const *@import("../expr/mod.zig").Expr,
    sources: []const SourceRef,
    parent_ctx: ?*const EvalCtx,
    runtime: *QueryRuntime,
    matched_row_ids: []const usize,
) Error![]Value {
    const row = try allocator.alloc(Value, projections.len);
    for (projections, 0..) |pexpr, i| {
        row[i] = try evalAggregateExprForMatches(
            self,
            allocator,
            pexpr,
            sources,
            parent_ctx,
            runtime,
            matched_row_ids,
        );
    }
    return row;
}

fn evalAggregateExprForMatches(
    self: anytype,
    allocator: std.mem.Allocator,
    pexpr: *@import("../expr/mod.zig").Expr,
    sources: []const SourceRef,
    parent_ctx: ?*const EvalCtx,
    runtime: *QueryRuntime,
    matched_row_ids: []const usize,
) Error!Value {
    switch (pexpr.*) {
        .literal => |v| return v,
        .cast_expr => |cast_expr| {
            const value = try evalAggregateExprForMatches(self, allocator, cast_expr.expr, sources, parent_ctx, runtime, matched_row_ids);
            if (value == .null) return .null;
            if (ops.eqlIgnoreCase(cast_expr.target_type, "INTEGER")) {
                return switch (value) {
                    .integer => value,
                    .real => |v| Value{ .integer = @as(i64, @intFromFloat(v)) },
                    .text => |v| blk: {
                        const parsed_int = std.fmt.parseInt(i64, v, 10) catch {
                            const parsed_real = std.fmt.parseFloat(f64, v) catch 0;
                            break :blk Value{ .integer = @as(i64, @intFromFloat(parsed_real)) };
                        };
                        break :blk Value{ .integer = parsed_int };
                    },
                    .blob => Value{ .integer = 0 },
                    .null => .null,
                };
            }
            return value;
        },
        .unary => |u| {
            const inner = try evalAggregateExprForMatches(self, allocator, u.expr, sources, parent_ctx, runtime, matched_row_ids);
            switch (u.op) {
                .neg => {
                    if (inner == .null) return .null;
                    if (inner == .integer) return Value{ .integer = -inner.integer };
                    if (inner == .real) return Value{ .real = -inner.real };
                    if (ops.toNumber(inner)) |num| return Value{ .real = -num };
                    return .null;
                },
                .not_op => {
                    if (inner == .null) return .null;
                    return Value{ .integer = if (ops.toSqlBool(inner)) 0 else 1 };
                },
            }
        },
        .binary => |b| {
            const left = try evalAggregateExprForMatches(self, allocator, b.left, sources, parent_ctx, runtime, matched_row_ids);
            const right = try evalAggregateExprForMatches(self, allocator, b.right, sources, parent_ctx, runtime, matched_row_ids);
            if (b.op == .and_op) {
                if (left != .null and !ops.toSqlBool(left)) return Value{ .integer = 0 };
                if (right != .null and !ops.toSqlBool(right)) return Value{ .integer = 0 };
                if (left == .null or right == .null) return .null;
                return Value{ .integer = 1 };
            }
            if (b.op == .or_op) {
                if (left != .null and ops.toSqlBool(left)) return Value{ .integer = 1 };
                if (right != .null and ops.toSqlBool(right)) return Value{ .integer = 1 };
                if (left == .null or right == .null) return .null;
                return Value{ .integer = 0 };
            }
            return ops.evalBinary(b.op, left, right);
        },
        .between => |b| {
            const target = try evalAggregateExprForMatches(self, allocator, b.target, sources, parent_ctx, runtime, matched_row_ids);
            const low = try evalAggregateExprForMatches(self, allocator, b.low, sources, parent_ctx, runtime, matched_row_ids);
            const high = try evalAggregateExprForMatches(self, allocator, b.high, sources, parent_ctx, runtime, matched_row_ids);
            const ge = ops.evalBinary(.ge, target, low);
            const le = ops.evalBinary(.le, target, high);
            const between_value = sqlAnd(ge, le);
            if (b.not_between) return sqlNot(between_value);
            return between_value;
        },
        .is_null => |n| {
            const value = try evalAggregateExprForMatches(self, allocator, n.target, sources, parent_ctx, runtime, matched_row_ids);
            const is_null = value == .null;
            return Value{ .integer = if (if (n.not_null) !is_null else is_null) 1 else 0 };
        },
        .call => |call| return evalAggregateCallForMatches(self, allocator, call, sources, parent_ctx, runtime, matched_row_ids),
        else => return Error.UnsupportedSql,
    }
}

fn evalAggregateCallForMatches(
    self: anytype,
    allocator: std.mem.Allocator,
    call: @import("../expr/types.zig").Call,
    sources: []const SourceRef,
    parent_ctx: ?*const EvalCtx,
    runtime: *QueryRuntime,
    matched_row_ids: []const usize,
) Error!Value {
    if (call.star_arg and call.distinct) return Error.InvalidSql;
    if (call.star_arg and !ops.eqlIgnoreCase(call.name, "count")) return Error.InvalidSql;

    var distinct_values = std.ArrayList(Value).empty;
    defer {
        for (distinct_values.items) |value| switch (value) {
            .text => |t| allocator.free(t),
            .blob => |b| allocator.free(b),
            else => {},
        };
        distinct_values.deinit(allocator);
    }

    var count: i64 = 0;
    var sum: f64 = 0;
    var total: f64 = 0;
    var numeric_count: usize = 0;
    var int_sum: i64 = 0;
    var sum_all_integer = true;
    var sum_overflow = false;
    var concat_parts = std.ArrayList([]const u8).empty;
    defer {
        for (concat_parts.items) |part| allocator.free(part);
        concat_parts.deinit(allocator);
    }
    var min_value: ?f64 = null;
    var max_value: ?f64 = null;

    if (sources.len == 0) {
        if (ops.eqlIgnoreCase(call.name, "count")) return Value{ .integer = 0 };
        if (ops.eqlIgnoreCase(call.name, "total")) return Value{ .real = 0 };
        return .null;
    }

    const ctx_by_source = try allocator.alloc(EvalCtx, sources.len);
    defer allocator.free(ctx_by_source);

    var base: usize = 0;
    while (base < matched_row_ids.len) : (base += sources.len) {
        buildCtxChain(ctx_by_source, sources, parent_ctx, matched_row_ids[base .. base + sources.len]);
        const ctx = &ctx_by_source[sources.len - 1];

        if (call.star_arg) {
            count += 1;
            continue;
        }
        if (call.args.len == 0 or call.args.len > 2) return Error.InvalidSql;

        const value = try self.evalExpr(allocator, call.args[0], ctx, runtime);
        if (value == .null) continue;

        if (call.distinct) {
            var found = false;
            for (distinct_values.items) |existing| {
                if (existing.eql(value)) {
                    found = true;
                    break;
                }
            }
            if (found) continue;
            try distinct_values.append(allocator, try result_utils.cloneResultValue(allocator, value));
        }

        if (ops.eqlIgnoreCase(call.name, "count")) {
            count += 1;
            continue;
        }

        if (ops.eqlIgnoreCase(call.name, "group_concat")) {
            if (call.distinct and call.args.len != 1) return Error.InvalidSql;
            const text = try aggregateValueText(allocator, value);
            try concat_parts.append(allocator, text);
            continue;
        }

        const number = ops.toNumber(value) orelse 0;
        if (ops.eqlIgnoreCase(call.name, "avg")) {
            sum += number;
            numeric_count += 1;
            continue;
        }
        if (ops.eqlIgnoreCase(call.name, "sum")) {
            sum += number;
            numeric_count += 1;
            if ((value == .integer or (value == .real and @floor(value.real) == value.real)) and sum_all_integer and !sum_overflow) {
                const int_value = if (value == .integer) value.integer else @as(i64, @intFromFloat(value.real));
                int_sum = std.math.add(i64, int_sum, int_value) catch blk: {
                    sum_overflow = true;
                    break :blk int_sum;
                };
            } else {
                sum_all_integer = false;
            }
            continue;
        }
        if (ops.eqlIgnoreCase(call.name, "total")) {
            total += number;
            continue;
        }
        if (ops.eqlIgnoreCase(call.name, "min")) {
            if (min_value == null or number < min_value.?) min_value = number;
            continue;
        }
        if (ops.eqlIgnoreCase(call.name, "max")) {
            if (max_value == null or number > max_value.?) max_value = number;
            continue;
        }
        return Error.UnsupportedSql;
    }

    if (ops.eqlIgnoreCase(call.name, "count")) return Value{ .integer = count };
    if (ops.eqlIgnoreCase(call.name, "avg")) {
        if (numeric_count == 0) return .null;
        return Value{ .real = sum / @as(f64, @floatFromInt(numeric_count)) };
    }
    if (ops.eqlIgnoreCase(call.name, "sum")) {
        if (numeric_count == 0) return .null;
        if (sum_all_integer and sum_overflow) return Error.AggregateEmpty;
        if (sum < @as(f64, @floatFromInt(std.math.minInt(i64))) or sum > @as(f64, @floatFromInt(std.math.maxInt(i64)))) {
            return Error.AggregateEmpty;
        }
        if (sum_all_integer) return Value{ .integer = int_sum };
        if (@floor(sum) == sum) return Value{ .integer = @as(i64, @intFromFloat(sum)) };
        return Value{ .real = sum };
    }
    if (ops.eqlIgnoreCase(call.name, "total")) return Value{ .real = total };
    if (ops.eqlIgnoreCase(call.name, "min")) {
        if (min_value == null) return .null;
        if (@floor(min_value.?) == min_value.?) return Value{ .integer = @as(i64, @intFromFloat(min_value.?)) };
        return Value{ .real = min_value.? };
    }
    if (ops.eqlIgnoreCase(call.name, "max")) {
        if (max_value == null) return .null;
        if (@floor(max_value.?) == max_value.?) return Value{ .integer = @as(i64, @intFromFloat(max_value.?)) };
        return Value{ .real = max_value.? };
    }
    if (ops.eqlIgnoreCase(call.name, "group_concat")) {
        if (concat_parts.items.len == 0) return .null;
        const separator = if (call.args.len == 2 and matched_row_ids.len >= sources.len) blk: {
            buildCtxChain(ctx_by_source, sources, parent_ctx, matched_row_ids[0..sources.len]);
            const sep_value = try self.evalExpr(allocator, call.args[1], &ctx_by_source[sources.len - 1], runtime);
            break :blk try aggregateValueText(allocator, sep_value);
        } else try allocator.dupe(u8, ",");
        defer allocator.free(separator);

        var joined = std.ArrayList(u8).empty;
        defer joined.deinit(allocator);
        for (concat_parts.items, 0..) |part, i| {
            if (i != 0) try joined.appendSlice(allocator, separator);
            try joined.appendSlice(allocator, part);
        }
        return Value{ .text = try joined.toOwnedSlice(allocator) };
    }
    return Error.UnsupportedSql;
}

fn buildCtxChain(
    ctx_by_source: []EvalCtx,
    sources: []const SourceRef,
    parent_ctx: ?*const EvalCtx,
    row_ids: []const usize,
) void {
    for (sources, 0..) |source, i| {
        const parent = if (i == 0) parent_ctx else &ctx_by_source[i - 1];
        ctx_by_source[i] = .{
            .table = source.table,
            .table_name = source.table_name,
            .alias = source.alias,
            .row = source.table.rows.items[row_ids[i]],
            .parent = parent,
        };
    }
}

pub fn aggregateValueText(allocator: std.mem.Allocator, value: Value) Error![]const u8 {
    return switch (value) {
        .null => try allocator.dupe(u8, "NULL"),
        .integer => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
        .real => |v| try std.fmt.allocPrint(allocator, "{d:.3}", .{v}),
        .text => |v| try allocator.dupe(u8, v),
        .blob => try allocator.dupe(u8, "BLOB"),
    };
}

fn sqlNot(value: Value) Value {
    if (value == .null) return .null;
    return Value{ .integer = if (ops.toSqlBool(value)) 0 else 1 };
}

fn sqlAnd(left: Value, right: Value) Value {
    if (left != .null and !ops.toSqlBool(left)) return Value{ .integer = 0 };
    if (right != .null and !ops.toSqlBool(right)) return Value{ .integer = 0 };
    if (left == .null or right == .null) return .null;
    return Value{ .integer = 1 };
}
