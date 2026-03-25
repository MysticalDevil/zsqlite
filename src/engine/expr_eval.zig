const std = @import("std");
const Value = @import("../value.zig").Value;
const ops = @import("value_ops.zig");
const result_utils = @import("result_utils.zig");
const shared = @import("shared.zig");

const EvalCtx = shared.EvalCtx;
const QueryRuntime = shared.QueryRuntime;
const Error = shared.Error;

pub fn evalExpr(
    self: anytype,
    allocator: std.mem.Allocator,
    node: *@import("../expr/mod.zig").Expr,
    ctx: *const EvalCtx,
    runtime: *QueryRuntime,
) Error!Value {
    if (self.metrics_enabled) self.metrics_data.eval_expr_calls += 1;
    switch (node.*) {
        .literal => |v| return v,
        .ident => |id| return self.resolveIdentifier(ctx, id.qualifier, id.name),
        .cast_expr => |cast_expr| {
            const value = try evalExpr(self, allocator, cast_expr.expr, ctx, runtime);
            if (value == .null) return .null;
            if (@import("value_ops.zig").eqlIgnoreCase(cast_expr.target_type, "INTEGER")) {
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
            if (@import("value_ops.zig").eqlIgnoreCase(cast_expr.target_type, "REAL")) {
                if (@import("value_ops.zig").toNumber(value)) |num| return Value{ .real = num };
                return Value{ .real = 0 };
            }
            if (@import("value_ops.zig").eqlIgnoreCase(cast_expr.target_type, "TEXT")) {
                return switch (value) {
                    .text => |v| Value{ .text = try allocator.dupe(u8, v) },
                    .integer => |v| Value{ .text = try std.fmt.allocPrint(allocator, "{d}", .{v}) },
                    .real => |v| Value{ .text = try std.fmt.allocPrint(allocator, "{d}", .{v}) },
                    .blob => Value{ .text = try allocator.dupe(u8, "BLOB") },
                    .null => .null,
                };
            }
            return value;
        },
        .unary => |u| {
            const inner = try evalExpr(self, allocator, u.expr, ctx, runtime);
            switch (u.op) {
                .neg => {
                    if (inner == .integer) return Value{ .integer = -inner.integer };
                    if (inner == .real) return Value{ .real = -inner.real };
                    if (ops.toNumber(inner)) |num| return Value{ .real = -num };
                    return .null;
                },
                .not_op => {
                    if (inner == .null) return .null;
                    const b = ops.toSqlBool(inner);
                    return Value{ .integer = if (b) 0 else 1 };
                },
            }
        },
        .binary => |b| {
            if (b.op == .and_op) {
                const l = try evalExpr(self, allocator, b.left, ctx, runtime);
                if (l != .null and !ops.toSqlBool(l)) return Value{ .integer = 0 };
                const r = try evalExpr(self, allocator, b.right, ctx, runtime);
                if (r != .null and !ops.toSqlBool(r)) return Value{ .integer = 0 };
                if (l == .null or r == .null) return .null;
                return Value{ .integer = 1 };
            }
            if (b.op == .or_op) {
                const l = try evalExpr(self, allocator, b.left, ctx, runtime);
                if (l != .null and ops.toSqlBool(l)) return Value{ .integer = 1 };
                const r = try evalExpr(self, allocator, b.right, ctx, runtime);
                if (r != .null and ops.toSqlBool(r)) return Value{ .integer = 1 };
                if (l == .null or r == .null) return .null;
                return Value{ .integer = 0 };
            }
            const l = try evalExpr(self, allocator, b.left, ctx, runtime);
            const r = try evalExpr(self, allocator, b.right, ctx, runtime);
            return ops.evalBinary(b.op, l, r);
        },
        .between => |b| {
            const t = try evalExpr(self, allocator, b.target, ctx, runtime);
            const lo = try evalExpr(self, allocator, b.low, ctx, runtime);
            const hi = try evalExpr(self, allocator, b.high, ctx, runtime);
            const ge = ops.evalBinary(.ge, t, lo);
            const le = ops.evalBinary(.le, t, hi);
            const between_value = sqlAnd(ge, le);
            if (b.not_between) return sqlNot(between_value);
            return between_value;
        },
        .is_null => |n| {
            const v = try evalExpr(self, allocator, n.target, ctx, runtime);
            const is_null = v == .null;
            const out = if (n.not_null) !is_null else is_null;
            return Value{ .integer = if (out) 1 else 0 };
        },
        .in_list => |n| {
            const target = try evalExpr(self, allocator, n.target, ctx, runtime);
            if (n.subquery) |sql_text| {
                var rs = try self.queryTextWithParent(allocator, sql_text, ctx, runtime);
                defer rs.deinit();
                if (rs.rows.items.len == 0) return Value{ .integer = if (n.not_in) 1 else 0 };
                if (target == .null) return .null;

                var has_null = false;
                for (rs.rows.items) |row| {
                    if (row.len == 0) continue;
                    const rhs = row[0];
                    if (rhs == .null) {
                        has_null = true;
                        continue;
                    }
                    if (ops.compareValues(target, rhs) == 0) {
                        return Value{ .integer = if (n.not_in) 0 else 1 };
                    }
                }
                if (has_null) return .null;
                return Value{ .integer = if (n.not_in) 1 else 0 };
            }
            if (n.items.len == 0) return Value{ .integer = if (n.not_in) 1 else 0 };
            if (target == .null) return .null;

            var has_null = false;
            for (n.items) |item| {
                const rhs = try evalExpr(self, allocator, item, ctx, runtime);
                if (rhs == .null) {
                    has_null = true;
                    continue;
                }
                if (ops.compareValues(target, rhs) == 0) {
                    return Value{ .integer = if (n.not_in) 0 else 1 };
                }
            }
            if (has_null) return .null;
            return Value{ .integer = if (n.not_in) 1 else 0 };
        },
        .call => |call| {
            if (ops.eqlIgnoreCase(call.name, "abs")) {
                if (call.args.len != 1) return Error.InvalidSql;
                const v = try evalExpr(self, allocator, call.args[0], ctx, runtime);
                if (v == .integer) {
                    const i = v.integer;
                    if (i == std.math.minInt(i64)) return Error.InvalidSql;
                    return Value{ .integer = if (i < 0) -i else i };
                }
                if (v == .real) return Value{ .real = @abs(v.real) };
                if (ops.toNumber(v)) |n| return Value{ .real = @abs(n) };
                return .null;
            }
            if (ops.eqlIgnoreCase(call.name, "coalesce")) {
                for (call.args) |arg| {
                    const v = try evalExpr(self, allocator, arg, ctx, runtime);
                    if (v != .null) return v;
                }
                return .null;
            }
            if (ops.eqlIgnoreCase(call.name, "ifnull")) {
                if (call.args.len != 2) return Error.InvalidSql;
                const left = try evalExpr(self, allocator, call.args[0], ctx, runtime);
                if (left != .null) return left;
                return evalExpr(self, allocator, call.args[1], ctx, runtime);
            }
            if (ops.eqlIgnoreCase(call.name, "nullif")) {
                if (call.args.len != 2) return Error.InvalidSql;
                const left = try evalExpr(self, allocator, call.args[0], ctx, runtime);
                const right = try evalExpr(self, allocator, call.args[1], ctx, runtime);
                if (left == .null or right == .null) return left;
                if (ops.compareValues(left, right) == 0) return .null;
                return left;
            }
            return Error.UnsupportedSql;
        },
        .case_expr => |c| {
            if (c.base) |base_expr| {
                const base = try evalExpr(self, allocator, base_expr, ctx, runtime);
                for (c.whens) |w| {
                    const when_val = try evalExpr(self, allocator, w.cond, ctx, runtime);
                    const cmp = ops.evalBinary(.eq, base, when_val);
                    if (ops.toSqlBool(cmp)) return evalExpr(self, allocator, w.value, ctx, runtime);
                }
            } else {
                for (c.whens) |w| {
                    const cond = try evalExpr(self, allocator, w.cond, ctx, runtime);
                    if (ops.toSqlBool(cond)) return evalExpr(self, allocator, w.value, ctx, runtime);
                }
            }
            if (c.else_expr) |e| return evalExpr(self, allocator, e, ctx, runtime);
            return .null;
        },
        .subquery => |sql_text| {
            const parsed_stmt = try self.getParsedSubquery(runtime, sql_text);
            const mode_ptr = blk: {
                if (runtime.scalar_mode.getPtr(sql_text)) |existing| break :blk existing;
                const owned = try allocator.dupe(u8, sql_text);
                const gop = try runtime.scalar_mode.getOrPut(owned);
                if (gop.found_existing) {
                    allocator.free(owned);
                } else {
                    gop.value_ptr.* = .unknown;
                }
                break :blk gop.value_ptr;
            };

            if (mode_ptr.* == .uncorrelated) {
                if (runtime.scalar_cache.get(sql_text)) |cached| {
                    if (self.metrics_enabled) self.metrics_data.subquery_cache_hits += 1;
                    return cached;
                }
                return Error.InvalidSql;
            }

            if (mode_ptr.* == .unknown) {
                var uncorr_result = self.queryParsedWithParent(allocator, parsed_stmt, null, runtime);
                if (uncorr_result) |*rs_uncorr| {
                    defer rs_uncorr.deinit();
                    if (self.metrics_enabled) self.metrics_data.subquery_exec_calls += 1;
                    var cached_value: Value = .null;
                    if (rs_uncorr.rows.items.len != 0 and rs_uncorr.rows.items[0].len != 0) {
                        cached_value = try result_utils.cloneResultValue(allocator, rs_uncorr.rows.items[0][0]);
                    }
                    const owned = try allocator.dupe(u8, sql_text);
                    const cache_gop = try runtime.scalar_cache.getOrPut(owned);
                    if (cache_gop.found_existing) {
                        allocator.free(owned);
                        switch (cache_gop.value_ptr.*) {
                            .text => |t| allocator.free(t),
                            .blob => |b| allocator.free(b),
                            else => {},
                        }
                    }
                    cache_gop.value_ptr.* = cached_value;
                    mode_ptr.* = .uncorrelated;
                    return cached_value;
                } else |uncorr_err| {
                    if (uncorr_err != Error.UnknownColumn) return uncorr_err;
                    mode_ptr.* = .correlated;
                }
            }

            var rs = try self.queryParsedWithParent(allocator, parsed_stmt, ctx, runtime);
            defer rs.deinit();
            if (self.metrics_enabled) self.metrics_data.subquery_exec_calls += 1;
            if (rs.rows.items.len == 0 or rs.rows.items[0].len == 0) return .null;
            return rs.rows.items[0][0];
        },
        .exists_subquery => |sql_text| {
            const parsed_stmt = try self.getParsedSubquery(runtime, sql_text);
            const mode_ptr = blk: {
                if (runtime.exists_mode.getPtr(sql_text)) |existing| break :blk existing;
                const owned = try allocator.dupe(u8, sql_text);
                const gop = try runtime.exists_mode.getOrPut(owned);
                if (gop.found_existing) {
                    allocator.free(owned);
                } else {
                    gop.value_ptr.* = .unknown;
                }
                break :blk gop.value_ptr;
            };

            if (mode_ptr.* == .uncorrelated) {
                if (runtime.exists_cache.get(sql_text)) |cached| {
                    if (self.metrics_enabled) self.metrics_data.subquery_cache_hits += 1;
                    return cached;
                }
                return Error.InvalidSql;
            }

            if (mode_ptr.* == .unknown) {
                var uncorr_result = self.queryParsedWithParent(allocator, parsed_stmt, null, runtime);
                if (uncorr_result) |*rs_uncorr| {
                    defer rs_uncorr.deinit();
                    if (self.metrics_enabled) self.metrics_data.subquery_exec_calls += 1;
                    const cached_value = Value{ .integer = if (rs_uncorr.rows.items.len > 0) 1 else 0 };
                    const owned = try allocator.dupe(u8, sql_text);
                    const cache_gop = try runtime.exists_cache.getOrPut(owned);
                    if (cache_gop.found_existing) {
                        allocator.free(owned);
                        switch (cache_gop.value_ptr.*) {
                            .text => |t| allocator.free(t),
                            .blob => |b| allocator.free(b),
                            else => {},
                        }
                    }
                    cache_gop.value_ptr.* = cached_value;
                    mode_ptr.* = .uncorrelated;
                    return cached_value;
                } else |uncorr_err| {
                    if (uncorr_err != Error.UnknownColumn) return uncorr_err;
                    mode_ptr.* = .correlated;
                }
            }

            var rs = try self.queryParsedWithParent(allocator, parsed_stmt, ctx, runtime);
            defer rs.deinit();
            if (self.metrics_enabled) self.metrics_data.subquery_exec_calls += 1;
            return Value{ .integer = if (rs.rows.items.len > 0) 1 else 0 };
        },
    }
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
