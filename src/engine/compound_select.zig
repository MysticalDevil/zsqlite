const std = @import("std");
const sql = @import("../sql.zig");
const result_utils = @import("result_utils.zig");
const shared = @import("shared.zig");

const RowSet = shared.RowSet;
const SortRow = shared.SortRow;
const EvalCtx = shared.EvalCtx;
const Error = shared.Error;

pub fn executeCompoundSelect(
    self: anytype,
    allocator: std.mem.Allocator,
    compound: sql.CompoundSelect,
    parent_ctx: ?*const EvalCtx,
) Error!RowSet {
    if (compound.arms.len == 0) return Error.InvalidSql;
    if (compound.ops.len + 1 != compound.arms.len) return Error.InvalidSql;

    var arm_sets = std.ArrayList(RowSet).empty;
    defer {
        for (arm_sets.items) |*rs| rs.deinit();
        arm_sets.deinit(allocator);
    }
    for (compound.arms) |arm_sql| {
        try arm_sets.append(allocator, try self.queryTextWithParent(allocator, arm_sql, parent_ctx, null));
    }

    var groups = std.ArrayList(RowSet).empty;
    defer {
        for (groups.items) |*rs| rs.deinit();
        groups.deinit(allocator);
    }
    var group_ops = std.ArrayList(sql.SetOp).empty;
    defer group_ops.deinit(allocator);

    var current = try result_utils.cloneRowSet(allocator, &arm_sets.items[0]);
    var current_owned = true;
    defer if (current_owned) current.deinit();

    for (compound.ops, 0..) |set_op, idx| {
        const rhs = &arm_sets.items[idx + 1];
        if (set_op == .intersect) {
            const next = try result_utils.rowSetIntersect(allocator, &current, rhs);
            current.deinit();
            current = next;
            current_owned = true;
            continue;
        }

        try groups.append(allocator, current);
        current_owned = false;
        try group_ops.append(allocator, set_op);
        current = try result_utils.cloneRowSet(allocator, rhs);
        current_owned = true;
    }
    try groups.append(allocator, current);
    current_owned = false;

    if (groups.items.len == 0) return Error.InvalidSql;
    var out = try result_utils.cloneRowSet(allocator, &groups.items[0]);
    errdefer out.deinit();

    for (group_ops.items, 0..) |set_op, idx| {
        const rhs = &groups.items[idx + 1];
        switch (set_op) {
            .union_all => {
                for (rhs.rows.items) |row| try result_utils.appendClonedRow(allocator, &out, row);
            },
            .union_distinct => {
                result_utils.dedupRowsInPlace(allocator, &out);
                for (rhs.rows.items) |row| try result_utils.appendDistinctRow(allocator, &out, row);
            },
            .except => {
                const next = try result_utils.rowSetExcept(allocator, &out, rhs);
                out.deinit();
                out = next;
            },
            .intersect => {
                const next = try result_utils.rowSetIntersect(allocator, &out, rhs);
                out.deinit();
                out = next;
            },
        }
    }

    if (compound.order_by.len > 0) {
        var rows = std.ArrayList(SortRow).empty;
        defer {
            for (rows.items) |entry| {
                allocator.free(entry.keys);
                allocator.free(entry.descending);
            }
            rows.deinit(allocator);
        }

        for (out.rows.items) |row| {
            const keys = try allocator.alloc(@import("../value.zig").Value, compound.order_by.len);
            const descending = try allocator.alloc(bool, compound.order_by.len);
            for (compound.order_by, 0..) |term, i| {
                descending[i] = term.descending;
                if (!term.is_ordinal) return Error.InvalidSql;
                if (term.ordinal == 0 or term.ordinal > row.len) return Error.InvalidSql;
                keys[i] = try result_utils.cloneResultValue(allocator, row[term.ordinal - 1]);
            }
            try rows.append(allocator, .{ .values = row, .keys = keys, .descending = descending });
        }

        std.sort.heap(SortRow, rows.items, {}, result_utils.sortRowLessThan);
        out.rows.clearRetainingCapacity();
        for (rows.items) |entry| try out.rows.append(allocator, entry.values);
    }

    return out;
}
