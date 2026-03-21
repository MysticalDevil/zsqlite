const std = @import("std");
const types = @import("types.zig");
const common = @import("common.zig");

pub fn parseSelect(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const trimmed = std.mem.trim(u8, sql_text, " \t\r\n");
    if (!startsWithKeyword(trimmed, "SELECT")) return types.ParseError.InvalidSql;

    const order_idx = findTopLevelOrderBy(trimmed);
    const core_sql = if (order_idx) |idx|
        std.mem.trim(u8, trimmed[0..idx], " \t\r\n")
    else
        trimmed;
    const trailing_order_by = if (order_idx) |idx| blk: {
        const order_text = std.mem.trim(u8, trimmed[idx + "ORDER BY".len ..], " \t\r\n");
        if (order_text.len == 0) return types.ParseError.InvalidSql;
        break :blk try parseOrderBy(allocator, order_text);
    } else try allocator.alloc(types.OrderTerm, 0);

    var arms = std.ArrayList([]const u8).empty;
    defer arms.deinit(allocator);
    var ops = std.ArrayList(types.SetOp).empty;
    defer ops.deinit(allocator);

    var in_string = false;
    var depth: usize = 0;
    var arm_start: usize = 0;
    var i: usize = 0;
    while (i < core_sql.len) : (i += 1) {
        const c = core_sql[i];
        if (in_string) {
            if (c == '\'') in_string = false;
            continue;
        }
        if (c == '\'') {
            in_string = true;
            continue;
        }
        if (c == '(') {
            depth += 1;
            continue;
        }
        if (c == ')') {
            if (depth > 0) depth -= 1;
            continue;
        }
        if (depth != 0) continue;

        if (matchSetOpAt(core_sql, i)) |matched| {
            const arm_text = std.mem.trim(u8, core_sql[arm_start..i], " \t\r\n");
            if (arm_text.len == 0) return types.ParseError.InvalidSql;
            try arms.append(allocator, try allocator.dupe(u8, arm_text));
            try ops.append(allocator, matched.op);
            arm_start = matched.end_idx;
            i = matched.end_idx - 1;
        }
    }

    if (ops.items.len == 0) return parseSimpleSelect(allocator, trimmed);

    const last_arm = std.mem.trim(u8, core_sql[arm_start..], " \t\r\n");
    if (last_arm.len == 0) return types.ParseError.InvalidSql;
    try arms.append(allocator, try allocator.dupe(u8, last_arm));
    if (arms.items.len != ops.items.len + 1) return types.ParseError.InvalidSql;

    return .{ .compound_select = .{
        .arms = try arms.toOwnedSlice(allocator),
        .ops = try ops.toOwnedSlice(allocator),
        .order_by = trailing_order_by,
    } };
}

fn parseFromItem(allocator: std.mem.Allocator, text: []const u8) types.ParseError!types.FromItem {
    var it = std.mem.tokenizeAny(u8, text, " \t\r\n");
    const table_name = it.next() orelse return types.ParseError.InvalidSql;

    var alias: ?[]const u8 = null;
    if (it.next()) |second| {
        if (common.eqlIgnoreCase(second, "AS")) {
            const alias_name = it.next() orelse return types.ParseError.InvalidSql;
            alias = try allocator.dupe(u8, alias_name);
        } else {
            alias = try allocator.dupe(u8, second);
        }
    }

    return .{
        .table_name = try allocator.dupe(u8, table_name),
        .alias = alias,
    };
}

fn parseFromList(allocator: std.mem.Allocator, text: []const u8) types.ParseError![]const types.FromItem {
    const parts = try common.splitTopLevelComma(allocator, text);
    var out = std.ArrayList(types.FromItem).empty;
    defer out.deinit(allocator);
    for (parts) |part| {
        const item = try parseFromItem(allocator, part);
        try out.append(allocator, item);
    }
    if (out.items.len == 0) return types.ParseError.InvalidSql;
    return out.toOwnedSlice(allocator);
}

fn parseOrderBy(allocator: std.mem.Allocator, text: []const u8) types.ParseError![]const types.OrderTerm {
    const parts = try common.splitTopLevelComma(allocator, text);
    var out = std.ArrayList(types.OrderTerm).empty;
    defer out.deinit(allocator);

    for (parts) |part| {
        const term = std.mem.trim(u8, part, " \t\r\n");
        if (term.len == 0) return types.ParseError.InvalidSql;
        const ordinal = std.fmt.parseInt(usize, term, 10) catch 0;
        if (ordinal > 0) {
            try out.append(allocator, .{
                .expr = try allocator.dupe(u8, term),
                .is_ordinal = true,
                .ordinal = ordinal,
            });
        } else {
            try out.append(allocator, .{
                .expr = try allocator.dupe(u8, term),
                .is_ordinal = false,
                .ordinal = 0,
            });
        }
    }

    return out.toOwnedSlice(allocator);
}

fn parseSimpleSelect(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const from_idx = findTopLevelKeyword(sql_text, "FROM") orelse return types.ParseError.InvalidSql;

    const select_pos = findTopLevelKeyword(sql_text, "SELECT") orelse return types.ParseError.InvalidSql;
    if (select_pos != 0) return types.ParseError.InvalidSql;
    const proj_sql = std.mem.trim(u8, sql_text["SELECT".len..from_idx], " \t\r\n");
    if (proj_sql.len == 0) return types.ParseError.InvalidSql;

    const after_from_start = skipSpaces(sql_text, from_idx + "FROM".len);
    const after_from = sql_text[after_from_start..];
    const where_idx_rel = findTopLevelKeyword(after_from, "WHERE");
    const order_idx_rel = findTopLevelOrderBy(after_from);

    var from_end = after_from.len;
    if (where_idx_rel) |idx| from_end = @min(from_end, idx);
    if (order_idx_rel) |idx| from_end = @min(from_end, idx);

    const from_part = std.mem.trim(u8, after_from[0..from_end], " \t\r\n");
    if (from_part.len == 0) return types.ParseError.InvalidSql;
    const from_list = try parseFromList(allocator, from_part);

    const where_expr = if (where_idx_rel) |widx| blk: {
        const start = widx + "WHERE".len;
        const end = if (order_idx_rel) |oidx| oidx else after_from.len;
        const text = std.mem.trim(u8, after_from[start..end], " \t\r\n");
        if (text.len == 0) return types.ParseError.InvalidSql;
        break :blk try allocator.dupe(u8, text);
    } else null;

    const order_by = if (order_idx_rel) |oidx|
        try parseOrderBy(allocator, std.mem.trim(u8, after_from[oidx + "ORDER BY".len ..], " \t\r\n"))
    else
        try allocator.alloc(types.OrderTerm, 0);

    return .{ .select = .{
        .from = from_list,
        .projections = try common.splitTopLevelComma(allocator, proj_sql),
        .where_expr = where_expr,
        .order_by = order_by,
    } };
}

const MatchedSetOp = struct {
    op: types.SetOp,
    end_idx: usize,
};

fn matchSetOpAt(sql_text: []const u8, start: usize) ?MatchedSetOp {
    if (!hasWordBoundaryBefore(sql_text, start)) return null;

    const union_len = "UNION".len;
    if (start + union_len <= sql_text.len and
        common.eqlIgnoreCase(sql_text[start .. start + union_len], "UNION") and
        hasWordBoundaryAfter(sql_text, start + union_len))
    {
        var idx = skipSpaces(sql_text, start + union_len);
        if (idx < sql_text.len and startsWithKeyword(sql_text[idx..], "ALL")) {
            idx = skipSpaces(sql_text, idx + "ALL".len);
            return .{ .op = .union_all, .end_idx = idx };
        }
        return .{ .op = .union_distinct, .end_idx = idx };
    }

    const intersect_len = "INTERSECT".len;
    if (start + intersect_len <= sql_text.len and
        common.eqlIgnoreCase(sql_text[start .. start + intersect_len], "INTERSECT") and
        hasWordBoundaryAfter(sql_text, start + intersect_len))
    {
        const idx = skipSpaces(sql_text, start + intersect_len);
        return .{ .op = .intersect, .end_idx = idx };
    }

    const except_len = "EXCEPT".len;
    if (start + except_len <= sql_text.len and
        common.eqlIgnoreCase(sql_text[start .. start + except_len], "EXCEPT") and
        hasWordBoundaryAfter(sql_text, start + except_len))
    {
        const idx = skipSpaces(sql_text, start + except_len);
        return .{ .op = .except, .end_idx = idx };
    }

    return null;
}

fn findTopLevelOrderBy(sql_text: []const u8) ?usize {
    var in_string = false;
    var depth: usize = 0;
    var i: usize = 0;
    while (i < sql_text.len) : (i += 1) {
        const c = sql_text[i];
        if (in_string) {
            if (c == '\'') in_string = false;
            continue;
        }
        if (c == '\'') {
            in_string = true;
            continue;
        }
        if (c == '(') {
            depth += 1;
            continue;
        }
        if (c == ')') {
            if (depth > 0) depth -= 1;
            continue;
        }
        if (depth != 0) continue;
        if (!startsWithKeyword(sql_text[i..], "ORDER")) continue;
        const next = skipSpaces(sql_text, i + "ORDER".len);
        if (startsWithKeyword(sql_text[next..], "BY")) return i;
    }
    return null;
}

fn findTopLevelKeyword(sql_text: []const u8, keyword: []const u8) ?usize {
    var in_string = false;
    var depth: usize = 0;
    var i: usize = 0;
    while (i < sql_text.len) : (i += 1) {
        const c = sql_text[i];
        if (in_string) {
            if (c == '\'') in_string = false;
            continue;
        }
        if (c == '\'') {
            in_string = true;
            continue;
        }
        if (c == '(') {
            depth += 1;
            continue;
        }
        if (c == ')') {
            if (depth > 0) depth -= 1;
            continue;
        }
        if (depth != 0) continue;
        if (startsWithKeyword(sql_text[i..], keyword)) return i;
    }
    return null;
}

fn skipSpaces(text: []const u8, start: usize) usize {
    var idx = start;
    while (idx < text.len and isSqlSpace(text[idx])) : (idx += 1) {}
    return idx;
}

fn startsWithKeyword(text: []const u8, keyword: []const u8) bool {
    if (text.len < keyword.len) return false;
    if (!common.eqlIgnoreCase(text[0..keyword.len], keyword)) return false;
    return hasWordBoundaryAfter(text, keyword.len);
}

fn hasWordBoundaryBefore(text: []const u8, idx: usize) bool {
    if (idx == 0) return true;
    return !isWordChar(text[idx - 1]);
}

fn hasWordBoundaryAfter(text: []const u8, idx: usize) bool {
    if (idx >= text.len) return true;
    return !isWordChar(text[idx]);
}

fn isWordChar(c: u8) bool {
    return std.ascii.isAlphabetic(c) or std.ascii.isDigit(c) or c == '_';
}

fn isSqlSpace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\r' or c == '\n';
}
