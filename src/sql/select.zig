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
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return types.ParseError.InvalidSql;

    if (trimmed[0] == '(') {
        const close_idx = findMatchingCloseParen(trimmed) orelse return types.ParseError.InvalidSql;
        const subquery_sql = std.mem.trim(u8, trimmed[1..close_idx], " \t\r\n");
        if (subquery_sql.len == 0) return types.ParseError.InvalidSql;

        const rest = std.mem.trim(u8, trimmed[close_idx + 1 ..], " \t\r\n");
        if (rest.len == 0) return types.ParseError.InvalidSql;

        var alias_text = rest;
        if (common.startsWithIgnoreCase(rest, "AS ")) {
            alias_text = std.mem.trim(u8, rest["AS ".len ..], " \t\r\n");
        }
        if (alias_text.len == 0 or std.mem.indexOfAny(u8, alias_text, " \t\r\n") != null) return types.ParseError.InvalidSql;

        return .{
            .table_name = try allocator.dupe(u8, alias_text),
            .subquery_sql = try allocator.dupe(u8, subquery_sql),
            .alias = try allocator.dupe(u8, alias_text),
            .index_hint = .none,
        };
    }

    var it = std.mem.tokenizeAny(u8, trimmed, " \t\r\n");
    const table_name = it.next() orelse return types.ParseError.InvalidSql;

    var alias: ?[]const u8 = null;
    var index_hint: types.IndexHint = .none;

    while (it.next()) |token| {
        if (common.eqlIgnoreCase(token, "AS")) {
            const alias_name = it.next() orelse return types.ParseError.InvalidSql;
            alias = try allocator.dupe(u8, alias_name);
            continue;
        }
        if (common.eqlIgnoreCase(token, "NOT")) {
            const third = it.next() orelse return types.ParseError.InvalidSql;
            if (!common.eqlIgnoreCase(third, "INDEXED")) return types.ParseError.InvalidSql;
            index_hint = .not_indexed;
            continue;
        }
        if (common.eqlIgnoreCase(token, "INDEXED")) {
            if (!common.eqlIgnoreCase(it.next() orelse return types.ParseError.InvalidSql, "BY")) return types.ParseError.InvalidSql;
            const index_name = it.next() orelse return types.ParseError.InvalidSql;
            index_hint = .{ .indexed_by = try allocator.dupe(u8, index_name) };
            continue;
        }
        alias = try allocator.dupe(u8, token);
    }

    return .{
        .table_name = try allocator.dupe(u8, table_name),
        .subquery_sql = null,
        .alias = alias,
        .index_hint = index_hint,
    };
}

fn parseFromList(allocator: std.mem.Allocator, text: []const u8) types.ParseError![]const types.FromItem {
    const parts = try common.splitTopLevelComma(allocator, text);
    var out = std.ArrayList(types.FromItem).empty;
    defer out.deinit(allocator);
    for (parts) |part| {
        try appendFromPart(allocator, &out, part);
    }
    if (out.items.len == 0) return types.ParseError.InvalidSql;
    return out.toOwnedSlice(allocator);
}

fn appendFromPart(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(types.FromItem),
    text: []const u8,
) types.ParseError!void {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return types.ParseError.InvalidSql;

    if (try splitTopLevelCrossJoin(allocator, trimmed)) |parts| {
        for (parts) |part| {
            try appendFromPart(allocator, out, part);
        }
        return;
    }

    if (trimmed[0] == '(') {
        const close_idx = findMatchingCloseParen(trimmed) orelse return types.ParseError.InvalidSql;
        if (close_idx == trimmed.len - 1) {
            const inner = std.mem.trim(u8, trimmed[1..close_idx], " \t\r\n");
            if (inner.len == 0) return types.ParseError.InvalidSql;
            if (!startsWithKeyword(inner, "SELECT")) {
                const nested = try parseFromList(allocator, inner);
                for (nested) |item| try out.append(allocator, item);
                return;
            }
        }
    }

    const item = try parseFromItem(allocator, trimmed);
    try out.append(allocator, item);
}

fn findMatchingCloseParen(text: []const u8) ?usize {
    if (text.len == 0 or text[0] != '(') return null;

    var depth: usize = 0;
    var in_string = false;
    for (text, 0..) |c, i| {
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
            depth -= 1;
            if (depth == 0) return i;
        }
    }
    return null;
}

fn splitTopLevelCrossJoin(allocator: std.mem.Allocator, text: []const u8) !?[]const []const u8 {
    var out = std.ArrayList([]const u8).empty;
    defer out.deinit(allocator);

    var depth: usize = 0;
    var in_string = false;
    var start: usize = 0;
    var i: usize = 0;
    while (i + "CROSS JOIN".len <= text.len) : (i += 1) {
        const c = text[i];
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
        if (!common.eqlIgnoreCase(text[i .. i + "CROSS JOIN".len], "CROSS JOIN")) continue;
        if (!hasWordBoundaryBefore(text, i) or !hasWordBoundaryAfter(text, i + "CROSS JOIN".len)) continue;

        const part = std.mem.trim(u8, text[start..i], " \t\r\n");
        if (part.len == 0) return types.ParseError.InvalidSql;
        try out.append(allocator, try allocator.dupe(u8, part));
        start = i + "CROSS JOIN".len;
        i = start - 1;
    }

    if (out.items.len == 0) return null;

    const last = std.mem.trim(u8, text[start..], " \t\r\n");
    if (last.len == 0) return types.ParseError.InvalidSql;
    try out.append(allocator, try allocator.dupe(u8, last));
    const owned = try out.toOwnedSlice(allocator);
    return owned;
}

fn parseOrderBy(allocator: std.mem.Allocator, text: []const u8) types.ParseError![]const types.OrderTerm {
    const parts = try common.splitTopLevelComma(allocator, text);
    var out = std.ArrayList(types.OrderTerm).empty;
    defer out.deinit(allocator);

    for (parts) |part| {
        const term = std.mem.trim(u8, part, " \t\r\n");
        if (term.len == 0) return types.ParseError.InvalidSql;
        var descending = false;
        var expr_text = term;
        if (term.len > 5 and common.eqlIgnoreCase(term[term.len - 5 ..], " DESC")) {
            descending = true;
            expr_text = std.mem.trim(u8, term[0 .. term.len - 5], " \t\r\n");
        } else if (term.len > 4 and common.eqlIgnoreCase(term[term.len - 4 ..], " ASC")) {
            expr_text = std.mem.trim(u8, term[0 .. term.len - 4], " \t\r\n");
        }
        const ordinal = std.fmt.parseInt(usize, expr_text, 10) catch 0;
        if (ordinal > 0) {
            try out.append(allocator, .{
                .expr = try allocator.dupe(u8, expr_text),
                .is_ordinal = true,
                .ordinal = ordinal,
                .descending = descending,
            });
        } else {
            try out.append(allocator, .{
                .expr = try allocator.dupe(u8, expr_text),
                .is_ordinal = false,
                .ordinal = 0,
                .descending = descending,
            });
        }
    }

    return out.toOwnedSlice(allocator);
}

fn parseSimpleSelect(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const select_pos = findTopLevelKeyword(sql_text, "SELECT") orelse return types.ParseError.InvalidSql;
    if (select_pos != 0) return types.ParseError.InvalidSql;
    var distinct = false;
    var projection_start = "SELECT".len;
    const after_select_idx = skipSpaces(sql_text, "SELECT".len);
    const after_select = sql_text[after_select_idx..];
    if (startsWithKeyword(after_select, "DISTINCT")) {
        distinct = true;
        projection_start = after_select_idx + "DISTINCT".len;
    } else if (startsWithKeyword(after_select, "ALL")) {
        projection_start = after_select_idx + "ALL".len;
    }
    if (findTopLevelKeyword(sql_text, "FROM")) |from_idx| {
        const proj_sql = std.mem.trim(u8, sql_text[projection_start..from_idx], " \t\r\n");
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
            .distinct = distinct,
            .from = from_list,
            .projections = try common.splitTopLevelComma(allocator, proj_sql),
            .where_expr = where_expr,
            .order_by = order_by,
        } };
    }

    const where_idx = findTopLevelKeyword(sql_text, "WHERE");
    const order_idx = findTopLevelOrderBy(sql_text);
    var proj_end = sql_text.len;
    if (where_idx) |idx| proj_end = @min(proj_end, idx);
    if (order_idx) |idx| proj_end = @min(proj_end, idx);

    const proj_sql = std.mem.trim(u8, sql_text[projection_start..proj_end], " \t\r\n");
    if (proj_sql.len == 0) return types.ParseError.InvalidSql;

    const where_expr = if (where_idx) |widx| blk: {
        const start = widx + "WHERE".len;
        const end = if (order_idx) |oidx| oidx else sql_text.len;
        const text = std.mem.trim(u8, sql_text[start..end], " \t\r\n");
        if (text.len == 0) return types.ParseError.InvalidSql;
        break :blk try allocator.dupe(u8, text);
    } else null;

    const order_by = if (order_idx) |oidx|
        try parseOrderBy(allocator, std.mem.trim(u8, sql_text[oidx + "ORDER BY".len ..], " \t\r\n"))
    else
        try allocator.alloc(types.OrderTerm, 0);

    return .{ .select = .{
        .distinct = distinct,
        .from = try allocator.alloc(types.FromItem, 0),
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
