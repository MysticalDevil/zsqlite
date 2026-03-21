const std = @import("std");
const types = @import("types.zig");
const common = @import("common.zig");

pub fn parseSelect(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const from_idx = common.findTopLevelKeyword(sql_text, " FROM ") orelse return types.ParseError.InvalidSql;
    const proj_sql = std.mem.trim(u8, sql_text["SELECT ".len..from_idx], " \t\r\n");
    if (proj_sql.len == 0) return types.ParseError.InvalidSql;

    const after_from = sql_text[from_idx + " FROM ".len ..];
    const where_idx_rel = common.findTopLevelKeyword(after_from, " WHERE ");
    const order_idx_rel = common.findTopLevelKeyword(after_from, " ORDER BY ");

    var from_end = after_from.len;
    if (where_idx_rel) |idx| from_end = @min(from_end, idx);
    if (order_idx_rel) |idx| from_end = @min(from_end, idx);

    const from_part = std.mem.trim(u8, after_from[0..from_end], " \t\r\n");
    if (from_part.len == 0) return types.ParseError.InvalidSql;

    const from_parsed = try parseFrom(allocator, from_part);

    const where_expr = if (where_idx_rel) |widx| blk: {
        const start = widx + " WHERE ".len;
        const end = if (order_idx_rel) |oidx| oidx else after_from.len;
        const text = std.mem.trim(u8, after_from[start..end], " \t\r\n");
        if (text.len == 0) return types.ParseError.InvalidSql;
        break :blk try allocator.dupe(u8, text);
    } else null;

    const order_by = if (order_idx_rel) |oidx|
        try parseOrderBy(allocator, std.mem.trim(u8, after_from[oidx + " ORDER BY ".len ..], " \t\r\n"))
    else
        try allocator.alloc(types.OrderTerm, 0);

    return types.Statement{ .select = .{
        .table_name = from_parsed.table_name,
        .table_alias = from_parsed.alias,
        .projections = try common.splitTopLevelComma(allocator, proj_sql),
        .where_expr = where_expr,
        .order_by = order_by,
    } };
}

const FromParsed = struct {
    table_name: []const u8,
    alias: ?[]const u8,
};

fn parseFrom(allocator: std.mem.Allocator, text: []const u8) types.ParseError!FromParsed {
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
