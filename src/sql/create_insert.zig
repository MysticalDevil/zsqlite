const std = @import("std");
const types = @import("types.zig");
const Value = @import("../value.zig").Value;
const common = @import("common.zig");

pub fn parseCreate(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const after_kw = sql_text["CREATE TABLE ".len..];
    const lparen = std.mem.indexOfScalar(u8, after_kw, '(') orelse return types.ParseError.InvalidSql;
    const rparen = std.mem.lastIndexOfScalar(u8, after_kw, ')') orelse return types.ParseError.InvalidSql;
    if (rparen <= lparen + 1) return types.ParseError.InvalidSql;

    const table_name = std.mem.trim(u8, after_kw[0..lparen], " \t\r\n");
    if (table_name.len == 0) return types.ParseError.InvalidSql;

    var cols = std.ArrayList([]const u8).empty;
    defer cols.deinit(allocator);

    var col_it = std.mem.splitScalar(u8, after_kw[lparen + 1 .. rparen], ',');
    while (col_it.next()) |raw_col| {
        const col = std.mem.trim(u8, raw_col, " \t\r\n");
        if (col.len == 0) continue;
        const col_name = firstToken(col);
        if (col_name.len == 0) return types.ParseError.InvalidSql;
        try cols.append(allocator, try allocator.dupe(u8, col_name));
    }

    if (cols.items.len == 0) return types.ParseError.InvalidSql;

    return types.Statement{ .create_table = .{
        .table_name = try allocator.dupe(u8, table_name),
        .columns = try cols.toOwnedSlice(allocator),
    } };
}

pub fn parseCreateIndex(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const after_kw = sql_text["CREATE INDEX ".len..];
    const on_idx = common.indexOfIgnoreCase(after_kw, " ON ") orelse return types.ParseError.InvalidSql;
    const index_name = std.mem.trim(u8, after_kw[0..on_idx], " \t\r\n");
    if (index_name.len == 0) return types.ParseError.InvalidSql;

    const after_on = std.mem.trim(u8, after_kw[on_idx + " ON ".len ..], " \t\r\n");
    const lparen = std.mem.indexOfScalar(u8, after_on, '(') orelse return types.ParseError.InvalidSql;
    const rparen = std.mem.lastIndexOfScalar(u8, after_on, ')') orelse return types.ParseError.InvalidSql;
    if (rparen <= lparen + 1) return types.ParseError.InvalidSql;

    const table_name = std.mem.trim(u8, after_on[0..lparen], " \t\r\n");
    if (table_name.len == 0) return types.ParseError.InvalidSql;

    const cols = std.mem.trim(u8, after_on[lparen + 1 .. rparen], " \t\r\n");
    if (cols.len == 0) return types.ParseError.InvalidSql;

    return .{ .create_index = .{
        .index_name = try allocator.dupe(u8, index_name),
        .table_name = try allocator.dupe(u8, table_name),
    } };
}

pub fn parseInsert(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const after_kw = sql_text["INSERT INTO ".len..];
    const values_pos = common.indexOfIgnoreCase(after_kw, " VALUES") orelse return types.ParseError.InvalidSql;
    const target_part = std.mem.trim(u8, after_kw[0..values_pos], " \t\r\n");
    const values_part = std.mem.trimStart(u8, after_kw[values_pos + " VALUES".len ..], " \t\r\n");

    if (target_part.len == 0) return types.ParseError.InvalidSql;

    var table_name = target_part;
    var columns: ?[]const []const u8 = null;

    if (std.mem.lastIndexOfScalar(u8, target_part, ')')) |rparen| {
        const lparen = std.mem.lastIndexOfScalar(u8, target_part[0..rparen], '(') orelse return types.ParseError.InvalidSql;
        table_name = std.mem.trim(u8, target_part[0..lparen], " \t\r\n");
        const cols_part = target_part[lparen + 1 .. rparen];

        var cols = std.ArrayList([]const u8).empty;
        defer cols.deinit(allocator);
        var col_it = std.mem.splitScalar(u8, cols_part, ',');
        while (col_it.next()) |raw_col| {
            const col = std.mem.trim(u8, raw_col, " \t\r\n");
            if (col.len == 0) return types.ParseError.InvalidSql;
            try cols.append(allocator, try allocator.dupe(u8, col));
        }
        columns = try cols.toOwnedSlice(allocator);
    }

    if (table_name.len == 0) return types.ParseError.InvalidSql;

    const lparen = std.mem.indexOfScalar(u8, values_part, '(') orelse return types.ParseError.InvalidSql;
    const rparen = std.mem.lastIndexOfScalar(u8, values_part, ')') orelse return types.ParseError.InvalidSql;
    if (rparen <= lparen + 1) return types.ParseError.InvalidSql;

    var values = std.ArrayList(Value).empty;
    defer values.deinit(allocator);

    const value_slice = values_part[lparen + 1 .. rparen];
    var lit_it = std.mem.splitScalar(u8, value_slice, ',');
    while (lit_it.next()) |raw_lit| {
        const lit = std.mem.trim(u8, raw_lit, " \t\r\n");
        if (lit.len == 0) return types.ParseError.InvalidSql;
        try values.append(allocator, try parseLiteral(allocator, lit));
    }

    return types.Statement{ .insert = .{
        .table_name = try allocator.dupe(u8, table_name),
        .columns = columns,
        .values = try values.toOwnedSlice(allocator),
    } };
}

fn parseLiteral(allocator: std.mem.Allocator, lit: []const u8) types.ParseError!Value {
    if (common.eqlIgnoreCase(lit, "NULL")) return .null;

    if (lit.len >= 2 and lit[0] == '\'' and lit[lit.len - 1] == '\'') {
        return .{ .text = try allocator.dupe(u8, lit[1 .. lit.len - 1]) };
    }

    if (std.mem.indexOfScalar(u8, lit, '.')) |_| {
        const f = std.fmt.parseFloat(f64, lit) catch return types.ParseError.InvalidLiteral;
        return .{ .real = f };
    }

    const i = std.fmt.parseInt(i64, lit, 10) catch return types.ParseError.InvalidLiteral;
    return .{ .integer = i };
}

fn firstToken(s: []const u8) []const u8 {
    var it = std.mem.tokenizeAny(u8, s, " \t\r\n");
    return it.next() orelse "";
}
