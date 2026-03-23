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
    var integer_affinity = std.ArrayList(bool).empty;
    defer integer_affinity.deinit(allocator);
    var primary_key_col: ?usize = null;

    var col_idx: usize = 0;
    var col_it = std.mem.splitScalar(u8, after_kw[lparen + 1 .. rparen], ',');
    while (col_it.next()) |raw_col| {
        const col = std.mem.trim(u8, raw_col, " \t\r\n");
        if (col.len == 0) continue;
        const col_name = firstToken(col);
        if (col_name.len == 0) return types.ParseError.InvalidSql;
        try cols.append(allocator, try allocator.dupe(u8, col_name));
        try integer_affinity.append(allocator, common.indexOfIgnoreCase(col, "INTEGER") != null);
        if (primary_key_col == null and common.indexOfIgnoreCase(col, "PRIMARY KEY") != null) {
            primary_key_col = col_idx;
        }
        col_idx += 1;
    }

    if (cols.items.len == 0) return types.ParseError.InvalidSql;

    return types.Statement{ .create_table = .{
        .table_name = try allocator.dupe(u8, table_name),
        .columns = try cols.toOwnedSlice(allocator),
        .integer_affinity = try integer_affinity.toOwnedSlice(allocator),
        .primary_key_col = primary_key_col,
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
    const values_pos = common.indexOfIgnoreCase(after_kw, " VALUES");
    const select_pos = common.indexOfIgnoreCase(after_kw, " SELECT");
    const split_pos = if (values_pos) |idx|
        idx
    else if (select_pos) |idx|
        idx
    else
        return types.ParseError.InvalidSql;
    const target_part = std.mem.trim(u8, after_kw[0..split_pos], " \t\r\n");
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

    if (values_pos) |idx| {
        const values_part = std.mem.trimStart(u8, after_kw[idx + " VALUES".len ..], " \t\r\n");
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
            .select_sql = null,
            .or_replace = false,
        } };
    }

    return types.Statement{ .insert = .{
        .table_name = try allocator.dupe(u8, table_name),
        .columns = columns,
        .values = null,
        .select_sql = try allocator.dupe(u8, std.mem.trim(u8, after_kw[select_pos.? + 1 ..], " \t\r\n")),
        .or_replace = false,
    } };
}

pub fn parseInsertOrReplace(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const rewritten = try std.fmt.allocPrint(allocator, "INSERT INTO {s}", .{sql_text["INSERT OR REPLACE INTO ".len..]});
    const stmt = try parseInsert(allocator, rewritten);
    switch (stmt) {
        .insert => |ins| {
            var out = ins;
            out.or_replace = true;
            return .{ .insert = out };
        },
        else => return types.ParseError.InvalidSql,
    }
}

pub fn parseReplace(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const rewritten = try std.fmt.allocPrint(allocator, "INSERT INTO {s}", .{sql_text["REPLACE INTO ".len..]});
    const stmt = try parseInsert(allocator, rewritten);
    switch (stmt) {
        .insert => |ins| {
            var out = ins;
            out.or_replace = true;
            return .{ .insert = out };
        },
        else => return types.ParseError.InvalidSql,
    }
}

pub fn parseUpdate(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const after_kw = std.mem.trim(u8, sql_text["UPDATE ".len..], " \t\r\n");
    const set_idx = common.indexOfIgnoreCase(after_kw, " SET ") orelse return types.ParseError.InvalidSql;
    const table_name = std.mem.trim(u8, after_kw[0..set_idx], " \t\r\n");
    if (table_name.len == 0) return types.ParseError.InvalidSql;

    const after_set = after_kw[set_idx + " SET ".len ..];
    const where_idx = common.findTopLevelKeyword(after_set, "WHERE");
    const assign_text = if (where_idx) |idx|
        std.mem.trim(u8, after_set[0..idx], " \t\r\n")
    else
        std.mem.trim(u8, after_set, " \t\r\n");
    if (assign_text.len == 0) return types.ParseError.InvalidSql;

    const parts = common.splitTopLevelComma(allocator, assign_text) catch return types.ParseError.InvalidSql;
    var assignments = std.ArrayList(types.Assignment).empty;
    defer assignments.deinit(allocator);
    for (parts) |part| {
        const eq_idx = std.mem.indexOfScalar(u8, part, '=') orelse return types.ParseError.InvalidSql;
        const column_name = std.mem.trim(u8, part[0..eq_idx], " \t\r\n");
        const expr = std.mem.trim(u8, part[eq_idx + 1 ..], " \t\r\n");
        if (column_name.len == 0 or expr.len == 0) return types.ParseError.InvalidSql;
        try assignments.append(allocator, .{
            .column_name = try allocator.dupe(u8, column_name),
            .expr = try allocator.dupe(u8, expr),
        });
    }

    const where_expr = if (where_idx) |idx| blk: {
        const text = std.mem.trim(u8, after_set[idx + "WHERE".len ..], " \t\r\n");
        if (text.len == 0) return types.ParseError.InvalidSql;
        break :blk try allocator.dupe(u8, text);
    } else null;

    return .{ .update = .{
        .table_name = try allocator.dupe(u8, table_name),
        .assignments = try assignments.toOwnedSlice(allocator),
        .where_expr = where_expr,
    } };
}

pub fn parseCreateView(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const after_kw = if (common.startsWithIgnoreCase(sql_text, "CREATE TEMPORARY VIEW "))
        sql_text["CREATE TEMPORARY VIEW ".len..]
    else if (common.startsWithIgnoreCase(sql_text, "CREATE TEMP VIEW "))
        sql_text["CREATE TEMP VIEW ".len..]
    else
        sql_text["CREATE VIEW ".len..];
    const as_idx = common.indexOfIgnoreCase(after_kw, " AS ") orelse return types.ParseError.InvalidSql;
    const view_name = std.mem.trim(u8, after_kw[0..as_idx], " \t\r\n");
    const select_sql = std.mem.trim(u8, after_kw[as_idx + " AS ".len ..], " \t\r\n");
    if (view_name.len == 0 or select_sql.len == 0) return types.ParseError.InvalidSql;
    return .{ .create_view = .{
        .view_name = try allocator.dupe(u8, view_name),
        .select_sql = try allocator.dupe(u8, select_sql),
    } };
}

pub fn parseCreateTrigger(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const after_kw = std.mem.trim(u8, sql_text["CREATE TRIGGER ".len..], " \t\r\n");
    const name_end = std.mem.indexOfAny(u8, after_kw, " \t\r\n") orelse return types.ParseError.InvalidSql;
    const trigger_name = std.mem.trim(u8, after_kw[0..name_end], " \t\r\n");
    if (trigger_name.len == 0) return types.ParseError.InvalidSql;

    var rest = std.mem.trim(u8, after_kw[name_end..], " \t\r\n");
    var timing: types.TriggerTiming = .none;
    if (common.startsWithIgnoreCase(rest, "BEFORE ")) {
        timing = .before;
        rest = std.mem.trim(u8, rest["BEFORE ".len..], " \t\r\n");
    } else if (common.startsWithIgnoreCase(rest, "AFTER ")) {
        timing = .after;
        rest = std.mem.trim(u8, rest["AFTER ".len..], " \t\r\n");
    }

    const event_end = std.mem.indexOfAny(u8, rest, " \t\r\n") orelse return types.ParseError.InvalidSql;
    const event_text = std.mem.trim(u8, rest[0..event_end], " \t\r\n");
    const event: types.TriggerEvent = if (common.eqlIgnoreCase(event_text, "INSERT"))
        .insert
    else if (common.eqlIgnoreCase(event_text, "UPDATE"))
        .update
    else if (common.eqlIgnoreCase(event_text, "DELETE"))
        .delete
    else
        return types.ParseError.UnsupportedSql;

    rest = std.mem.trim(u8, rest[event_end..], " \t\r\n");
    if (!common.startsWithIgnoreCase(rest, "ON ")) return types.ParseError.InvalidSql;
    rest = std.mem.trim(u8, rest["ON ".len..], " \t\r\n");

    const begin_idx = common.indexOfIgnoreCase(rest, " BEGIN ") orelse return types.ParseError.InvalidSql;
    const table_name = std.mem.trim(u8, rest[0..begin_idx], " \t\r\n");
    if (table_name.len == 0) return types.ParseError.InvalidSql;

    const body_and_end = std.mem.trim(u8, rest[begin_idx + " BEGIN ".len ..], " \t\r\n");
    if (!common.endsWithIgnoreCase(body_and_end, " END")) return types.ParseError.InvalidSql;
    const body_sql = std.mem.trim(u8, body_and_end[0 .. body_and_end.len - " END".len], " \t\r\n");
    if (body_sql.len == 0) return types.ParseError.InvalidSql;

    return .{ .create_trigger = .{
        .trigger_name = try allocator.dupe(u8, trigger_name),
        .table_name = try allocator.dupe(u8, table_name),
        .timing = timing,
        .event = event,
        .body_sql = try allocator.dupe(u8, body_sql),
    } };
}

pub fn parseDropTable(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    return .{ .drop_table = try parseDropObject(allocator, sql_text["DROP TABLE ".len..]) };
}

pub fn parseDropIndex(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    return .{ .drop_index = try parseDropObject(allocator, sql_text["DROP INDEX ".len..]) };
}

pub fn parseDropTrigger(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    return .{ .drop_trigger = try parseDropObject(allocator, sql_text["DROP TRIGGER ".len..]) };
}

pub fn parseDropView(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    return .{ .drop_view = try parseDropObject(allocator, sql_text["DROP VIEW ".len..]) };
}

pub fn parseReindex(allocator: std.mem.Allocator, sql_text: []const u8) types.ParseError!types.Statement {
    const target = std.mem.trim(u8, sql_text["REINDEX ".len..], " \t\r\n");
    if (target.len == 0) return types.ParseError.InvalidSql;
    return .{ .reindex = .{ .target_name = try allocator.dupe(u8, target) } };
}

fn parseDropObject(allocator: std.mem.Allocator, remainder: []const u8) types.ParseError!types.DropObject {
    const trimmed = std.mem.trim(u8, remainder, " \t\r\n");
    var if_exists = false;
    var object_name = trimmed;
    if (common.startsWithIgnoreCase(trimmed, "IF EXISTS ")) {
        if_exists = true;
        object_name = std.mem.trim(u8, trimmed["IF EXISTS ".len..], " \t\r\n");
    }
    if (object_name.len == 0) return types.ParseError.InvalidSql;
    return .{
        .object_name = try allocator.dupe(u8, object_name),
        .if_exists = if_exists,
    };
}

fn parseLiteral(allocator: std.mem.Allocator, lit: []const u8) types.ParseError!Value {
    if (common.eqlIgnoreCase(lit, "NULL")) return .null;

    if (lit.len >= 2 and lit[0] == '\'' and lit[lit.len - 1] == '\'') {
        return .{ .text = try allocator.dupe(u8, lit[1 .. lit.len - 1]) };
    }

    if (std.mem.indexOf(u8, lit, "<<")) |shift_idx| {
        const lhs_text = std.mem.trim(u8, lit[0..shift_idx], " \t\r\n");
        const rhs_text = std.mem.trim(u8, lit[shift_idx + 2 ..], " \t\r\n");
        const lhs = std.fmt.parseInt(i64, lhs_text, 10) catch return types.ParseError.InvalidLiteral;
        const rhs = std.fmt.parseInt(u6, rhs_text, 10) catch return types.ParseError.InvalidLiteral;
        const shifted = @as(u64, @bitCast(lhs)) << rhs;
        return .{ .integer = @bitCast(shifted) };
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
