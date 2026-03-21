const std = @import("std");
const Value = @import("value.zig").Value;

pub const ParseError = error{ InvalidSql, UnsupportedSql, InvalidLiteral, OutOfMemory };

pub const Statement = union(enum) {
    create_table: CreateTable,
    insert: Insert,
    select: Select,
};

pub const CreateTable = struct {
    table_name: []const u8,
    columns: []const []const u8,
};

pub const Insert = struct {
    table_name: []const u8,
    columns: ?[]const []const u8,
    values: []const Value,
};

pub const Select = struct {
    table_name: []const u8,
    projections: []const []const u8,
    where_eq: ?WhereEq,
};

pub const WhereEq = struct {
    column: []const u8,
    value: Value,
};

pub fn parse(allocator: std.mem.Allocator, sql_raw: []const u8) ParseError!Statement {
    const sql = std.mem.trim(u8, sql_raw, " \t\r\n;");

    if (startsWithIgnoreCase(sql, "CREATE TABLE ")) {
        return parseCreate(allocator, sql);
    }
    if (startsWithIgnoreCase(sql, "INSERT INTO ")) {
        return parseInsert(allocator, sql);
    }
    if (startsWithIgnoreCase(sql, "SELECT ")) {
        return parseSelect(allocator, sql);
    }
    return ParseError.UnsupportedSql;
}

fn parseCreate(allocator: std.mem.Allocator, sql: []const u8) ParseError!Statement {
    const after_kw = sql["CREATE TABLE ".len..];
    const lparen = std.mem.indexOfScalar(u8, after_kw, '(') orelse return ParseError.InvalidSql;
    const rparen = std.mem.lastIndexOfScalar(u8, after_kw, ')') orelse return ParseError.InvalidSql;
    if (rparen <= lparen + 1) return ParseError.InvalidSql;

    const table_name = std.mem.trim(u8, after_kw[0..lparen], " \t");
    if (table_name.len == 0) return ParseError.InvalidSql;

    var cols = std.ArrayList([]const u8).empty;
    defer cols.deinit(allocator);

    var col_it = std.mem.splitScalar(u8, after_kw[lparen + 1 .. rparen], ',');
    while (col_it.next()) |raw_col| {
        const col = std.mem.trim(u8, raw_col, " \t");
        if (col.len == 0) continue;
        const col_name = firstToken(col);
        if (col_name.len == 0) return ParseError.InvalidSql;
        try cols.append(allocator, try allocator.dupe(u8, col_name));
    }

    if (cols.items.len == 0) return ParseError.InvalidSql;

    return Statement{ .create_table = .{
        .table_name = try allocator.dupe(u8, table_name),
        .columns = try cols.toOwnedSlice(allocator),
    } };
}

fn parseInsert(allocator: std.mem.Allocator, sql: []const u8) ParseError!Statement {
    const after_kw = sql["INSERT INTO ".len..];
    const values_pos = indexOfIgnoreCase(after_kw, " VALUES") orelse return ParseError.InvalidSql;
    const target_part = std.mem.trim(u8, after_kw[0..values_pos], " \t");
    const values_part = std.mem.trimStart(u8, after_kw[values_pos + " VALUES".len ..], " \t");

    if (target_part.len == 0) return ParseError.InvalidSql;

    var table_name = target_part;
    var columns: ?[]const []const u8 = null;

    if (std.mem.lastIndexOfScalar(u8, target_part, ')')) |rparen| {
        const lparen = std.mem.lastIndexOfScalar(u8, target_part[0..rparen], '(') orelse return ParseError.InvalidSql;
        table_name = std.mem.trim(u8, target_part[0..lparen], " \t");
        const cols_part = target_part[lparen + 1 .. rparen];

        var cols = std.ArrayList([]const u8).empty;
        defer cols.deinit(allocator);
        var col_it = std.mem.splitScalar(u8, cols_part, ',');
        while (col_it.next()) |raw_col| {
            const col = std.mem.trim(u8, raw_col, " \t");
            if (col.len == 0) return ParseError.InvalidSql;
            try cols.append(allocator, try allocator.dupe(u8, col));
        }
        columns = try cols.toOwnedSlice(allocator);
    }

    if (table_name.len == 0) return ParseError.InvalidSql;

    const lparen = std.mem.indexOfScalar(u8, values_part, '(') orelse return ParseError.InvalidSql;
    const rparen = std.mem.lastIndexOfScalar(u8, values_part, ')') orelse return ParseError.InvalidSql;
    if (rparen <= lparen + 1) return ParseError.InvalidSql;

    var values = std.ArrayList(Value).empty;
    defer values.deinit(allocator);

    var lit_it = std.mem.splitScalar(u8, values_part[lparen + 1 .. rparen], ',');
    while (lit_it.next()) |raw_lit| {
        const lit = std.mem.trim(u8, raw_lit, " \t");
        if (lit.len == 0) return ParseError.InvalidSql;
        try values.append(allocator, try parseLiteral(allocator, lit));
    }

    return Statement{ .insert = .{
        .table_name = try allocator.dupe(u8, table_name),
        .columns = columns,
        .values = try values.toOwnedSlice(allocator),
    } };
}

fn parseSelect(allocator: std.mem.Allocator, sql: []const u8) ParseError!Statement {
    const from_idx = indexOfIgnoreCase(sql, " FROM ") orelse return ParseError.InvalidSql;
    const proj_sql = std.mem.trim(u8, sql["SELECT ".len..from_idx], " \t");
    const after_from = sql[from_idx + " FROM ".len ..];

    const where_idx_rel = indexOfIgnoreCase(after_from, " WHERE ");
    const table_name = if (where_idx_rel) |idx|
        std.mem.trim(u8, after_from[0..idx], " \t")
    else
        std.mem.trim(u8, after_from, " \t");
    if (table_name.len == 0) return ParseError.InvalidSql;

    var projections = std.ArrayList([]const u8).empty;
    defer projections.deinit(allocator);

    if (std.mem.eql(u8, proj_sql, "*")) {
        try projections.append(allocator, try allocator.dupe(u8, "*"));
    } else {
        var proj_it = std.mem.splitScalar(u8, proj_sql, ',');
        while (proj_it.next()) |raw_proj| {
            const proj = std.mem.trim(u8, raw_proj, " \t");
            if (proj.len == 0) return ParseError.InvalidSql;
            try projections.append(allocator, try allocator.dupe(u8, proj));
        }
    }

    var where_eq: ?WhereEq = null;
    if (where_idx_rel) |idx| {
        const where_sql = std.mem.trim(u8, after_from[idx + " WHERE ".len ..], " \t");
        const eq_idx = std.mem.indexOfScalar(u8, where_sql, '=') orelse return ParseError.InvalidSql;
        const lhs = std.mem.trim(u8, where_sql[0..eq_idx], " \t");
        const rhs = std.mem.trim(u8, where_sql[eq_idx + 1 ..], " \t");
        if (lhs.len == 0 or rhs.len == 0) return ParseError.InvalidSql;
        where_eq = .{ .column = try allocator.dupe(u8, lhs), .value = try parseLiteral(allocator, rhs) };
    }

    return Statement{ .select = .{
        .table_name = try allocator.dupe(u8, table_name),
        .projections = try projections.toOwnedSlice(allocator),
        .where_eq = where_eq,
    } };
}

fn parseLiteral(allocator: std.mem.Allocator, lit: []const u8) ParseError!Value {
    if (eqlIgnoreCase(lit, "NULL")) return .null;

    if (lit.len >= 2 and lit[0] == '\'' and lit[lit.len - 1] == '\'') {
        return .{ .text = try allocator.dupe(u8, lit[1 .. lit.len - 1]) };
    }

    if (std.mem.indexOfScalar(u8, lit, '.')) |_| {
        const f = std.fmt.parseFloat(f64, lit) catch return ParseError.InvalidLiteral;
        return .{ .real = f };
    }

    const i = std.fmt.parseInt(i64, lit, 10) catch return ParseError.InvalidLiteral;
    return .{ .integer = i };
}

fn firstToken(s: []const u8) []const u8 {
    var it = std.mem.splitAny(u8, s, " \t");
    return it.first();
}

fn startsWithIgnoreCase(haystack: []const u8, prefix: []const u8) bool {
    if (haystack.len < prefix.len) return false;
    return eqlIgnoreCase(haystack[0..prefix.len], prefix);
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (std.ascii.toLower(ca) != std.ascii.toLower(cb)) return false;
    }
    return true;
}

fn indexOfIgnoreCase(haystack: []const u8, needle: []const u8) ?usize {
    if (needle.len == 0 or haystack.len < needle.len) return null;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return i;
    }
    return null;
}
