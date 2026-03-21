const std = @import("std");
const types = @import("types.zig");
const common = @import("common.zig");
const create_insert = @import("create_insert.zig");
const select_mod = @import("select.zig");

pub fn parse(allocator: std.mem.Allocator, sql_raw: []const u8) types.ParseError!types.Statement {
    const sql_text = std.mem.trim(u8, sql_raw, " \t\r\n;");

    if (common.startsWithIgnoreCase(sql_text, "CREATE TABLE ")) {
        return create_insert.parseCreate(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "INSERT INTO ")) {
        return create_insert.parseInsert(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "SELECT ")) {
        return select_mod.parseSelect(allocator, sql_text);
    }
    return types.ParseError.UnsupportedSql;
}
