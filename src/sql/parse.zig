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
    if (common.startsWithIgnoreCase(sql_text, "CREATE INDEX ")) {
        return create_insert.parseCreateIndex(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "CREATE VIEW ")) {
        return create_insert.parseCreateView(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "CREATE TEMP VIEW ")) {
        return create_insert.parseCreateView(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "CREATE TEMPORARY VIEW ")) {
        return create_insert.parseCreateView(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "CREATE TRIGGER ")) {
        return create_insert.parseCreateTrigger(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "INSERT INTO ")) {
        return create_insert.parseInsert(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "INSERT OR REPLACE INTO ")) {
        return create_insert.parseInsertOrReplace(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "REPLACE INTO ")) {
        return create_insert.parseReplace(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "UPDATE ")) {
        return create_insert.parseUpdate(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "DROP TABLE ")) {
        return create_insert.parseDropTable(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "DROP INDEX ")) {
        return create_insert.parseDropIndex(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "DROP TRIGGER ")) {
        return create_insert.parseDropTrigger(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "DROP VIEW ")) {
        return create_insert.parseDropView(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "REINDEX ")) {
        return create_insert.parseReindex(allocator, sql_text);
    }
    if (common.startsWithIgnoreCase(sql_text, "SELECT ")) {
        return select_mod.parseSelect(allocator, sql_text);
    }
    return types.ParseError.UnsupportedSql;
}
