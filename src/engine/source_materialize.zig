const std = @import("std");
const sql = @import("../sql.zig");
const shared = @import("shared.zig");
const select_helpers = @import("select_helpers.zig");
const result_utils = @import("result_utils.zig");

const Table = shared.Table;
const EvalCtx = shared.EvalCtx;
const QueryRuntime = shared.QueryRuntime;
const Error = shared.Error;

pub fn materializeSubquery(
    self: anytype,
    allocator: std.mem.Allocator,
    from_item: sql.FromItem,
    parent_ctx: ?*const EvalCtx,
    runtime: *QueryRuntime,
    temp_tables: *std.ArrayList(*Table),
) Error!*Table {
    const subquery_sql = from_item.subquery_sql orelse return Error.InvalidSql;
    const alias = from_item.alias orelse return Error.InvalidSql;

    const stmt = try self.getParsedSubquery(runtime, subquery_sql);
    var row_set = try self.queryParsedWithParent(allocator, stmt, parent_ctx, runtime);
    defer row_set.deinit();

    const temp_table = try allocator.create(Table);
    errdefer allocator.destroy(temp_table);
    temp_table.* = .{
        .name = try allocator.dupe(u8, alias),
        .columns = std.ArrayList([]const u8).empty,
        .integer_affinity = std.ArrayList(bool).empty,
        .column_has_null = std.ArrayList(bool).empty,
        .rows = std.ArrayList([]@import("../value.zig").Value).empty,
        .row_states = std.ArrayList(@import("shared.zig").RowState).empty,
        .primary_key_col = null,
    };
    errdefer temp_table.deinit(allocator);

    const column_names = try deriveSubqueryColumnNames(allocator, stmt);
    defer {
        for (column_names) |name| allocator.free(name);
        allocator.free(column_names);
    }
    for (column_names) |name| {
        try temp_table.columns.append(allocator, try allocator.dupe(u8, name));
        try temp_table.integer_affinity.append(allocator, false);
        try temp_table.column_has_null.append(allocator, false);
    }

    for (row_set.rows.items) |row| {
        const copied = try allocator.alloc(@import("../value.zig").Value, row.len);
        for (row, 0..) |value, i| {
            copied[i] = try result_utils.cloneResultValue(allocator, value);
            if (value == .null) temp_table.column_has_null.items[i] = true;
        }
        try temp_table.rows.append(allocator, copied);
        try temp_table.row_states.append(allocator, .live);
    }
    try temp_tables.append(allocator, temp_table);
    return temp_table;
}

fn deriveSubqueryColumnNames(allocator: std.mem.Allocator, stmt: sql.Statement) Error![]const []const u8 {
    return switch (stmt) {
        .select => |sel| try deriveSelectColumnNames(allocator, sel),
        .compound_select => |compound| blk: {
            if (compound.arms.len == 0) return Error.InvalidSql;
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const arm_stmt = sql.parse(arena.allocator(), compound.arms[0]) catch return Error.InvalidSql;
            if (arm_stmt != .select) return Error.UnsupportedSql;
            break :blk try deriveSelectColumnNames(allocator, arm_stmt.select);
        },
        else => Error.InvalidSql,
    };
}

fn deriveSelectColumnNames(allocator: std.mem.Allocator, sel: sql.Select) Error![]const []const u8 {
    var names = std.ArrayList([]const u8).empty;
    defer names.deinit(allocator);

    for (sel.projections) |projection| {
        const trimmed = std.mem.trim(u8, projection, " \t\r\n");
        if (std.mem.eql(u8, trimmed, "*")) return Error.UnsupportedSql;
        if (select_helpers.parseQualifiedStar(trimmed) != null) return Error.UnsupportedSql;
        try names.append(allocator, try projectionColumnName(allocator, trimmed));
    }

    return names.toOwnedSlice(allocator);
}

fn projectionColumnName(allocator: std.mem.Allocator, projection: []const u8) std.mem.Allocator.Error![]const u8 {
    if (findTopLevelAsAlias(projection)) |idx| {
        return allocator.dupe(u8, std.mem.trim(u8, projection[idx + 4 ..], " \t\r\n"));
    }
    if (findTopLevelBareAlias(projection)) |idx| {
        return allocator.dupe(u8, std.mem.trim(u8, projection[idx + 1 ..], " \t\r\n"));
    }
    return allocator.dupe(u8, projection);
}

fn findTopLevelAsAlias(text: []const u8) ?usize {
    var in_string = false;
    var depth: usize = 0;
    var i: usize = 0;
    while (i + 4 <= text.len) : (i += 1) {
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
        if (depth == 0 and std.mem.eql(u8, text[i .. i + 4], " AS ")) return i;
    }
    return null;
}

fn findTopLevelBareAlias(text: []const u8) ?usize {
    var in_string = false;
    var depth: usize = 0;
    var last_space: ?usize = null;
    var i: usize = 0;
    while (i < text.len) : (i += 1) {
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
        if (depth == 0 and std.ascii.isWhitespace(c)) last_space = i;
    }
    return last_space;
}
