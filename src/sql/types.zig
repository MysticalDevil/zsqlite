const Value = @import("../value.zig").Value;

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

pub const OrderTerm = struct {
    expr: []const u8,
    is_ordinal: bool,
    ordinal: usize,
};

pub const Select = struct {
    table_name: []const u8,
    table_alias: ?[]const u8,
    projections: []const []const u8,
    where_expr: ?[]const u8,
    order_by: []const OrderTerm,
};
