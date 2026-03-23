const Value = @import("../value.zig").Value;

pub const ParseError = error{ InvalidSql, UnsupportedSql, InvalidLiteral, OutOfMemory };

pub const Statement = union(enum) {
    create_table: CreateTable,
    create_index: CreateIndex,
    insert: Insert,
    select: Select,
    compound_select: CompoundSelect,
};

pub const CreateTable = struct {
    table_name: []const u8,
    columns: []const []const u8,
};

pub const CreateIndex = struct {
    index_name: []const u8,
    table_name: []const u8,
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
    from: []const FromItem,
    projections: []const []const u8,
    where_expr: ?[]const u8,
    order_by: []const OrderTerm,
};

pub const FromItem = struct {
    table_name: []const u8,
    alias: ?[]const u8,
};

pub const SetOp = enum {
    union_distinct,
    union_all,
    intersect,
    except,
};

pub const CompoundSelect = struct {
    arms: []const []const u8,
    ops: []const SetOp,
    order_by: []const OrderTerm,
};
