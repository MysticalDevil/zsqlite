const Value = @import("../value.zig").Value;

pub const ParseError = error{ InvalidSql, UnsupportedSql, InvalidLiteral, OutOfMemory };

pub const Statement = union(enum) {
    create_table: CreateTable,
    create_index: CreateIndex,
    create_view: CreateView,
    create_trigger: CreateTrigger,
    insert: Insert,
    update: Update,
    drop_table: DropObject,
    drop_index: DropObject,
    drop_trigger: DropObject,
    drop_view: DropObject,
    reindex: Reindex,
    select: Select,
    compound_select: CompoundSelect,
};

pub const CreateTable = struct {
    table_name: []const u8,
    columns: []const []const u8,
    integer_affinity: []const bool,
    primary_key_col: ?usize,
};

pub const CreateIndex = struct {
    index_name: []const u8,
    table_name: []const u8,
};

pub const CreateView = struct {
    view_name: []const u8,
    select_sql: []const u8,
};

pub const TriggerTiming = enum {
    none,
    before,
    after,
};

pub const TriggerEvent = enum {
    insert,
    update,
    delete,
};

pub const CreateTrigger = struct {
    trigger_name: []const u8,
    table_name: []const u8,
    timing: TriggerTiming,
    event: TriggerEvent,
    body_sql: []const u8,
};

pub const Insert = struct {
    table_name: []const u8,
    columns: ?[]const []const u8,
    values: ?[]const Value,
    select_sql: ?[]const u8,
    or_replace: bool,
};

pub const Assignment = struct {
    column_name: []const u8,
    expr: []const u8,
};

pub const Update = struct {
    table_name: []const u8,
    assignments: []const Assignment,
    where_expr: ?[]const u8,
};

pub const DropObject = struct {
    object_name: []const u8,
    if_exists: bool,
};

pub const Reindex = struct {
    target_name: []const u8,
};

pub const OrderTerm = struct {
    expr: []const u8,
    is_ordinal: bool,
    ordinal: usize,
};

pub const Select = struct {
    distinct: bool,
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
