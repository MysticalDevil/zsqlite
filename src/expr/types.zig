const Value = @import("../value.zig").Value;

pub const BinaryOp = enum {
    add,
    sub,
    mul,
    div,
    eq,
    ne,
    lt,
    le,
    gt,
    ge,
    and_op,
    or_op,
};

pub const UnaryOp = enum {
    neg,
    not_op,
};

pub const Identifier = struct {
    qualifier: ?[]const u8,
    name: []const u8,
};

pub const Between = struct {
    target: *Expr,
    low: *Expr,
    high: *Expr,
    not_between: bool,
};

pub const Call = struct {
    name: []const u8,
    args: []const *Expr,
    star_arg: bool,
    distinct: bool,
};

pub const CaseWhen = struct {
    cond: *Expr,
    value: *Expr,
};

pub const CaseExpr = struct {
    base: ?*Expr,
    whens: []const CaseWhen,
    else_expr: ?*Expr,
};

pub const IsNull = struct {
    target: *Expr,
    not_null: bool,
};

pub const InList = struct {
    target: *Expr,
    items: []const *Expr,
    subquery: ?[]const u8,
    not_in: bool,
};

pub const Expr = union(enum) {
    literal: Value,
    ident: Identifier,
    unary: struct {
        op: UnaryOp,
        expr: *Expr,
    },
    binary: struct {
        op: BinaryOp,
        left: *Expr,
        right: *Expr,
    },
    between: Between,
    is_null: IsNull,
    in_list: InList,
    call: Call,
    case_expr: CaseExpr,
    subquery: []const u8,
    exists_subquery: []const u8,
};
