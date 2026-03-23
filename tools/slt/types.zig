pub const SortMode = enum { nosort, rowsort, valuesort };

pub const DebugOptions = struct {
    enabled: bool,
    label_filter: ?[]const u8,
    no_label_only: bool,
    join_min: ?usize,
    join_max: ?usize,
    join_variant_min: ?usize,
    join_variant_max: ?usize,
    trace_query: bool,
    stop_on_fail: bool,
};

pub const QueryMeta = struct {
    label: ?[]const u8,
    column_types: []const u8,
};

pub const StatementExpectation = enum { ok, err };

pub const CompareDetails = struct {
    ok: bool,
    actual_count: usize,
    actual_hash: ?[16]u8,
    expected_count: ?usize,
    expected_hash: ?[16]u8,
};

pub const PendingDirective = union(enum) {
    none,
    skipif: []const u8,
    onlyif: []const u8,
};

pub const JoinKey = struct {
    primary: usize,
    variant: ?usize,
};

pub const QueryBlock = struct {
    sql: []u8,
    expected: @import("std").ArrayList([]const u8),
};

pub const ExpectedHash = struct {
    expected_count: usize,
    expected_hash: [16]u8,
};
