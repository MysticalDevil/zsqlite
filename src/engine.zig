const std = @import("std");
const Value = @import("value.zig").Value;
const sql = @import("sql.zig");
const vm = @import("vm.zig");

pub const Error = error{
    OutOfMemory,
    TableAlreadyExists,
    UnknownTable,
    ColumnCountMismatch,
    UnknownColumn,
    InvalidSql,
    UnsupportedSql,
    InvalidLiteral,
};

pub const ExecResult = struct {
    rows_affected: usize = 0,
};

pub const RowState = enum { row, done };

pub const RowSet = struct {
    allocator: std.mem.Allocator,
    rows: std.ArrayList([]Value),

    pub fn init(allocator: std.mem.Allocator) RowSet {
        return .{ .allocator = allocator, .rows = std.ArrayList([]Value).empty };
    }

    pub fn deinit(self: *RowSet) void {
        for (self.rows.items) |r| {
            self.allocator.free(r);
        }
        self.rows.deinit(self.allocator);
    }
};

const Table = struct {
    name: []const u8,
    columns: std.ArrayList([]const u8),
    rows: std.ArrayList([]Value),

    fn deinit(self: *Table, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        for (self.columns.items) |c| allocator.free(c);
        self.columns.deinit(allocator);
        for (self.rows.items) |row| {
            for (row) |v| switch (v) {
                .text => |t| allocator.free(t),
                .blob => |b| allocator.free(b),
                else => {},
            };
            allocator.free(row);
        }
        self.rows.deinit(allocator);
    }
};

pub const Engine = struct {
    allocator: std.mem.Allocator,
    tables: std.StringHashMap(Table),

    pub fn init(allocator: std.mem.Allocator) Engine {
        return .{ .allocator = allocator, .tables = std.StringHashMap(Table).init(allocator) };
    }

    pub fn deinit(self: *Engine) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var table = entry.value_ptr.*;
            table.deinit(self.allocator);
        }
        self.tables.deinit();
    }

    pub fn exec(self: *Engine, sql_text: []const u8) Error!ExecResult {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);
        switch (stmt) {
            .create_table => |create| {
                try self.handleCreate(create);
                return .{ .rows_affected = 0 };
            },
            .insert => |ins| {
                try self.handleInsert(ins);
                return .{ .rows_affected = 1 };
            },
            .select => return Error.UnsupportedSql,
        }
    }

    pub fn query(self: *Engine, allocator: std.mem.Allocator, sql_text: []const u8) Error!RowSet {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return mapParseError(err);
        return switch (stmt) {
            .select => |sel| try self.handleSelect(allocator, sel),
            else => Error.UnsupportedSql,
        };
    }

    fn handleCreate(self: *Engine, create: sql.CreateTable) Error!void {
        const key = try self.allocator.dupe(u8, create.table_name);
        errdefer self.allocator.free(key);
        const gop = try self.tables.getOrPut(key);
        if (gop.found_existing) {
            self.allocator.free(key);
            return Error.TableAlreadyExists;
        }

        var table = Table{
            .name = try self.allocator.dupe(u8, create.table_name),
            .columns = std.ArrayList([]const u8).empty,
            .rows = std.ArrayList([]Value).empty,
        };
        for (create.columns) |col| {
            try table.columns.append(self.allocator, try self.allocator.dupe(u8, col));
        }
        gop.value_ptr.* = table;
    }

    fn handleInsert(self: *Engine, ins: sql.Insert) Error!void {
        const table = self.tables.getPtr(ins.table_name) orelse return Error.UnknownTable;
        const row = try self.allocator.alloc(Value, table.columns.items.len);

        for (row) |*cell| cell.* = .null;

        if (ins.columns) |cols| {
            if (ins.values.len != cols.len) return Error.ColumnCountMismatch;
            for (cols, ins.values) |col_name, v| {
                const idx = vm.columnIndex(table.columns.items, col_name) orelse return Error.UnknownColumn;
                row[idx] = try v.clone(self.allocator);
            }
        } else {
            if (ins.values.len != table.columns.items.len) return Error.ColumnCountMismatch;
            for (ins.values, 0..) |v, i| {
                row[i] = try v.clone(self.allocator);
            }
        }
        try table.rows.append(self.allocator, row);
    }

    fn handleSelect(self: *Engine, allocator: std.mem.Allocator, sel: sql.Select) Error!RowSet {
        var result = RowSet.init(allocator);
        errdefer result.deinit();

        const table = self.tables.get(sel.table_name) orelse return Error.UnknownTable;
        const cols = table.columns.items;
        const program = vm.Program.fromSelect(sel);

        for (table.rows.items) |row| {
            const ok = vm.matchesWhere(cols, row, program.where_eq) catch return Error.UnknownColumn;
            if (!ok) continue;
            const projected = vm.projectRow(allocator, cols, row, program.projections) catch return Error.UnknownColumn;
            try result.rows.append(allocator, projected);
        }

        return result;
    }

    pub fn prepare(self: *Engine, allocator: std.mem.Allocator, sql_text: []const u8) Error!Stmt {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const stmt = sql.parse(arena.allocator(), sql_text) catch |err| return Engine.mapParseError(err);

        return switch (stmt) {
            .select => .{ .row_set = try self.query(allocator, sql_text), .cursor = 0 },
            else => blk: {
                try self.exec(sql_text);
                break :blk .{ .row_set = null, .cursor = 0 };
            },
        };
    }

    fn mapParseError(err: anyerror) Error {
        return switch (err) {
            sql.ParseError.InvalidSql => Error.InvalidSql,
            sql.ParseError.UnsupportedSql => Error.UnsupportedSql,
            sql.ParseError.InvalidLiteral => Error.InvalidLiteral,
            sql.ParseError.OutOfMemory => Error.OutOfMemory,
            else => Error.InvalidSql,
        };
    }
};

pub const Stmt = struct {
    row_set: ?RowSet,
    cursor: usize,

    pub fn deinit(self: *Stmt) void {
        if (self.row_set) |*rs| rs.deinit();
    }

    pub fn step(self: *Stmt) RowState {
        const rs = &(self.row_set orelse return .done);
        if (self.cursor >= rs.rows.items.len) return .done;
        self.cursor += 1;
        return .row;
    }

    pub fn column(self: *Stmt, idx: usize) Value {
        const rs = &(self.row_set orelse unreachable);
        const row_idx = self.cursor - 1;
        return rs.rows.items[row_idx][idx];
    }

    pub fn bind(_: *Stmt, _: usize, _: Value) Error!void {
        return Error.UnsupportedSql;
    }
};
