const Value = @import("../value.zig").Value;
const shared = @import("shared.zig");

pub const Stmt = struct {
    row_set: ?shared.RowSet,
    cursor: usize,

    pub fn deinit(self: *Stmt) void {
        if (self.row_set) |*rs| rs.deinit();
    }

    pub fn step(self: *Stmt) shared.RowState {
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

    pub fn bind(_: *Stmt, _: usize, _: Value) shared.Error!void {
        return shared.Error.UnsupportedSql;
    }
};
