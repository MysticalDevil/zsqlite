const std = @import("std");
const Value = @import("../value.zig").Value;
const types = @import("types.zig");
const tokenizer = @import("tokenizer.zig");

pub const ParseError = error{ InvalidExpression, OutOfMemory };

const Parser = struct {
    allocator: std.mem.Allocator,
    source: []const u8,
    tokens: []const tokenizer.Token,
    idx: usize,

    fn peek(self: *const Parser) ?tokenizer.Token {
        if (self.idx >= self.tokens.len) return null;
        return self.tokens[self.idx];
    }

    fn next(self: *Parser) ?tokenizer.Token {
        const tok = self.peek() orelse return null;
        self.idx += 1;
        return tok;
    }

    fn tokText(self: *const Parser, tok: tokenizer.Token) []const u8 {
        return self.source[tok.start..tok.end];
    }

    fn expectKind(self: *Parser, kind: tokenizer.TokenKind) ParseError!tokenizer.Token {
        const tok = self.next() orelse return ParseError.InvalidExpression;
        if (tok.kind != kind) return ParseError.InvalidExpression;
        return tok;
    }

    fn consumeKind(self: *Parser, kind: tokenizer.TokenKind) ParseError!void {
        const tok = try self.expectKind(kind);
        if (tok.kind == kind) return;
        return ParseError.InvalidExpression;
    }

    fn matchKind(self: *Parser, kind: tokenizer.TokenKind) bool {
        const tok = self.peek() orelse return false;
        if (tok.kind != kind) return false;
        self.idx += 1;
        return true;
    }

    fn matchKeyword(self: *Parser, word: []const u8) bool {
        const tok = self.peek() orelse return false;
        if (tok.kind != .ident) return false;
        if (!eqlIgnoreCase(self.tokText(tok), word)) return false;
        self.idx += 1;
        return true;
    }

    fn parseExpr(self: *Parser) ParseError!*types.Expr {
        return self.parseOr();
    }

    fn parseOr(self: *Parser) ParseError!*types.Expr {
        var left = try self.parseAnd();
        while (self.matchKeyword("OR")) {
            const right = try self.parseAnd();
            left = try self.makeBinary(.or_op, left, right);
        }
        return left;
    }

    fn parseAnd(self: *Parser) ParseError!*types.Expr {
        var left = try self.parseNot();
        while (self.matchKeyword("AND")) {
            const right = try self.parseNot();
            left = try self.makeBinary(.and_op, left, right);
        }
        return left;
    }

    fn parseNot(self: *Parser) ParseError!*types.Expr {
        if (self.matchKeyword("NOT")) {
            const inner = try self.parseNot();
            return self.makeUnary(.not_op, inner);
        }
        return self.parseCompare();
    }

    fn parseCompare(self: *Parser) ParseError!*types.Expr {
        const left = try self.parseAdd();

        if (self.matchKeyword("NOT")) {
            if (self.matchKeyword("NULL")) {
                return self.makeIsNull(left, true);
            }
            if (self.matchKeyword("BETWEEN")) {
                const low = try self.parseAdd();
                if (!self.matchKeyword("AND")) return ParseError.InvalidExpression;
                const high = try self.parseAdd();
                return self.makeBetween(left, low, high, true);
            }
            if (self.matchKeyword("IN")) return self.parseInList(left, true);
            self.idx -= 1;
        }

        if (self.matchKeyword("BETWEEN")) {
            const low = try self.parseAdd();
            if (!self.matchKeyword("AND")) return ParseError.InvalidExpression;
            const high = try self.parseAdd();
            return self.makeBetween(left, low, high, false);
        }

        if (self.matchKeyword("IS")) {
            const not_null = self.matchKeyword("NOT");
            if (!self.matchKeyword("NULL")) return ParseError.InvalidExpression;
            return self.makeIsNull(left, not_null);
        }
        if (self.matchKeyword("IN")) return self.parseInList(left, false);

        if (self.matchKind(.eq)) return self.makeBinary(.eq, left, try self.parseAdd());
        if (self.matchKind(.ne)) return self.makeBinary(.ne, left, try self.parseAdd());
        if (self.matchKind(.lt)) return self.makeBinary(.lt, left, try self.parseAdd());
        if (self.matchKind(.le)) return self.makeBinary(.le, left, try self.parseAdd());
        if (self.matchKind(.gt)) return self.makeBinary(.gt, left, try self.parseAdd());
        if (self.matchKind(.ge)) return self.makeBinary(.ge, left, try self.parseAdd());

        return left;
    }

    fn parseAdd(self: *Parser) ParseError!*types.Expr {
        var left = try self.parseMul();
        while (true) {
            if (self.matchKind(.plus)) {
                left = try self.makeBinary(.add, left, try self.parseMul());
                continue;
            }
            if (self.matchKind(.minus)) {
                left = try self.makeBinary(.sub, left, try self.parseMul());
                continue;
            }
            break;
        }
        return left;
    }

    fn parseMul(self: *Parser) ParseError!*types.Expr {
        var left = try self.parseUnary();
        while (true) {
            if (self.matchKind(.star)) {
                left = try self.makeBinary(.mul, left, try self.parseUnary());
                continue;
            }
            if (self.matchKind(.slash)) {
                left = try self.makeBinary(.div, left, try self.parseUnary());
                continue;
            }
            break;
        }
        return left;
    }

    fn parseUnary(self: *Parser) ParseError!*types.Expr {
        if (self.matchKind(.minus)) return self.makeUnary(.neg, try self.parseUnary());
        return self.parsePrimary();
    }

    fn parsePrimary(self: *Parser) ParseError!*types.Expr {
        if (self.matchKeyword("CASE")) return self.parseCaseExpr();

        if (self.matchKeyword("EXISTS")) {
            try self.consumeKind(.lparen);
            const sql_text = try self.captureSubqueryUntilRParen();
            const node = try self.allocator.create(types.Expr);
            node.* = .{ .exists_subquery = sql_text };
            return node;
        }

        if (self.matchKind(.lparen)) {
            const next_tok = self.peek() orelse return ParseError.InvalidExpression;
            if (next_tok.kind == .ident and eqlIgnoreCase(self.tokText(next_tok), "SELECT")) {
                const sql_text = try self.captureSubqueryUntilRParen();
                const node = try self.allocator.create(types.Expr);
                node.* = .{ .subquery = sql_text };
                return node;
            }
            const inner = try self.parseExpr();
            try self.consumeKind(.rparen);
            return inner;
        }

        if (self.peek()) |tok| switch (tok.kind) {
            .number => {
                self.idx += 1;
                const text = self.tokText(tok);
                const node = try self.allocator.create(types.Expr);
                if (std.mem.indexOfScalar(u8, text, '.')) |_| {
                    const f = std.fmt.parseFloat(f64, text) catch return ParseError.InvalidExpression;
                    node.* = .{ .literal = .{ .real = f } };
                } else {
                    const i = std.fmt.parseInt(i64, text, 10) catch return ParseError.InvalidExpression;
                    node.* = .{ .literal = .{ .integer = i } };
                }
                return node;
            },
            .string => {
                self.idx += 1;
                const text = self.tokText(tok);
                if (text.len < 2) return ParseError.InvalidExpression;
                const node = try self.allocator.create(types.Expr);
                node.* = .{ .literal = .{ .text = try self.allocator.dupe(u8, text[1 .. text.len - 1]) } };
                return node;
            },
            .ident => {
                self.idx += 1;
                const ident = self.tokText(tok);
                if (eqlIgnoreCase(ident, "x") or eqlIgnoreCase(ident, "X")) {
                    if (self.peek()) |next_tok| {
                        if (next_tok.kind == .string) {
                            self.idx += 1;
                            const text = self.tokText(next_tok);
                            if (text.len < 2) return ParseError.InvalidExpression;
                            const hex_text = text[1 .. text.len - 1];
                            if (hex_text.len % 2 != 0) return ParseError.InvalidExpression;
                            const blob = try self.allocator.alloc(u8, hex_text.len / 2);
                            const decoded = std.fmt.hexToBytes(blob, hex_text) catch return ParseError.InvalidExpression;
                            if (decoded.len != blob.len) return ParseError.InvalidExpression;
                            const node = try self.allocator.create(types.Expr);
                            node.* = .{ .literal = .{ .blob = blob } };
                            return node;
                        }
                    }
                }
                if (eqlIgnoreCase(ident, "NULL")) {
                    const node = try self.allocator.create(types.Expr);
                    node.* = .{ .literal = .null };
                    return node;
                }

                if (self.matchKind(.lparen)) {
                    return self.parseCall(ident);
                }

                if (self.matchKind(.dot)) {
                    const name_tok = self.expectKind(.ident) catch return ParseError.InvalidExpression;
                    const node = try self.allocator.create(types.Expr);
                    node.* = .{ .ident = .{
                        .qualifier = try self.allocator.dupe(u8, ident),
                        .name = try self.allocator.dupe(u8, self.tokText(name_tok)),
                    } };
                    return node;
                }

                const node = try self.allocator.create(types.Expr);
                node.* = .{ .ident = .{
                    .qualifier = null,
                    .name = try self.allocator.dupe(u8, ident),
                } };
                return node;
            },
            else => {},
        };

        return ParseError.InvalidExpression;
    }

    fn parseCall(self: *Parser, name: []const u8) ParseError!*types.Expr {
        var args = std.ArrayList(*types.Expr).empty;
        defer args.deinit(self.allocator);
        var star_arg = false;
        var distinct = false;

        if (self.matchKeyword("DISTINCT")) {
            distinct = true;
        }

        if (self.matchKind(.star)) {
            if (distinct) return ParseError.InvalidExpression;
            star_arg = true;
            try self.consumeKind(.rparen);
        } else if (self.matchKind(.rparen)) {} else {
            while (true) {
                try args.append(self.allocator, try self.parseExpr());
                if (self.matchKind(.comma)) continue;
                try self.consumeKind(.rparen);
                break;
            }
        }

        const node = try self.allocator.create(types.Expr);
        node.* = .{ .call = .{
            .name = try self.allocator.dupe(u8, name),
            .args = try args.toOwnedSlice(self.allocator),
            .star_arg = star_arg,
            .distinct = distinct,
        } };
        return node;
    }

    fn parseCaseExpr(self: *Parser) ParseError!*types.Expr {
        var base: ?*types.Expr = null;
        if (!self.nextIsKeyword("WHEN")) base = try self.parseExpr();

        var whens = std.ArrayList(types.CaseWhen).empty;
        defer whens.deinit(self.allocator);

        while (self.matchKeyword("WHEN")) {
            const cond = try self.parseExpr();
            if (!self.matchKeyword("THEN")) return ParseError.InvalidExpression;
            const value = try self.parseExpr();
            try whens.append(self.allocator, .{ .cond = cond, .value = value });
        }

        var else_expr: ?*types.Expr = null;
        if (self.matchKeyword("ELSE")) else_expr = try self.parseExpr();
        if (!self.matchKeyword("END")) return ParseError.InvalidExpression;

        const node = try self.allocator.create(types.Expr);
        node.* = .{ .case_expr = .{
            .base = base,
            .whens = try whens.toOwnedSlice(self.allocator),
            .else_expr = else_expr,
        } };
        return node;
    }

    fn nextIsKeyword(self: *Parser, word: []const u8) bool {
        const tok = self.peek() orelse return false;
        if (tok.kind != .ident) return false;
        return eqlIgnoreCase(self.tokText(tok), word);
    }

    fn captureSubqueryUntilRParen(self: *Parser) ParseError![]const u8 {
        const start_tok = self.peek() orelse return ParseError.InvalidExpression;
        var depth: usize = 1;
        var i = self.idx;
        while (i < self.tokens.len) : (i += 1) {
            const tok = self.tokens[i];
            if (tok.kind == .lparen) depth += 1;
            if (tok.kind == .rparen) {
                if (depth == 0) return ParseError.InvalidExpression;
                depth -= 1;
                if (depth == 0) {
                    const subquery = std.mem.trim(u8, self.source[start_tok.start..tok.start], " \t\r\n");
                    self.idx = i + 1;
                    return self.allocator.dupe(u8, subquery) catch ParseError.OutOfMemory;
                }
            }
        }
        return ParseError.InvalidExpression;
    }

    fn makeUnary(self: *Parser, op: types.UnaryOp, expr_node: *types.Expr) ParseError!*types.Expr {
        const node = try self.allocator.create(types.Expr);
        node.* = .{ .unary = .{ .op = op, .expr = expr_node } };
        return node;
    }

    fn makeBinary(self: *Parser, op: types.BinaryOp, left: *types.Expr, right: *types.Expr) ParseError!*types.Expr {
        const node = try self.allocator.create(types.Expr);
        node.* = .{ .binary = .{ .op = op, .left = left, .right = right } };
        return node;
    }

    fn makeBetween(self: *Parser, target: *types.Expr, low: *types.Expr, high: *types.Expr, not_between: bool) ParseError!*types.Expr {
        const node = try self.allocator.create(types.Expr);
        node.* = .{ .between = .{
            .target = target,
            .low = low,
            .high = high,
            .not_between = not_between,
        } };
        return node;
    }

    fn makeIsNull(self: *Parser, target: *types.Expr, not_null: bool) ParseError!*types.Expr {
        const node = try self.allocator.create(types.Expr);
        node.* = .{ .is_null = .{
            .target = target,
            .not_null = not_null,
        } };
        return node;
    }

    fn parseInList(self: *Parser, target: *types.Expr, not_in: bool) ParseError!*types.Expr {
        var subquery: ?[]const u8 = null;
        if (!self.matchKind(.lparen)) {
            const tok = self.peek() orelse return ParseError.InvalidExpression;
            if (tok.kind != .ident) return ParseError.InvalidExpression;
            self.idx += 1;
            subquery = try std.fmt.allocPrint(self.allocator, "SELECT * FROM {s}", .{self.tokText(tok)});
            const node = try self.allocator.create(types.Expr);
            node.* = .{ .in_list = .{
                .target = target,
                .items = try self.allocator.alloc(*types.Expr, 0),
                .subquery = subquery,
                .not_in = not_in,
            } };
            return node;
        }

        var items = std.ArrayList(*types.Expr).empty;
        defer items.deinit(self.allocator);

        if (self.peek()) |tok| {
            if (tok.kind == .ident and eqlIgnoreCase(self.tokText(tok), "SELECT")) {
                subquery = try self.captureSubqueryUntilRParen();
            } else if (!self.matchKind(.rparen)) {
                const first = try self.parseOr();
                try items.append(self.allocator, first);
                while (self.matchKind(.comma)) {
                    const item = try self.parseOr();
                    try items.append(self.allocator, item);
                }

                if (!self.matchKind(.rparen)) return ParseError.InvalidExpression;
            }
        } else {
            return ParseError.InvalidExpression;
        }

        const node = try self.allocator.create(types.Expr);
        node.* = .{ .in_list = .{
            .target = target,
            .items = try items.toOwnedSlice(self.allocator),
            .subquery = subquery,
            .not_in = not_in,
        } };
        return node;
    }
};

pub fn parse(allocator: std.mem.Allocator, source: []const u8) ParseError!*types.Expr {
    const trimmed = std.mem.trim(u8, source, " \t\r\n");
    const tokens = tokenizer.tokenize(allocator, trimmed) catch return ParseError.InvalidExpression;
    defer allocator.free(tokens);
    var p = Parser{
        .allocator = allocator,
        .source = trimmed,
        .tokens = tokens,
        .idx = 0,
    };
    const node = try p.parseExpr();
    if (p.idx != p.tokens.len) return ParseError.InvalidExpression;
    return node;
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (std.ascii.toLower(ca) != std.ascii.toLower(cb)) return false;
    }
    return true;
}
