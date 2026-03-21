const std = @import("std");

pub const TokenKind = enum {
    ident,
    number,
    string,
    lparen,
    rparen,
    comma,
    dot,
    plus,
    minus,
    star,
    slash,
    eq,
    ne,
    lt,
    le,
    gt,
    ge,
};

pub const Token = struct {
    kind: TokenKind,
    start: usize,
    end: usize,
};

pub const TokenizeError = error{ InvalidToken, OutOfMemory };

pub fn tokenize(allocator: std.mem.Allocator, source: []const u8) TokenizeError![]const Token {
    var out = std.ArrayList(Token).empty;
    defer out.deinit(allocator);

    var i: usize = 0;
    while (i < source.len) {
        const c = source[i];
        if (isSpace(c)) {
            i += 1;
            continue;
        }
        if (isIdentStart(c)) {
            const start = i;
            i += 1;
            while (i < source.len and isIdentContinue(source[i])) : (i += 1) {}
            try out.append(allocator, .{ .kind = .ident, .start = start, .end = i });
            continue;
        }
        if (isDigit(c)) {
            const start = i;
            i += 1;
            while (i < source.len and isDigit(source[i])) : (i += 1) {}
            if (i < source.len and source[i] == '.') {
                i += 1;
                while (i < source.len and isDigit(source[i])) : (i += 1) {}
            }
            try out.append(allocator, .{ .kind = .number, .start = start, .end = i });
            continue;
        }
        if (c == '\'') {
            const start = i;
            i += 1;
            while (i < source.len and source[i] != '\'') : (i += 1) {}
            if (i >= source.len) return TokenizeError.InvalidToken;
            i += 1;
            try out.append(allocator, .{ .kind = .string, .start = start, .end = i });
            continue;
        }

        if (c == '(') {
            try out.append(allocator, .{ .kind = .lparen, .start = i, .end = i + 1 });
            i += 1;
            continue;
        }
        if (c == ')') {
            try out.append(allocator, .{ .kind = .rparen, .start = i, .end = i + 1 });
            i += 1;
            continue;
        }
        if (c == ',') {
            try out.append(allocator, .{ .kind = .comma, .start = i, .end = i + 1 });
            i += 1;
            continue;
        }
        if (c == '.') {
            try out.append(allocator, .{ .kind = .dot, .start = i, .end = i + 1 });
            i += 1;
            continue;
        }
        if (c == '+') {
            try out.append(allocator, .{ .kind = .plus, .start = i, .end = i + 1 });
            i += 1;
            continue;
        }
        if (c == '-') {
            try out.append(allocator, .{ .kind = .minus, .start = i, .end = i + 1 });
            i += 1;
            continue;
        }
        if (c == '*') {
            try out.append(allocator, .{ .kind = .star, .start = i, .end = i + 1 });
            i += 1;
            continue;
        }
        if (c == '/') {
            try out.append(allocator, .{ .kind = .slash, .start = i, .end = i + 1 });
            i += 1;
            continue;
        }
        if (c == '=') {
            try out.append(allocator, .{ .kind = .eq, .start = i, .end = i + 1 });
            i += 1;
            continue;
        }
        if (c == '<') {
            if (i + 1 < source.len and source[i + 1] == '=') {
                try out.append(allocator, .{ .kind = .le, .start = i, .end = i + 2 });
                i += 2;
                continue;
            }
            if (i + 1 < source.len and source[i + 1] == '>') {
                try out.append(allocator, .{ .kind = .ne, .start = i, .end = i + 2 });
                i += 2;
                continue;
            }
            try out.append(allocator, .{ .kind = .lt, .start = i, .end = i + 1 });
            i += 1;
            continue;
        }
        if (c == '>') {
            if (i + 1 < source.len and source[i + 1] == '=') {
                try out.append(allocator, .{ .kind = .ge, .start = i, .end = i + 2 });
                i += 2;
                continue;
            }
            try out.append(allocator, .{ .kind = .gt, .start = i, .end = i + 1 });
            i += 1;
            continue;
        }
        if (c == '!') {
            if (i + 1 < source.len and source[i + 1] == '=') {
                try out.append(allocator, .{ .kind = .ne, .start = i, .end = i + 2 });
                i += 2;
                continue;
            }
            return TokenizeError.InvalidToken;
        }

        return TokenizeError.InvalidToken;
    }

    return out.toOwnedSlice(allocator);
}

fn isSpace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\r' or c == '\n';
}

fn isIdentStart(c: u8) bool {
    return std.ascii.isAlphabetic(c) or c == '_';
}

fn isIdentContinue(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

fn isDigit(c: u8) bool {
    return c >= '0' and c <= '9';
}
