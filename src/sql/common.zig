const std = @import("std");

pub fn startsWithIgnoreCase(haystack: []const u8, prefix: []const u8) bool {
    if (haystack.len < prefix.len) return false;
    return eqlIgnoreCase(haystack[0..prefix.len], prefix);
}

pub fn endsWithIgnoreCase(haystack: []const u8, suffix: []const u8) bool {
    if (haystack.len < suffix.len) return false;
    return eqlIgnoreCase(haystack[haystack.len - suffix.len ..], suffix);
}

pub fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (std.ascii.toLower(ca) != std.ascii.toLower(cb)) return false;
    }
    return true;
}

pub fn indexOfIgnoreCase(haystack: []const u8, needle: []const u8) ?usize {
    if (needle.len == 0 or haystack.len < needle.len) return null;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return i;
    }
    return null;
}

pub fn splitTopLevelComma(allocator: std.mem.Allocator, text: []const u8) ![]const []const u8 {
    var out = std.ArrayList([]const u8).empty;
    defer out.deinit(allocator);

    var depth: usize = 0;
    var in_string = false;
    var start: usize = 0;
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
            if (depth == 0) return error.InvalidSql;
            depth -= 1;
            continue;
        }
        if (c == ',' and depth == 0) {
            const part = std.mem.trim(u8, text[start..i], " \t\r\n");
            if (part.len == 0) return error.InvalidSql;
            try out.append(allocator, try allocator.dupe(u8, part));
            start = i + 1;
        }
    }

    if (in_string or depth != 0) return error.InvalidSql;

    const last = std.mem.trim(u8, text[start..], " \t\r\n");
    if (last.len == 0) return error.InvalidSql;
    try out.append(allocator, try allocator.dupe(u8, last));
    return out.toOwnedSlice(allocator);
}

pub fn findTopLevelKeyword(sql_text: []const u8, keyword: []const u8) ?usize {
    var depth: usize = 0;
    var in_string = false;
    var i: usize = 0;
    while (i + keyword.len <= sql_text.len) : (i += 1) {
        const c = sql_text[i];
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
        if (depth == 0 and eqlIgnoreCase(sql_text[i .. i + keyword.len], keyword)) return i;
    }
    return null;
}
