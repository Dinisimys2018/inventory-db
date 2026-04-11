const std = @import("std");


pub fn printObj(title: []const u8, obj: anytype) void {
    std.debug.print("\n==={s}===\n{any}\n=======\n", .{title, obj});
}