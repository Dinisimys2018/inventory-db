const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

pub const Coder = struct {
    pub fn encode(ptr: anytype) []u8 {
        return std.mem.asBytes(ptr);
    }
};