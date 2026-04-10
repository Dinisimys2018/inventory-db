const std = @import("std");
const assert = std.debug.assert;

pub const Product = struct {
    id: u128,
};

pub const Attribute = struct {};

pub const sort = @import("sort.zig");
pub const rb_tree = @import("rb_tree.zig");
