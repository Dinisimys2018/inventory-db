const std = @import("std");
const assert = std.debug.assert;

pub const OrderItemRow = struct {
    id: u8,
    order_id: u32,
    // product_id: u32,
    // quantity: u32, //100_00 = 100.01
    // price: u32, //100_00 = 100.01
    // total: u32, //100_00 = 100.01
};

comptime {
    // assert(@sizeOf(OrderItemRow) == 21);
}

test "d" {
    const alloc = std.testing.allocator;
    
    var multi_arr: std.MultiArrayList(OrderItemRow) = try .initCapacity(alloc, 100);
    defer multi_arr.deinit(alloc);
    
    multi_arr.appendAssumeCapacity(.{.id = 1, .order_id = 1});
    std.debug.print("{any}", .{multi_arr.get(0)});
}
