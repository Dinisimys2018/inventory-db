const std = @import("std");
const assert = std.debug.assert;

const printObj = @import("utils/debug.zig").printObj;

pub const OrderItem = struct {
    pub const OrderId = u32;
    pub const ProductId = u32;

    time_label: u64 = 0,
    order_id: u32,
    product_id: u32,
    quantity: u32 = 0, //100_00 = 100.01
    // price: u32, //100_00 = 100.01
    // total: u32, //100_00 = 100.01
};

comptime {
    // assert(@sizeOf(OrderItemRow) == 21);
}

test "OrderItemRow" {
    printObj("sizeOf OrderItem (bytes)", @sizeOf(OrderItem));
}
