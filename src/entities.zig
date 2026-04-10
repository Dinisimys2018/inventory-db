const std = @import("std");
const assert = std.debug.assert;

pub const OrderItemRow = struct {
    // time_label: u64,
    order_id: u32,
    product_id: u32,
    // quantity: u32, //100_00 = 100.01
    // price: u32, //100_00 = 100.01
    // total: u32, //100_00 = 100.01
};

comptime {
    // assert(@sizeOf(OrderItemRow) == 21);
}

test "b" {
    std.debug.print("--{d}--", .{@sizeOf(OrderItemRow)});
}
