const std = @import("std");
const assert = std.debug.assert;

const EntityFieldIndexListType = @import("index_table.zig").EntityFieldIndexListType;
const EntityFieldIndex = @import("index_table.zig").EntityFieldIndex;

pub const OrderItemRow = struct {
    // time_label: u64,
    order_id: u32,
    product_id: u32,
    // quantity: u32, //100_00 = 100.01
    // price: u32, //100_00 = 100.01
    // total: u32, //100_00 = 100.01

    pub const IndexesMeta = EntityFieldIndexListType(2);

    pub const indexes_meta: IndexesMeta = [2]EntityFieldIndex{
            .{
                .field_name = "order_id",
                .index_strategy = .indexes_u32,
            },
            .{
                .field_name = "product_id",
                .index_strategy = .indexes_u32,
            },
    };
};

comptime {
    // assert(@sizeOf(OrderItemRow) == 21);
}

test "OrderItemRow" {
    std.debug.print("--{d}--", .{@sizeOf(OrderItemRow)});
}
