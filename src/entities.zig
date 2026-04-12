const std = @import("std");
const assert = std.debug.assert;

const EntityFieldIndexListType = @import("index_table.zig").EntityFieldIndexListType;

pub const OrderItemRow = struct {
    // time_label: u64,
    order_id: u32,
    product_id: u32,
    // quantity: u32, //100_00 = 100.01
    // price: u32, //100_00 = 100.01
    // total: u32, //100_00 = 100.01

    pub const IndexesMeta = EntityFieldIndexListType(.{
        .indexes_u32_count = 2,
    });

    pub const indexes_meta: IndexesMeta = .{
        .indexes_u32 = .{
            .{
                .field_name = "order_id",
            },
            .{
                .field_name = "product_id",
            },
        },
    };
};

comptime {
    // assert(@sizeOf(OrderItemRow) == 21);
}

test "OrderItemRow" {
    std.debug.print("--{d}--", .{@sizeOf(OrderItemRow)});
}
