const std = @import("std");
const assert = std.debug.assert;

const EntityFieldIndex = @import("index_mem_table.zig").EntityFieldIndex;
const IndexTableStrategy = @import("index_mem_table.zig").IndexTableStrategy;
const EntityFieldIndexListType = @import("index_mem_table.zig").EntityFieldIndexListType;


pub const OrderItemRow = struct {
    // time_label: u64,
    order_id: u32,
    product_id: u32,
    // quantity: u32, //100_00 = 100.01
    // price: u32, //100_00 = 100.01
    // total: u32, //100_00 = 100.01

    pub const IndexesMeta = EntityFieldIndexListType(1);

    pub const indexes_meta: IndexesMeta = .{
        .{
            .field_name = "order_id",
            .index_strategy = .{.index_u32 = .{.ff = 56} },
        },
    };
};


comptime {
    // assert(@sizeOf(OrderItemRow) == 21);
}

test "b" {
    std.debug.print("--{d}--", .{@sizeOf(OrderItemRow)});
}
