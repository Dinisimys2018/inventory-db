const std = @import("std");
const assert = std.debug.assert;

const printObj = @import("utils/debug.zig").printObj;

pub const OrderItem = struct {
    pub const OrderId = u32;
    pub const ProductId = u32;

    // FIELDS
    time_label: u64 = 0,
    order_id: OrderId,
    product_id: ProductId,
    quantity: u32 = 0, //100_00 = 100.01

    pub const Entities = std.MultiArrayList(OrderItem);

    pub const SortCtx = struct {
        entities: *Entities,
        pub fn lessThan(ctx: @This(), a_index: usize, b_index: usize) bool {
            const a = ctx.entities.get(a_index);
            const b = ctx.entities.get(b_index);

            if (a.order_id != b.order_id) return a.order_id > b.order_id;

            if (a.product_id != b.product_id) return a.product_id > b.product_id;

            return a.time_label > b.time_label;
        }
    };


    pub const Field = enum {
        order_id,
        product_id,
        time_label,
        quantity,
    };

    const FieldEntry = Entities.Field;

    pub const map_field_tags: std.EnumMap(Field,  Entities.Field) = .init(.{
        .order_id = std.meta.stringToEnum(FieldEntry, "order_id") orelse unreachable,
        .product_id = std.meta.stringToEnum(FieldEntry, "product_id") orelse unreachable,
        .time_label = std.meta.stringToEnum(FieldEntry, "time_label") orelse unreachable,
        .quantity = std.meta.stringToEnum(FieldEntry, "quantity") orelse unreachable,
    });
};

test "OrderItemRow" {
    printObj("sizeOf OrderItem (bytes)", @sizeOf(OrderItem));
}
