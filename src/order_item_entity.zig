const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const printObj = @import("utils/debug.zig").printObj;

const index_table = @import("index_table.zig");
const lookup = @import("lookup.zig");
const mem_table = @import("mem_table.zig");

pub const OrderItem = struct {
    pub const module_name = "order_items";

    pub const TimeLabel = u64;
    pub const OrderId = u32;
    pub const ProductId = u32;
    pub const Quantity = u32;

    // FIELDS
    time_label: TimeLabel,
    batch_offset: mem_table.BatchOffset,
    order_id: OrderId,
    product_id: ProductId,
    quantity: Quantity, //100_00 = 100.01

    pub const Entities = std.MultiArrayList(OrderItem);

    pub const SortCtx = struct {
        entities: *Entities,
        pub fn lessThan(ctx: @This(), a_index: usize, b_index: usize) bool {
            const a = ctx.entities.get(a_index);
            const b = ctx.entities.get(b_index);

            if (a.order_id != b.order_id) return a.order_id > b.order_id;

            if (a.product_id != b.product_id) return a.product_id > b.product_id;

            if (a.time_label == b.time_label) return a.batch_offset > b.batch_offset;

            return a.time_label > b.time_label;
        }
    };

    pub const Field = enum {
        order_id,
        product_id,
        time_label,
        quantity,
    };

    pub const FieldMeta = struct {
        tag: Entities.Field,
        size: u16,
        name: []const u8,
    };

    pub const MapMetaFields = std.EnumMap(Field, FieldMeta);

    pub const map_fields_meta: MapMetaFields = .init(.{
        .order_id = .{
            .tag = std.meta.stringToEnum(Entities.Field, "order_id") orelse unreachable,
            .size = @sizeOf(OrderId),
            .name = "order_id",
        },
        .product_id = .{
            .tag = std.meta.stringToEnum(Entities.Field, "product_id") orelse unreachable,
            .size = @sizeOf(ProductId),
            .name = "product_id",
        },
        .time_label = .{
            .tag = std.meta.stringToEnum(Entities.Field, "time_label") orelse unreachable,
            .size = @sizeOf(TimeLabel),
            .name = "time_label",
        },
        .quantity = .{
            .tag = std.meta.stringToEnum(Entities.Field, "quantity") orelse unreachable,
            .size = @sizeOf(Quantity),
            .name = "quantity",
        },
    });

    pub const values_map_fields_meta: [4]FieldMeta = map_fields_meta.values();

    pub const IndexTable = index_table.IndexTableWithTwoKeysType(
        OrderItem,
        "order_id",
        "product_id",
    );

    pub const Lookup = lookup.LookupWithTwoKeysType;

    pub fn init(allocator: Allocator) !*OrderItem {
        const order_item = try allocator.create(OrderItem);

        return order_item;
    }

    pub fn deinit(order_item: *OrderItem, allocator: Allocator) void {
        allocator.destroy(order_item);
    }
};

test "OrderItemRow" {
    printObj("sizeOf OrderItem (bytes)", @sizeOf(OrderItem));
}
