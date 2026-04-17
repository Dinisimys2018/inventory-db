//TODO: - change all errors to Error Enum Type

const std = @import("std");
const ArrayList = std.array_list;
const assert = std.debug.assert;
const testing = std.testing;

const printObj = @import("utils/debug.zig").printObj;

const OrderId = u32;
const TablePrimaryKey = u96;
const EntityPtr = u32;

const LookupValue = std.AutoArrayHashMapUnmanaged(TablePrimaryKey, std.ArrayList(EntityPtr));
const IndexValue = struct {};

pub fn OrderIdIndexBlockType(init_entries_max_count_per_index: EntityPtr) type {
    _ = init_entries_max_count_per_index;

    return struct {
        const OrderIdIndexBlock = @This();

        map: std.AutoHashMapUnmanaged(OrderId, LookupValue),
        has_keys: bool,
        min_key: OrderId,
        max_key: OrderId,

        pub fn init(allocator: std.mem.Allocator) !*OrderIdIndexBlock {
            var index_block = try allocator.create(OrderIdIndexBlock);
            index_block.map = .empty;
            // try index_block.map.ensureTotalCapacity(allocator, init_entries_max_count_per_index);
            index_block.has_keys = false;
            index_block.max_key = 0;
            index_block.min_key = 0;

            return index_block;
        }

        pub fn deinit(index_block: *OrderIdIndexBlock, allocator: std.mem.Allocator) void {
            var map_iterator = index_block.map.iterator();

            while (map_iterator.next()) |table_map| {
                var lookup_iterator = table_map.value_ptr.iterator();
                while (lookup_iterator.next()) |entry| {
                    entry.value_ptr.deinit(allocator);
                }
                table_map.value_ptr.deinit(allocator);
            }

            index_block.map.deinit(allocator);
            allocator.destroy(index_block);
        }

        pub fn insert(
            index_block: *OrderIdIndexBlock,
            allocator: std.mem.Allocator,
            table_primary_key: TablePrimaryKey,
            keys: []OrderId,
        ) !void {
            var entity_ptr: EntityPtr = 0;

            if (!index_block.has_keys) {
                index_block.min_key = keys[0];
                index_block.max_key = keys[0];
            }

            while (entity_ptr < keys.len) : (entity_ptr += 1) {
                if (keys[entity_ptr] < index_block.min_key) {
                    index_block.min_key = keys[entity_ptr];
                } else if (keys[entity_ptr] > index_block.max_key) {
                    index_block.max_key = keys[entity_ptr];
                }

                const table_map = try index_block.map.getOrPut(allocator, keys[entity_ptr]);

                if (!table_map.found_existing) {
                    table_map.value_ptr.* = .empty;
                }

                const entity_list = try table_map.value_ptr.getOrPut(allocator, table_primary_key);

                if (!entity_list.found_existing) {
                    entity_list.value_ptr.* = .empty;
                }
                try entity_list.value_ptr.append(allocator, entity_ptr);
            }
        }

        pub fn find(index_block: *OrderIdIndexBlock, value: anytype) !LookupValue {
            return index_block.map.get(value) orelse error.NotFound;
        }
    };
}

test "IndexTable" {
    const allocator = std.testing.allocator;

    const init_entries_max_count_per_index = 5;

    const index_block: *OrderIdIndexBlockType(init_entries_max_count_per_index) = try .init(allocator);
    defer index_block.deinit(allocator);

    var entity_base_ids = [_]OrderId{ 100, 200, 300, 400, 500 };

    try index_block.insert(allocator, 1, &entity_base_ids);
    try index_block.insert(allocator, 2, &entity_base_ids);

    const find_res = try index_block.find(300);
    printObj("find_res", find_res.get(1));
}
