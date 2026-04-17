//TODO: - change all errors to Error Enum Type

const std = @import("std");
const ArrayList = std.array_list;
const assert = std.debug.assert;
const testing = std.testing;

const printObj = @import("utils/debug.zig").printObj;

const OrderId = u32;
const EntityPtr = u32;
const TablePtr = u32;

const LookupValue = std.ArrayList(EntityPtr);

const IndexValue = struct {
    key: OrderId,
    entity_ptr: EntityPtr,

    pub fn compareKeys(target_key: comptime_int, index_value: *IndexValue) std.math.Order {
        return std.math.order(target_key, index_value.key);
    }

    pub fn lessThan(_: @TypeOf(.{}), a: *IndexValue, b: *IndexValue) bool {
        return a.key < b.key;
    }
};

pub fn OrderIdIndexTableType(comptime keys_max_count: EntityPtr) type {
    return struct {
        const OrderIdIndexTable = @This();

        table_ptr: TablePtr,
        entries: []*IndexValue,
        current_entry_index: TablePtr,
        has_keys: bool,
        min_key: OrderId,
        max_key: OrderId,

        pub fn init(allocator: std.mem.Allocator, table_ptr: TablePtr) !*OrderIdIndexTable {
            var index_block = try allocator.create(OrderIdIndexTable);
            index_block.table_ptr = table_ptr;
            index_block.has_keys = false;
            index_block.current_entry_index = 0;
            index_block.max_key = 0;
            index_block.min_key = 0;

            index_block.entries = try allocator.alloc(*IndexValue, keys_max_count);
            var entry_idx: EntityPtr = 0;
            while (entry_idx < keys_max_count) : (entry_idx += 1) {
                index_block.entries[entry_idx] = try allocator.create(IndexValue);
            }

            return index_block;
        }

        pub fn deinit(index_block: *OrderIdIndexTable, allocator: std.mem.Allocator) void {
            for (index_block.entries) |entry| allocator.destroy(entry);
            allocator.free(index_block.entries);
            allocator.destroy(index_block);
        }

        pub fn insert(
            index_block: *OrderIdIndexTable,
            last_entry_ptr_table: EntityPtr,
            keys: []OrderId,
        ) void {
            if (!index_block.has_keys) {
                index_block.min_key = keys[0];
                index_block.max_key = keys[0];
            }

            for (keys) |key| {
                if (key < index_block.min_key) {
                    index_block.min_key = key;
                } else if (key > index_block.max_key) {
                    index_block.max_key = key;
                }

                index_block.entries[index_block.current_entry_index].key = key;
                index_block.entries[index_block.current_entry_index].entity_ptr = last_entry_ptr_table + 1;

                index_block.current_entry_index += 1;
            }

            std.sort.block(*IndexValue, index_block.entries, .{}, IndexValue.lessThan);

            index_block.has_keys = true;
        }

        pub fn lookup(index_block: *OrderIdIndexTable, key: anytype) struct { usize, usize } {
            return std.sort.equalRange(*IndexValue, index_block.entries, key, IndexValue.compareKeys);
        }
    };
}

test "IndexTable" {
    const allocator = std.testing.allocator;

    const keys_max_count = 10;
    const table_ptr: TablePtr = 0;

    const index_block: *OrderIdIndexTableType(keys_max_count) = try .init(allocator, table_ptr);
    defer index_block.deinit(allocator);

    var entity_base_ids = [_]OrderId{ 100, 200, 300, 400, 500 };
    var last_entry_ptr_table: EntityPtr = 0;

    index_block.insert(last_entry_ptr_table, &entity_base_ids);
    last_entry_ptr_table = entity_base_ids.len;

    index_block.insert(last_entry_ptr_table, &entity_base_ids);

    printObj("entries", index_block.entries);

    const find_res = index_block.lookup(100);
    printObj("find_res", find_res);
}
