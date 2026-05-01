const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const printObj = @import("utils/debug.zig").printObj;

pub fn IndexTableWithTwoKeysType(
    comptime Entity: type,
    comptime name_first_key: []const u8,
    comptime name_second_key: []const u8,
) type {
    return struct {
        const IndexTable = @This();
        // re-export
        pub const FirstKey = @FieldType(Entity, name_first_key);
        pub const SecondKey = @FieldType(Entity, name_second_key);

        // FIELDS
        min_first_key: FirstKey,
        max_first_key: FirstKey,
        min_second_key: SecondKey,
        max_second_key: SecondKey,

        pub fn init(allocator: Allocator) !*IndexTable {
            const index_table = try allocator.create(IndexTable);
            index_table.clear();

            return index_table;
        }

        pub fn deinit(index_table: *IndexTable, allocator: Allocator) void {
            allocator.destroy(index_table);
        }

        pub fn rewriteMin(index_table: *IndexTable, entity: *const Entity) void {
            const first_new_value = @field(entity, name_first_key);
            const second_new_value = @field(entity, name_second_key);
            
            assert(index_table.min_first_key == 0 or index_table.min_first_key >= first_new_value);
            assert(index_table.min_second_key == 0 or index_table.min_second_key >= second_new_value);

            index_table.min_first_key = first_new_value;
            index_table.min_second_key = second_new_value;
        }

        pub fn rewriteMax(index_table: *IndexTable, entity: *const Entity) void {
            const first_new_value = @field(entity, name_first_key);
            const second_new_value = @field(entity, name_second_key);

            assert(index_table.max_first_key == 0 or index_table.max_first_key <= first_new_value);
            assert(index_table.max_second_key == 0 or index_table.max_second_key <= second_new_value);

            index_table.max_first_key = first_new_value;
            index_table.max_second_key = second_new_value;
        }


        pub fn clear(index_table: *IndexTable) void {
            index_table.min_first_key = 0;
            index_table.max_first_key = 0;
            index_table.min_second_key = 0;
            index_table.max_second_key = 0;
        }

        pub fn inFirstInterval(index_table: *IndexTable, key_value: FirstKey) bool {
            return key_value >= index_table.min_first_key and key_value <= index_table.max_first_key;
        }

         pub fn inSecondInterval(index_table: *IndexTable, key_value: SecondKey) bool {
            return key_value >= index_table.min_second_key and key_value <= index_table.max_second_key;
        }
    };
}
