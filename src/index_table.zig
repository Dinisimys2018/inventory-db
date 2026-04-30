const std = @import("std");
const Allocator = std.mem.Allocator;


pub const Union = union(enum) {
    complex: IndexTableWithTwoKeysType
};

pub fn IndexTableWithTwoKeysType(
    comptime Entity: type,
    comptime _FirstKey: type,
    comptime _SecondKey: type,
    comptime first_key_name: []const u8,
    comptime second_key_name: []const u8,
) type {
    return struct {
        const IndexTable = @This();
        // re-export
        pub const FirstKey = _FirstKey;
        pub const SecondKey = _SecondKey;

        // FIELDS
        min_first_key: FirstKey,
        max_first_key: FirstKey,
        min_second_key: SecondKey,
        max_second_key: SecondKey,

        pub fn init(allocator: Allocator) !*IndexTable {
            const index_table = try allocator.create(IndexTable);

            return index_table;
        }

        pub fn deinit(index_table: *IndexTable, allocator: Allocator) void {
            allocator.destroy(index_table);
        }

        pub fn initAllKeys(
            index_table: *IndexTable,
            first_key_value: FirstKey,
            second_key_value: SecondKey,
        ) void {
            index_table.min_first_key = first_key_value;
            index_table.max_first_key = first_key_value;
            index_table.min_second_key = second_key_value;
            index_table.max_second_key = second_key_value;
        }

        pub fn maybeRewriteFirstKey(index_table: *IndexTable, key_value: FirstKey) void {
            if (key_value < index_table.min_first_key) {
                index_table.min_first_key = key_value;
            } else if (key_value > index_table.max_first_key) {
                index_table.max_first_key = key_value;
            }
        }

        pub fn maybeRewriteSecondKey(index_table: *IndexTable, key_value: SecondKey) void {
            if (key_value < index_table.min_second_key) {
                index_table.min_second_key = key_value;
            } else if (key_value > index_table.max_second_key) {
                index_table.max_second_key = key_value;
            }
        }

        pub fn clear(index_table: *IndexTable) void {
            index_table.min_first_key = undefined;
            index_table.max_first_key = undefined;
            index_table.min_second_key = undefined;
            index_table.max_second_key = undefined;
        }

        pub fn insert(index_table: *IndexTable, entity: *const Entity) void {
            index_table.maybeRewriteFirstKey(@field(entity, first_key_name));
            index_table.maybeRewriteSecondKey(@field(entity, second_key_name));
        }

        pub fn inFirstInterval(index_table: *IndexTable, key_value: FirstKey) bool {
            return key_value >= index_table.min_first_key and key_value <= index_table.max_first_key;
        }

         pub fn inSecondInterval(index_table: *IndexTable, key_value: SecondKey) bool {
            return key_value >= index_table.min_second_key and key_value <= index_table.max_second_key;
        }
    };
}
