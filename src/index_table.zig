const std = @import("std");
const ArrayList = std.array_list;
const assert = std.debug.assert;
const testing = std.testing;

const printObj = @import("utils/debug.zig").printObj;

pub const EntityFieldIndex = struct {
    field_name: []const u8,
};

const EntityFieldIndexListOptions = struct {
    indexes_u32_count: u4 = 0,
};

pub fn EntityFieldIndexListType(comptime options: EntityFieldIndexListOptions) type {
    return struct {
        indexes_u32: [options.indexes_u32_count]EntityFieldIndex,
    };
}

const IndexesMap = std.StringArrayHashMapUnmanaged(usize);

pub fn IndexTableType(
    comptime Key: type,
    comptime TablePtr: type,
    comptime ValuePtr: type,
) type {
    return struct {
        const IndexTable = @This();

        const LookupValue = struct {
            table_ptr: TablePtr,
            value_ptr: ValuePtr,
        };

        const Entries = std.AutoHashMapUnmanaged(Key, ValuePtr);

        // Struct Fields
        table_ptr: TablePtr = undefined,
        entries: Entries = .empty,

        fn generateError() !void {
            return error.HandError;
        }

        pub fn init(allocator: std.mem.Allocator, table_ptr: TablePtr, entries_max_count: ValuePtr) !*IndexTable {
            var index_table = try allocator.create(IndexTable);
            errdefer allocator.destroy(index_table);

            index_table.table_ptr = table_ptr;

            index_table.entries = .empty;
            try index_table.entries.ensureTotalCapacity(allocator, entries_max_count);
            errdefer index_table.entries.deinit(allocator);

            return index_table;
        }

        pub fn deinit(index_table: *IndexTable, allocator: std.mem.Allocator) void {
            index_table.entries.deinit(allocator);
            allocator.destroy(index_table);
        }

        pub fn insert(index_table: *IndexTable, keys: []Key) void {
            var value_ptr: ValuePtr = 0;

            while (value_ptr < keys.len) : (value_ptr += 1) {
                index_table.entries.putAssumeCapacity(keys[value_ptr], value_ptr);
            }
        }

        pub fn find(index_table: *IndexTable, key: Key) ?LookupValue {
            const value_ptr_res = index_table.entries.get(key);
            if (value_ptr_res) |value_ptr| {
                return .{
                    .table_ptr = index_table.table_ptr,
                    .value_ptr = value_ptr,
                };
            }

            return null;
        }
    };
}

pub fn IndexPoolType(
    comptime TablePtr: type,
    comptime ValuePtr: type,
    comptime indexes_meta: anytype,
    comptime tables_max_count: TablePtr,
    comptime entries_max_count: ValuePtr,
) type {
    const IndexTableU32 = IndexTableType(u32, TablePtr, ValuePtr);
    const IndexListU32 = []*IndexTableU32;
    const IndexesMapList = []*IndexesMap;

    const indexes_u32_max_count = tables_max_count * indexes_meta.indexes_u32.len;
    const indexes_total_count = indexes_u32_max_count;

    return struct {
        const IndexPool = @This();

        //Struct fields
        indexes_map_list: IndexesMapList,
        indexes_u32: IndexListU32,

        pub fn init(allocator: std.mem.Allocator) !IndexPool {
            var indexes_map_list: IndexesMapList = try allocator.alloc(*IndexesMap, tables_max_count);
            errdefer allocator.free(indexes_map_list);

            var indexes_map_list_index: TablePtr = 0;

            errdefer {
                for (0..indexes_map_list_index) |table_ptr| {
                    indexes_map_list[table_ptr].deinit(allocator);
                    allocator.destroy(indexes_map_list[table_ptr]);
                }
            }

            while (indexes_map_list_index < tables_max_count) : (indexes_map_list_index += 1) {
                var indexes_map = try allocator.create(IndexesMap);
                errdefer allocator.destroy(indexes_map);

                indexes_map.* = .empty;
                try indexes_map.ensureTotalCapacity(allocator, indexes_total_count);
                indexes_map_list[indexes_map_list_index] = indexes_map;
            }

            var indexes_u32: IndexListU32 = try allocator.alloc(*IndexTableU32, indexes_u32_max_count);
            errdefer allocator.free(indexes_u32);

            var table_ptr: TablePtr = 0;
            var index_ptr_indexes_u32: u8 = 0;

            errdefer {
                for (0..index_ptr_indexes_u32) |index_ptr| indexes_u32[index_ptr].deinit(allocator);
            }

            while (table_ptr < tables_max_count) : (table_ptr += 1) {
                var index_ptr: u8 = 0;
                while (index_ptr < indexes_meta.indexes_u32.len) : (index_ptr += 1) {
                    indexes_u32[index_ptr_indexes_u32] = try .init(allocator, table_ptr, entries_max_count);
                    indexes_map_list[table_ptr].putAssumeCapacity(indexes_meta.indexes_u32[index_ptr].field_name, @intFromPtr(indexes_u32[index_ptr_indexes_u32]));
                    index_ptr_indexes_u32 += 1;
                }
            }

            return .{
                .indexes_map_list = indexes_map_list,
                .indexes_u32 = indexes_u32,
            };
        }

        pub fn deinit(index_pool: *IndexPool, allocator: std.mem.Allocator) void {
            for (index_pool.indexes_u32) |index| {
                index.deinit(allocator);
            }
            allocator.free(index_pool.indexes_u32);

            for (index_pool.indexes_map_list) |indexes_map| {
                indexes_map.deinit(allocator);
                allocator.destroy(indexes_map);
            }

            allocator.free(index_pool.indexes_map_list);
        }

        pub fn insert(index_pool: *IndexPool, table_ptr: TablePtr, field_name: []const u8, field_values: []u32) void {
            const index_ptr = index_pool.getIndexPtr(table_ptr, field_name);
            assert(index_ptr > 0);
            const index: *IndexTableU32 = @ptrFromInt(index_ptr);

            index.insert(field_values);
        }

        pub fn getIndexPtr(index_pool: *IndexPool, table_ptr: TablePtr, field_name: []const u8) usize {
            // TODO: first check field_name in field_list for early return
            return index_pool.indexes_map_list[table_ptr].get(field_name) orelse 0;
        }

        pub fn find(index_pool: *IndexPool, table_ptr: TablePtr, field_name: []const u8, field_value: anytype) ?IndexTableU32.LookupValue {
            const index_ptr = index_pool.getIndexPtr(table_ptr, field_name);
            assert(index_ptr > 0);
            const index: *IndexTableU32 = @ptrFromInt(index_ptr);
            return index.find(field_value);
        }
    };
}

// ==== Testing ====
const indexes_u32_count_test_entity = 2;

const TestEntity = struct {
    pub const IndexesMeta = EntityFieldIndexListType(.{
        .indexes_u32_count = indexes_u32_count_test_entity,
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

test "IndexPool" {
    const allocator = std.testing.allocator;
    const tables_max_count = 5;
    const entries_max_count = 5;

    const TestTablePtr = u32;
    const TestValuePtr = u32;

    const IndexPool = IndexPoolType(
        TestTablePtr,
        TestValuePtr,
        TestEntity.indexes_meta,
        tables_max_count,
        entries_max_count,
    );
    var index_pool: IndexPool = try .init(allocator);
    defer index_pool.deinit(allocator);

    try testing.expect(index_pool.indexes_u32.len == tables_max_count * indexes_u32_count_test_entity);

    var table_ptr: TestTablePtr = 0;

    var input = [_]u32{ 100, 200, 300, 400 };

    while (table_ptr < tables_max_count) : (table_ptr += 1) {
        try testing.expect(index_pool.getIndexPtr(table_ptr, "order_id") != 0);
        try testing.expect(index_pool.getIndexPtr(table_ptr, "product_id") != 0);

        index_pool.insert(table_ptr, "order_id", input[0..]);
        var value_ptr: TestValuePtr = 0;
        while (value_ptr < input.len) : (value_ptr += 1) {
            const find_lookup_value = index_pool.find(table_ptr, "order_id", input[value_ptr]) orelse return error.TestError;
            try testing.expect(find_lookup_value.table_ptr == table_ptr);
            try testing.expect(find_lookup_value.value_ptr == value_ptr);
        }
    }
}
