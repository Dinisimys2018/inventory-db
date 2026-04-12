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

    const indexes_u32_max_count = tables_max_count * indexes_meta.indexes_u32.len;

    return struct {
        const IndexPool = @This();

        //Struct fields
        indexes_u32: IndexListU32,

        pub fn init(allocator: std.mem.Allocator) !IndexPool {
            var indexes_u32: IndexListU32 = try allocator.alloc(*IndexTableU32, indexes_u32_max_count);
            errdefer allocator.free(indexes_u32);

            var table_ptr: TablePtr = 0;
            var index_ptr_indexes_u32: u8 = 0;

            errdefer {
                for(0..index_ptr_indexes_u32) |index_ptr| indexes_u32[index_ptr].deinit(allocator);
            }

            while (table_ptr < tables_max_count) : (table_ptr += 1) {
                var index_ptr: u8 = 0;
                while (index_ptr < indexes_meta.indexes_u32.len) : (index_ptr += 1) {
                    indexes_u32[index_ptr_indexes_u32] = try .init(allocator, table_ptr, entries_max_count);
                    index_ptr_indexes_u32 += 1;
                }
            }

            return .{
                .indexes_u32 = indexes_u32,
            };
        }

        pub fn deinit(index_pool: *IndexPool, allocator: std.mem.Allocator) void {
            for (index_pool.indexes_u32) |index| index.deinit(allocator);
            allocator.free(index_pool.indexes_u32);
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

    const IndexPool = IndexPoolType(
        u32,
        u32,
        TestEntity.indexes_meta,
        tables_max_count,
        entries_max_count,
    );
    var index_pool: IndexPool = try .init(allocator);
    defer index_pool.deinit(allocator);

    try testing.expect(index_pool.indexes_u32.len == tables_max_count * indexes_u32_count_test_entity);
}
