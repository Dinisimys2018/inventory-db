const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Writer = std.Io.Writer;
const Error = Writer.FileError;
const Allocator = std.mem.Allocator;
const printObj = @import("utils/debug.zig").printObj;

const Entity = @import("order_item_entity.zig").OrderItem;
const index_table = @import("index_table.zig");

const IndexTable = index_table.IndexTableWithTwoKeysType(
    Entity,
    Entity.OrderId,
    Entity.ProductId,
    "order_id",
    "product_id",
);

//TODO: P1 START duplicate code from mem_table
const EntitiesRange = struct { usize, usize };

pub const TableLookupResult = struct {
    table_ptr: usize,
    entities_range: EntitiesRange,
};

pub const LookupResult = std.ArrayList(TableLookupResult);
//TODO: P1 END duplicate code from mem_table

pub fn StorageTableType() type {
    return struct {
        const StorageTable = @This();

        // FIELDS
        pub fn init(allocator: Allocator) !*StorageTable {
            const storage_table = try allocator.create(StorageTable);
            return storage_table;
        }

        pub fn deinit(storage_table: *StorageTable, allocator: Allocator) void {
            allocator.destroy(storage_table);
        }

        pub fn lookupByFirstKey(storage_table: *StorageTable, key_value: IndexTable.FirstKey) !EntitiesRange {
            // const range = stdx_sort.equalRangeDesc(
            //     Entity.OrderId,
            //     mem_table.entities.slice().items(Entity.map_field_tags.get(.order_id)),
            //     key_value,
            //     stdx_sort.compareNumberKeys(Entity.OrderId),
            // );

            // if (range[1] == 0) return error.NotFound;

            // return range;
        }
    };
}

pub fn PoolStorageTablesType(comptime tables_max_count: usize) type {
    return struct {
        const StorageTable = StorageTableType();

        const PoolStorageTables = @This();

        // FIELDS
        tables: []*StorageTable,
        count_tables: usize,
        indexes: []*IndexTable,
        lookup_result: *LookupResult,

        pub fn init(allocator: Allocator) !*PoolStorageTables {
            const pool_storage_tables = try allocator.create(PoolStorageTables);
            pool_storage_tables.* = .{
                .tables = try allocator.alloc(*IndexTable, tables_max_count),
                .indexes = try allocator.alloc(*IndexTable, tables_max_count),
                .count_tables = 0,
                .lookup_result = try allocator.create(LookupResult),
            };

            for (0..tables_max_count) |table_ptr| {
                pool_storage_tables.tables[table_ptr] = try .init(allocator);
                pool_storage_tables.indexes[table_ptr] = try .init(allocator);
            }
            pool_storage_tables.lookup_result.* = try .initCapacity(allocator, tables_max_count);

            return pool_storage_tables;
        }

        pub fn deinit(pool_storage_tables: *PoolStorageTables, allocator: Allocator) void {
            pool_storage_tables.lookup_result.deinit(allocator);
            allocator.destroy(pool_storage_tables.lookup_result);

            for (pool_storage_tables.indexes) |index| {
                index.deinit(allocator);
            }
            for (pool_storage_tables.tables) |table| {
                table.deinit(allocator);
            }
            allocator.free(pool_storage_tables.indexes);
            allocator.free(pool_storage_tables.tables);

            allocator.destroy(pool_storage_tables);
        }

        pub fn appendTable(pool_storage_tables: *PoolStorageTables, index: *IndexTable) void {
            pool_storage_tables.indexes[pool_storage_tables.count_tables].* = index.*;
            pool_storage_tables.count_tables += 1;
        }

        pub fn lookupByFirstKey(pool_storage_tables: *PoolStorageTables, key_value: IndexTable.FirstKey) !*const LookupResult {
            assert(key_value != 0);

            //TODO: P3 need to check how we can clear result not before each lookup, but after this
            pool_storage_tables.lookup_result.clearRetainingCapacity();

            var last_indx = pool_storage_tables.count_tables;

            while (last_indx > 0) {
                last_indx -= 1;
                if (pool_storage_tables.indexes[last_indx].inFirstInterval(key_value)) {
                    const entities_range = pool_storage_tables.tables[last_indx].lookupByOrderId(key_value) catch continue;

                    pool_storage_tables.lookup_result.appendAssumeCapacity(.{
                        .table_ptr = last_indx,
                        .entities_range = entities_range,
                    });
                }
            }

            if (pool_storage_tables.lookup_result.items.len > 0) return pool_storage_tables.lookup_result;

            return error.NotFound;
        }
    };
}
