const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Io = std.Io;

const printObj = @import("utils/debug.zig").printObj;

const module = @import("module.zig");

const FieldMetaStorage = struct {
    size: u16,
};

pub fn HeadersStorageTableType(comptime config: *const module.ConfigModule) type {
    _ = config;
    return struct {
        const HeadersStorageTable = @This();
        // FIELDS
        index_size: usize,
        table_size: usize,
        fields_meta: std.StringArrayHashMapUnmanaged(FieldMetaStorage),

        pub fn init(allocator: Allocator) !*HeadersStorageTable {
            const headers = try allocator.create(HeadersStorageTable);
            return headers;
        }

        pub fn deinit(headers: *HeadersStorageTable, allocator: Allocator) void {
            allocator.destroy(headers);
        }
    };
}

pub fn StorageTableType(comptime config: *const module.ConfigModule) type {
    return struct {
        const StorageTable = @This();

        // FIELDS

        buffer_keys: []u8,

        pub fn init(allocator: Allocator) !*StorageTable {
            const storage_table = try allocator.create(StorageTable);
            storage_table.* = .{
                .buffer_keys = try allocator.alloc(u8, config.mem_tables_entities_max_count),
            };
            return storage_table;
        }

        pub fn deinit(storage_table: *StorageTable, allocator: Allocator) void {
            allocator.free(storage_table.buffer_keys);
            allocator.destroy(storage_table);
        }
    };
}

pub fn PoolStorageTablesType(
    comptime config: *const module.ConfigModule,
) type {
    const Components = config.Components();
    const Index = Components.IndexTable;
    const HeadersStorageTable = HeadersStorageTableType(config);

    return struct {
        const StorageTable = Components.StorageTable;

        const PoolStorageTables = @This();

        // FIELDS
        actual_count_tables: usize,
        headers: []*HeadersStorageTable,
        indexes: []*Index,
        tables: []*StorageTable,

        pub fn init(allocator: Allocator) !*PoolStorageTables {
            const pool_storage_tables = try allocator.create(PoolStorageTables);
            pool_storage_tables.* = .{
                .headers = try allocator.alloc(*HeadersStorageTable, config.level_0_tables_count),
                .indexes = try allocator.alloc(*Index, config.level_0_tables_count),
                .tables = try allocator.alloc(*StorageTable, config.level_0_tables_count),
                .actual_count_tables = 0,
            };

            for (0..config.level_0_tables_count) |table_ptr| {
                pool_storage_tables.headers[table_ptr] = try .init(allocator);
                pool_storage_tables.indexes[table_ptr] = try .init(allocator);
                pool_storage_tables.tables[table_ptr] = try .init(allocator);
            }

            return pool_storage_tables;
        }

        pub fn deinit(pool_storage_tables: *PoolStorageTables, allocator: Allocator) void {
            for (pool_storage_tables.headers) |headers| {
                headers.deinit(allocator);
            }

            allocator.free(pool_storage_tables.headers);

            for (pool_storage_tables.indexes) |index| {
                index.deinit(allocator);
            }
            allocator.free(pool_storage_tables.indexes);

            for (pool_storage_tables.tables) |table| {
                table.deinit(allocator);
            }

            allocator.free(pool_storage_tables.tables);

            allocator.destroy(pool_storage_tables);
        }

        pub fn appendTable(pool_storage_tables: *PoolStorageTables, index: *Index) void {
            pool_storage_tables.indexes[pool_storage_tables.actual_count_tables].* = index.*;
            pool_storage_tables.actual_count_tables += 1;
        }

        // pub fn lookupByFirstKey(pool_storage_tables: *PoolStorageTables, key_value: IndexTable.FirstKey) !*const LookupResult {
        //     assert(key_value != 0);

        //     //TODO: P3 need to check how we can clear result not before each lookup, but after this
        //     pool_storage_tables.lookup_result.clearRetainingCapacity();

        //     var last_indx = pool_storage_tables.actual_count_tables;

        //     while (last_indx > 0) {
        //         last_indx -= 1;
        //         if (pool_storage_tables.indexes[last_indx].inFirstInterval(key_value)) {
        //             const entities_range = pool_storage_tables.tables[last_indx].lookupByFirstKey(key_value) catch continue;

        //             pool_storage_tables.lookup_result.appendAssumeCapacity(.{
        //                 .table_ptr = last_indx,
        //                 .entities_range = entities_range,
        //             });
        //         }
        //     }

        //     if (pool_storage_tables.lookup_result.items.len > 0) return pool_storage_tables.lookup_result;

        //     return error.NotFound;
        // }
    };
}
