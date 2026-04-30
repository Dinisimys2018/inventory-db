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

pub fn StorageTableType() type {
    return struct {
        const StorageTable = @This();

        // FIELDS
        index: *IndexTable,

        pub fn init(allocator: Allocator) !*StorageTable {
            const storage_table = try allocator.create(StorageTable);
            storage_table.index = try .init(allocator);
            return storage_table;
        }

        pub fn copyIndex(storage_table: *StorageTable, index: *IndexTable) void {
            storage_table.index.* = index.*;
        }

        pub fn deinit(storage_table: *StorageTable, allocator: Allocator) void {
            storage_table.index.deinit(allocator);
            allocator.destroy(storage_table);
        }
    };
}

pub fn PoolStorageTablesType(comptime tables_max_count: usize) type {
    return struct {
        const PoolStorageTables = @This();
        const StorageTable = StorageTableType();

        // FIELDS
        tables: []*StorageTable,

        pub fn init(allocator: Allocator) !*PoolStorageTables {
            const pool_storage_tables = try allocator.create(PoolStorageTables);
            pool_storage_tables.tables = try allocator.alloc(*StorageTable, tables_max_count);
            for (0..tables_max_count) |table_ptr| {
                pool_storage_tables.tables[table_ptr] = try .init(allocator);
            }
            return pool_storage_tables;
        }

        pub fn deinit(pool_storage_tables: *PoolStorageTables, allocator: Allocator) void {
            for (pool_storage_tables.tables) |table| {
                table.deinit(allocator);
            }
            allocator.free(pool_storage_tables.tables);
            allocator.destroy(pool_storage_tables);
        }
    };
}
