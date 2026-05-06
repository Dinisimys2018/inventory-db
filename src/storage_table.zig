const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Io = std.Io;

const printObj = @import("utils/debug.zig").printObj;

const m_module = @import("module.zig");


pub fn PoolStorageTablesType(
    comptime config: *const m_module.ConfigModule,
    level: u8,
) type {
    const Components = config.Components();
    const Index = Components.IndexTable;
    const index_size = switch (level) {
        0 => Components.level_0_index_size,
        else => unreachable,
    };
    const table_size = switch (level) {
        0 => Components.level_0_table_size,
        else => unreachable,
    };


    return struct {
        const StorageTable = Components.StorageTable;

        const PoolStorageTables = @This();

        // FIELDS
        module: *Components.Module,
        actual_count_tables: usize,
        indexes: []*Index,
        table_offsets: []usize,

        pub fn init(allocator: Allocator, module: *Components.Module) !*PoolStorageTables {
            const pool_storage_tables = try allocator.create(PoolStorageTables);
            pool_storage_tables.* = .{
                .module = module,
                .indexes = try allocator.alloc(*Index, config.level_0_tables_count),
                .tables = try allocator.alloc(*StorageTable, config.level_0_tables_count),
                .actual_count_tables = 0,
            };

            for (0..config.level_0_tables_count) |table_ptr| {
                // TODO: P5 REBUILD
                // loading from storage for working between diferrent verions fields meta
                pool_storage_tables.indexes[table_ptr] = try .init(allocator);
                pool_storage_tables.tables[table_ptr] = try .init(allocator);

                pool_storage_tables.table_offsets[table_ptr] = table_ptr * module.storage_table_headers.table_size;
            }

            return pool_storage_tables;
        }

        pub fn deinit(pool_storage_tables: *PoolStorageTables, allocator: Allocator) void {
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

        pub fn readFieldSector(pool_storage_tables: *PoolStorageTables, io: Io, table_ptr: usize, field: Components.Entity.Field) !void {
            const headers = pool_storage_tables.headers_list[table_ptr];
            const position =
                try pool_storage_tables.module.storage.readFromZone(io, .tables_level_0, position, buffer);
        }
    };
}
