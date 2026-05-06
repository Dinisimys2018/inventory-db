const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Io = std.Io;

const printObj = @import("utils/debug.zig").printObj;

const m_module = @import("module.zig");

pub fn HeadersStorageLevelType(comptime config: *const m_module.ConfigModule, level: u8) type {
    const Components = config.Components();
    const actual_index_size = switch (level) {
        0 => Components.level_0_index_size,
        else => unreachable,
    };
    const actual_table_size = switch (level) {
        0 => Components.level_0_table_size,
        else => unreachable,
    };

    const actual_tables_count = switch (level) {
        0 => config.level_0_tables_count,
        else => unreachable,
    };

    return struct {
        const HeadersStorageLevel = @This();
        // FIELDS
        index_size: usize,
        table_size: usize,
        tables_count: usize,

        pub fn init(allocator: Allocator) !*HeadersStorageLevel {
            const headers = try allocator.create(HeadersStorageLevel);
            //TODO: P2 REBUILD
            // Research how we can restore headers and resolve conflicts of configs
            headers.* = .{
                .index_size = actual_index_size,
                .table_size = actual_table_size,
                .tables_count = actual_tables_count,
            };

            return headers;
        }

        pub fn deinit(headers: *HeadersStorageLevel, allocator: Allocator) void {
            allocator.destroy(headers);
        }
    };
}

pub fn PoolStorageTablesType(
    comptime config: *const m_module.ConfigModule,
    level: u8,
) type {
    const Components = config.Components();
    const Index = Components.IndexTable;
    const HeadersStorageLevel = HeadersStorageLevelType(config, level);

    return struct {
        const PoolStorageTables = @This();

        // FIELDS
        headers: *HeadersStorageLevel,
        actual_count_tables: usize,
        indexes: []*Index,
        table_offsets: []usize,

        pub fn init(allocator: Allocator) !*PoolStorageTables {
            const pool_storage_tables = try allocator.create(PoolStorageTables);
            pool_storage_tables.* = .{
                // TODO: P5 REBUILD
                // loading from storage for working between diferrent configs
                .headers = try .init(allocator),
                .actual_count_tables = 0,
            };

            pool_storage_tables.indexes = try allocator.alloc(*Index, pool_storage_tables.headers.tables_count);
            pool_storage_tables.table_offsets = try allocator.alloc(usize, pool_storage_tables.headers.tables_count);

            for (0..pool_storage_tables.headers.tables_count) |table_ptr| {
                pool_storage_tables.indexes[table_ptr] = try .init(allocator);
                pool_storage_tables.table_offsets[table_ptr] = table_ptr * pool_storage_tables.headers.table_size;
            }

            return pool_storage_tables;
        }

        pub fn deinit(pool_storage_tables: *PoolStorageTables, allocator: Allocator) void {
            pool_storage_tables.headers.deinit(allocator);

            for (pool_storage_tables.indexes) |index| {
                index.deinit(allocator);
            }
            allocator.free(pool_storage_tables.indexes);

            allocator.free(pool_storage_tables.table_offsets);

            allocator.destroy(pool_storage_tables);
        }

        pub fn appendTable(pool_storage_tables: *PoolStorageTables, index: *Index) void {
            pool_storage_tables.indexes[pool_storage_tables.actual_count_tables].* = index.*;
            pool_storage_tables.actual_count_tables += 1;
        }

        pub fn readFieldSector(pool_storage_tables: *PoolStorageTables, io: Io, table_ptr: usize, field: Components.Entity.Field) !void {
            const headers = pool_storage_tables.headers_list[table_ptr];
            const offset_field = pool_storage_tables.table_offsets[table_ptr];
            try pool_storage_tables.module.storage.readFromZone(io, .tables_level_0, position, buffer);
        }
    };
}
