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

    const actual_tables_count = switch (level) {
        0 => config.level_0_tables_count,
        else => unreachable,
    };

    const actual_entities_count = switch (level) {
        0 => config.mem_tables_entities_max_count,
        else => unreachable,
    };

    return struct {
        const HeadersStorageLevel = @This();

        const FieldMeta = struct {
            offset: usize,
            items_size: usize,
        };

        // FIELDS
        index_size: usize,
        tables_count: usize,
        entities_count: usize,
        map_fields: std.EnumMap(Components.Entity.Field, FieldMeta),

        pub fn init(allocator: Allocator, module: *Components.Module) !*HeadersStorageLevel {
            const headers = try allocator.create(HeadersStorageLevel);
            //TODO: P2 REBUILD
            // Research how we can restore headers and resolve conflicts of configs
            headers.* = .{
                .index_size = actual_index_size,
                .tables_count = actual_tables_count,
                .entities_count = actual_entities_count,
                .map_fields = undefined,
            };

            var field_offset: usize = 0;
            var field_items_size: usize = 0;
            var map_fields_meta_iterator = module.map_fields_meta.iterator();
            while (map_fields_meta_iterator.next()) |field_meta_kv| {
                field_items_size = field_meta_kv.value.size * headers.entities_count * headers.tables_count;

                headers.map_fields.put(
                    field_meta_kv.key,
                    .{
                        .offset = field_offset,
                        .items_size = field_items_size,
                    },
                );
                field_offset += field_items_size;
            }

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

    return struct {
        const PoolStorageTables = @This();
        pub const HeadersStorageLevel = HeadersStorageLevelType(config, level);

        // FIELDS
        headers: *HeadersStorageLevel,
        headers_encoded: [Components.level_0_headers_size]u8,
        actual_count_tables: usize,
        indexes: []*Index,
        table_offsets: []usize,
        module: *Components.Module,

        pub fn init(allocator: Allocator, module: *Components.Module) !*PoolStorageTables {
            const pool_storage_tables = try allocator.create(PoolStorageTables);
            // TODO: P3 REBUILD
            // loading from storage for working between diferrent configs
            pool_storage_tables.headers = try .init(allocator, module);
            pool_storage_tables.headers_encoded = std.mem.asBytes(pool_storage_tables.headers).*;
            pool_storage_tables.actual_count_tables = 0;
            pool_storage_tables.module = module;
            pool_storage_tables.indexes = try allocator.alloc(*Index, pool_storage_tables.headers.tables_count);
            pool_storage_tables.table_offsets = try allocator.alloc(usize, pool_storage_tables.headers.tables_count);

            for (0..pool_storage_tables.headers.tables_count) |table_ptr| {
                pool_storage_tables.indexes[table_ptr] = try .init(allocator);
                pool_storage_tables.table_offsets[table_ptr] = table_ptr * pool_storage_tables.headers.entities_count;
            }

            return pool_storage_tables;
        }

        pub fn deinit(pool_storage_tables: *PoolStorageTables, allocator: Allocator) void {
            pool_storage_tables.headers.deinit(allocator);
            pool_storage_tables.headers_encoded = undefined;

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

        pub fn readFieldSector(
            pool_storage_tables: *PoolStorageTables,
            io: Io,
            table_ptr: usize,
            field: Components.Entity.Field,
            buffer: []u8,
        ) usize {
            const field_offset = pool_storage_tables.headers.map_fields.getAssertContains(field).offset + pool_storage_tables.table_offsets[table_ptr];
                
            return pool_storage_tables.module.storage.readFromZone(
                io,
                .tables_level_0,
                field_offset,
                buffer,
            );
        }
    };
}
