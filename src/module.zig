const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Io = std.Io;

const printObj = @import("utils/debug.zig").printObj;

const mem_tables = @import("mem_table.zig");
const index_table = @import("index_table.zig");
const storage = @import("storage.zig");
const zones_storage = @import("zone_storage.zig");
const reader_mem_tables = @import("reader_mem_table.zig");
const storage_table = @import("storage_table.zig");

const Entity = @import("order_item_entity.zig").OrderItem;

const ConfigModule = struct {
    module_name: []const u8,
    mem_tables_max_count: mem_tables.MemTablePtr,
    mem_table_filled_limit: mem_tables.MemTablePtr,
    mem_tables_entities_max_count: mem_tables.MemEntryPtr,
    mem_tables_reader_buffer_size: usize,
    level_0_tables_count: u32,
};

const IndexTable = index_table.IndexTableWithTwoKeysType(
    Entity,
    Entity.OrderId,
    Entity.ProductId,
    "order_id",
    "product_id",
);

pub fn ModuleType(comptime config: ConfigModule) type {
    const mem_tables_entites_max_count_per_insert = config.mem_table_filled_limit * config.mem_tables_entities_max_count;
    const entity_size = @sizeOf(Entity);
    const index_table_size = @sizeOf(IndexTable);
    const index_tables_level_0_size: usize = index_table_size * config.level_0_tables_count;
    const data_tables_level_0_size: usize = entity_size * config.mem_tables_entities_max_count * config.level_0_tables_count;

    return struct {
        const Module = @This();
        const MemTablesPool = mem_tables.MemTablePoolType(
            config.mem_tables_max_count,
            config.mem_tables_entities_max_count,
        );

        const Level_0_PoolStorageTables = storage_table.PoolStorageTablesType(
            config.level_0_tables_count,
        );

        const IndexReaderMemTable = reader_mem_tables.IndexReaderMemTableType(MemTablesPool);
        const DataReaderMemTable = reader_mem_tables.DataReaderMemTableType(MemTablesPool);

        const GlobalZoneStorage = zones_storage.GlobalZoneType();

        const Storage = storage.StorageType(
            GlobalZoneStorage,
            config.module_name,
            config.mem_tables_reader_buffer_size,
        );

        // FIELDS
        config: ConfigModule = config,
        storage: *Storage,
        pool_mem_tables: *MemTablesPool,
        index_reader_mem_tables: *IndexReaderMemTable,
        data_reader_mem_tables: *DataReaderMemTable,

        level_0_pool_storage_tables: *Level_0_PoolStorageTables,

        pub fn init(allocator: std.mem.Allocator, io: std.Io, storage_base_dir: std.Io.Dir) !*Module {
            var global_zone_storage: *GlobalZoneStorage = try .init(allocator, 0);
            errdefer global_zone_storage.deinit(allocator);

            try global_zone_storage.initZone(allocator, .index_tables_level_0, index_tables_level_0_size);
            try global_zone_storage.initZone(allocator, .data_tables_level_0, data_tables_level_0_size);

            const storage_module: *Storage = try .init(
                allocator,
                io,
                storage_base_dir,
                global_zone_storage,
            );
            errdefer storage_module.deinit(allocator, io);

            var module = try allocator.create(Module);

            module.pool_mem_tables = try .init(allocator);
            module.storage = storage_module;
            module.index_reader_mem_tables = try .init(allocator, module.pool_mem_tables);
            module.data_reader_mem_tables = try .init(allocator, module.pool_mem_tables);

            module.level_0_pool_storage_tables = try .init(allocator);

            return module;
        }

        pub fn deinit(module: *Module, allocator: std.mem.Allocator, io: std.Io) void {
            module.storage.deinit(allocator, io);
            module.pool_mem_tables.deinit(allocator);
            module.index_reader_mem_tables.deinit(allocator);
            module.data_reader_mem_tables.deinit(allocator);
            module.level_0_pool_storage_tables.deinit(allocator);

            allocator.destroy(module);
        }

        pub fn insertToMemTables(module: *Module, io: std.Io, entities: []*Entity) !usize {
            var inserted: usize = 0;
            var end: usize = mem_tables_entites_max_count_per_insert;
            var attempts: usize = 0;

            while (inserted < entities.len) {
                attempts += 1;
                //TODO: P5 need to research limit (maybe trigger real error in release mode)
                assert(attempts < 20);

                if (end > entities.len) {
                    end = entities.len;
                }

                inserted += try module.pool_mem_tables.insert(io, entities[inserted..end]);

                assert(inserted > 0);
                end += inserted;

                //TODO: P3 Flush tables on storage - VERY SLOW operation
                // So, we need to reseach how can return response on client request
                // without awating for flushing.
                // For example: we can calculate total rest of entities for tables pool and insert only
                // slice via info about rest
                if (inserted >= mem_tables_entites_max_count_per_insert) {
                    try module.flushAllFilledMemTables(io);
                }
            }

            return inserted;
        }

        pub fn flushAllFilledMemTables(module: *Module, io: std.Io) !void {
            var table_ptr: mem_tables.MemTablePtr = 0;
            var total_streamed_bytes: usize = 0;

            while (table_ptr < module.pool_mem_tables.filled_table_ptrs.len) : (table_ptr += 1) {
                if (module.pool_mem_tables.filled_table_ptrs[table_ptr]) {
                    const index = module.pool_mem_tables.getIndex(table_ptr);

                    const index_bytes = std.mem.asBytes(index);

                    try module.storage.writeToZone(io, .index_tables_level_0, index_bytes);
                    total_streamed_bytes += index_bytes.len;

                    inline for (Entity.map_field_tags.values) |field| {
                        const field_items_bytes = std.mem.asBytes(&module.pool_mem_tables.tables[table_ptr].entities.items(field));
                        try module.storage.writeToZone(io, .data_tables_level_0, field_items_bytes);
                    }

                    module.level_0_pool_storage_tables.appendTable(index);
                    module.pool_mem_tables.freeFilledTable(table_ptr);
                }
            }
        }

        pub fn lookupByOrderId(module: *Module, value: Entity.OrderId) !*const mem_tables.LookupResult {
            const mem_lookup_result = module.pool_mem_tables.lookupByOrderId(value);

            return mem_lookup_result;
        }
    };
}

// TESTING

const TestEntity = @import("order_item_entity.zig").OrderItem;

fn testPreparingUniqueEntries(allocator: std.mem.Allocator, entries_total: usize) ![]*TestEntity {
    var input_entries: []*TestEntity = try allocator.alloc(*TestEntity, entries_total);
    errdefer allocator.free(input_entries);

    var index: mem_tables.MemEntryPtr = 0;
    errdefer {
        for (input_entries) |entity| {
            allocator.destroy(entity);
        }
    }

    while (index < entries_total) : (index += 1) {
        const entity = try allocator.create(TestEntity);
        entity.* = .{
            .time_label = 0,
            .order_id = @intCast(index + 1),
            .product_id = @intCast(index + 2),
        };
        input_entries[index] = entity;
    }

    return input_entries;
}

test "Module:pool_mem_tables: nothing to flush on storage" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const tmp_dir = testing.tmpDir(.{});

    const config_module: ConfigModule = .{
        .module_name = "order_items",
        .mem_tables_max_count = 5,
        .mem_table_filled_limit = 4,
        .mem_tables_entities_max_count = 5,
        .mem_tables_reader_buffer_size = 4 * 1024,
        .level_0_tables_count = 5 * 2,
    };

    var module: *ModuleType(config_module) = try .init(
        allocator,
        io,
        tmp_dir.dir,
    );
    defer module.deinit(allocator, io);
    // Preparing input data
    const entities_total = config_module.mem_tables_entities_max_count - 1;

    const input_entities = try testPreparingUniqueEntries(
        allocator,
        entities_total,
    );

    defer {
        for (input_entities) |entry| allocator.destroy(entry);
        allocator.free(input_entities);
    }

    // -------------------

    //==== General test ====

    const insert_result = try module.insertToMemTables(io, input_entities);
    const expected_entities_flushed = 0;

    try testing.expectEqual(entities_total, insert_result[0]);
    try testing.expectEqual(expected_entities_flushed, insert_result[1]);
}

test "Module:pool_mem_tables: limited filled tables to flush on storage" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const tmp_dir = testing.tmpDir(.{});

    const config_module: ConfigModule = .{
        .module_name = "order_items",
        .mem_tables_max_count = 5,
        .mem_table_filled_limit = 2,
        .mem_tables_entities_max_count = 5,
        .mem_tables_reader_buffer_size = 4 * 1024,
    };

    var module: *ModuleType(config_module) = try .init(
        allocator,
        io,
        tmp_dir.dir,
    );
    defer module.deinit(allocator, io);
    // Preparing input data
    const entities_total = config_module.mem_table_filled_limit * config_module.mem_tables_entities_max_count;

    const input_entities = try testPreparingUniqueEntries(
        allocator,
        entities_total,
    );

    defer {
        for (input_entities) |entry| allocator.destroy(entry);
        allocator.free(input_entities);
    }

    // -------------------

    //==== General test ====

    const insert_result = try module.insertToMemTables(io, input_entities);
    const expected_entities_flushed = entities_total;

    try testing.expectEqual(entities_total, insert_result[0]);
    try testing.expectEqual(expected_entities_flushed, insert_result[1]);
}

test "Module:pool_mem_tables: full-filled tables pool and all flush on storage" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const tmp_dir = testing.tmpDir(.{});

    const config_module: ConfigModule = .{
        .module_name = "order_items",
        .mem_tables_max_count = 5,
        .mem_table_filled_limit = 2,
        .mem_tables_entities_max_count = 5,
        .mem_tables_reader_buffer_size = 4 * 1024,
        .level_0_tables_count = 5 * 20,
    };

    var module: *ModuleType(config_module) = try .init(
        allocator,
        io,
        tmp_dir.dir,
    );
    defer module.deinit(allocator, io);
    // Preparing input data
    const entities_total = config_module.mem_tables_entities_max_count * config_module.mem_tables_max_count * 2;

    const input_entities = try testPreparingUniqueEntries(
        allocator,
        entities_total,
    );

    defer {
        for (input_entities) |entry| allocator.destroy(entry);
        allocator.free(input_entities);
    }

    // -------------------

    //==== General test ====

    const insert_result = try module.insertToMemTables(io, input_entities);

    try testing.expectEqual(entities_total, insert_result);
}
