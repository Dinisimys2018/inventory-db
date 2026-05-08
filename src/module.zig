const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Io = std.Io;

const printObj = @import("utils/debug.zig").printObj;

pub const mem_tables = @import("mem_table.zig");
pub const index_table = @import("index_table.zig");
pub const storage = @import("storage.zig");
pub const zones_storage = @import("zone_storage.zig");
pub const reader_mem_tables = @import("reader_mem_table.zig");
pub const storage_table = @import("storage_table.zig");
pub const lookup = @import("lookup.zig");
pub const m_entities = @import("entities.zig");

const OrderItem = @import("order_item_entity.zig").OrderItem;

pub const EntityEnum = enum {
    order_item,
};

pub const ConfigModule = struct {
    entity: EntityEnum,
    mem_tables_max_count: u16,
    mem_table_filled_limit: u16,
    mem_tables_entities_max_count: u16,
    level_0_tables_count: u16,
    limit_lookup_results: u8,

    pub fn Components(config: *const ConfigModule) type {
        // Use "struct" only for grouping many types to one "namespace"
        return struct {
            // TYPES
            pub const Entity = switch (config.entity) {
                .order_item => OrderItem,
            };

            pub const Module = ModuleType(config);
            pub const IndexTable = Entity.IndexTable;
            pub const MemTable = mem_tables.MemTableType(config);
            pub const MemTablesPool = mem_tables.MemTablePoolType(config);
            pub const GlobalZoneStorage = zones_storage.GlobalZoneType(config);
            pub const Storage = storage.StorageType(config);
            pub const Level_0_PoolStorageTables = storage_table.PoolStorageTablesType(config, 0);
            pub const Lookup = Entity.Lookup(config);

            // CONSTANTS
            pub const mem_tables_entites_max_count_per_insert = config.mem_table_filled_limit * config.mem_tables_entities_max_count;
            pub const entity_size = m_entities.sizeOf(Entity);

            pub const mem_index_size = @sizeOf(IndexTable);
            pub const mem_table_size = entity_size * config.mem_tables_entities_max_count;

            pub const level_0_table_size = mem_table_size;
            pub const level_0_index_size = mem_index_size;

            pub const level_0_headers_size: usize = @sizeOf(Level_0_PoolStorageTables.HeadersStorageLevel);
            pub const level_0_indexes_size: usize = level_0_index_size * config.level_0_tables_count;
            pub const level_0_tables_size: usize = level_0_table_size * config.level_0_tables_count;
        };
    }
};

pub fn ModuleType(comptime config: *const ConfigModule) type {
    const Components = config.Components();

    return struct {
        const Module = @This();

        // FIELDS
        map_fields_meta: *Components.Entity.MapMetaFields,
        prev_insert_batch_time_label: u64,
        prev_insert_batch_offset: mem_tables.BatchOffset,
        storage: *Components.Storage,
        pool_mem_tables: *Components.MemTablesPool,
        lookup: *Components.Lookup,
        level_0_pool_storage_tables: *Components.Level_0_PoolStorageTables,

        pub fn init(allocator: std.mem.Allocator, io: std.Io, storage_base_dir: std.Io.Dir) !*Module {
            var global_zone_storage: *Components.GlobalZoneStorage = try .init(allocator, 0);
            errdefer global_zone_storage.deinit(allocator);

            printObj("level_0_headers_size", Components.level_0_headers_size);

            try global_zone_storage.initZone(allocator, .headers_level_0, Components.level_0_headers_size);
            try global_zone_storage.initZone(allocator, .indexes_level_0, Components.level_0_indexes_size);
            try global_zone_storage.initZone(allocator, .tables_level_0, Components.level_0_tables_size);

            const storage_module: *Components.Storage = try .init(
                allocator,
                io,
                storage_base_dir,
                global_zone_storage,
            );
            errdefer storage_module.deinit(allocator, io);

            var module = try allocator.create(Module);

            module.map_fields_meta = try allocator.create(Components.Entity.MapMetaFields);
            module.map_fields_meta.* = Components.Entity.map_fields_meta;

            module.pool_mem_tables = try .init(allocator);

            module.storage = storage_module;
            module.level_0_pool_storage_tables = try .init(allocator, module);
            module.lookup = try .init(allocator, module, config.limit_lookup_results);
            module.prev_insert_batch_offset = 0;
            module.prev_insert_batch_time_label = 0;
            // TODO: P2 REBUILD
            // Resolve conflict between different configs (storage vs comptime)
            try module.storage.writeToZone(io, .headers_level_0, &module.level_0_pool_storage_tables.headers_encoded);

            return module;
        }

        pub fn deinit(module: *Module, allocator: std.mem.Allocator, io: std.Io) void {
            allocator.destroy(module.map_fields_meta);
            module.storage.deinit(allocator, io);
            module.lookup.deinit(allocator);
            module.pool_mem_tables.deinit(allocator);

            module.level_0_pool_storage_tables.deinit(allocator);

            allocator.destroy(module);
        }

        pub fn insertToMemTables(module: *Module, io: Io, entities: []*Components.Entity) !u16 {
            var inserted_total: u16 = 0;
            var inserted: u16 = 0;
            var end: u16 = Components.mem_tables_entites_max_count_per_insert;
            var attempts: u8 = 0;
            var batch_offset: mem_tables.BatchOffset = 1;

      
            while (inserted_total < entities.len) {
                
                attempts += 1;
                //TODO: P5 need to research limit (maybe trigger real error in release mode)
                assert(attempts < 20);

               //TODO: P3 flushAllFilledMemTables
               // If move io operation flushAllFilledMemTables(it do some delay between inserts) out of this function
               // then we can move generation batch_time_label out of loop
                const batch_time_label: u64 = @intCast(std.Io.Clock.awake.now(io).toMilliseconds());

                  if (batch_time_label == module.prev_insert_batch_time_label) {
                batch_offset = module.prev_insert_batch_offset;
                 } else {
                module.prev_insert_batch_time_label = batch_time_label;
                }

                if (end > entities.len) {
                    end = @intCast(entities.len);
                }

                inserted = try module.pool_mem_tables.insert(entities[inserted_total..end], batch_time_label, batch_offset);

                //TODO: P3 FLUSH_MEM_TABLES
                // Flush tables on storage - VERY SLOW operation
                // So, we need to reseach how can return response on client request
                // without awating for flushing.
                // For example: we can calculate total rest of entities for tables pool and insert only
                // slice via info about rest
                if (inserted == 0) {
                    try module.flushAllFilledMemTables(io);
                }
                batch_offset += inserted;
                inserted_total += inserted;
                end += inserted;
            }

            module.prev_insert_batch_offset = batch_offset;

            return inserted;
        }

        pub fn flushAllFilledMemTables(module: *Module, io: Io) !void {

            var table_ptr: mem_tables.MemTablePtr = 0;

            inline for (Components.Entity.map_fields_meta.values) |field| {
                table_ptr = module.pool_mem_tables.active_table_ptr;

                while (table_ptr < config.mem_tables_max_count) : (table_ptr += 1) {
                    const field_items_bytes = std.mem.sliceAsBytes(module.pool_mem_tables.tables[table_ptr].entities.items(field.tag));
                    try module.storage.writeToZone(io, .tables_level_0, field_items_bytes[0..]);
                }
            }

            
            table_ptr = module.pool_mem_tables.active_table_ptr;

            while (table_ptr < config.mem_tables_max_count) : (table_ptr += 1) {
                    const index = module.pool_mem_tables.getIndex(table_ptr);
                    const index_bytes = std.mem.asBytes(index);

                    try module.storage.writeToZone(io, .indexes_level_0, index_bytes);

                    module.level_0_pool_storage_tables.appendTable(index);
                    module.pool_mem_tables.clearTable(table_ptr);
            }

            module.pool_mem_tables.swapActiveTable();
        }

        pub fn lookupByOrderId(module: *Module, io: Io, value: Components.Entity.OrderId) []const Components.Entity {
            return module.lookup.lookupByFirstKey(io, value);
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
        .entity = .order_item,
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

    const config_module: ConfigModule = .{ .module_name = "order_items", .mem_tables_max_count = 5, .mem_table_filled_limit = 2, .mem_tables_entities_max_count = 5, .level_0_tables_count = 5 * 2 };

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

    const inserted = try module.insertToMemTables(io, input_entities);

    try testing.expectEqual(entities_total, inserted);
}

test "Module:pool_mem_tables: full-filled tables pool and all flush on storage" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const tmp_dir = testing.tmpDir(.{});

    const config_module: ConfigModule = .{
        .entity = .order_item,
        .mem_tables_max_count = 4,
        .mem_table_filled_limit = 2,
        .mem_tables_entities_max_count = 4,
        .level_0_tables_count = 4 * 20,
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

test "Module insert only to memory and lookup" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const config_module: ConfigModule = .{
        .entity = .order_item,
        .mem_tables_max_count = 4,
        .mem_table_filled_limit = 1,
        .mem_tables_entities_max_count = 4,
        .level_0_tables_count = 4 * 2,
        .limit_lookup_results = 200,
    };

    var module: *ModuleType(&config_module) = try .init(
        allocator,
        io,
        tmp_dir.dir,
    );
    defer module.deinit(allocator, io);
    // Preparing input data
    const entities_total = 4;

    var input_entities = try allocator.alloc(*TestEntity, entities_total);
    defer allocator.free(input_entities);

    for (0..entities_total) |index| {
        input_entities[index] = try allocator.create(TestEntity);
    }

    input_entities[0].* = .{
        .batch_offset = 0,
        .time_label = 0,
        .order_id = 1100,
        .product_id = 110,
        .quantity = 10,
    };

    input_entities[1].* = .{
        .batch_offset = 0,

        .time_label = 0,
        .order_id = 2200,
        .product_id = 220,
        .quantity = 20,
    };

    input_entities[2].* = .{
        .batch_offset = 0,

        .time_label = 0,
        .order_id = 3300,
        .product_id = 330,
        .quantity = 30,
    };

    input_entities[3].* = .{
        .batch_offset = 0,

        .time_label = 0,
        .order_id = 2200,
        .product_id = 440,
        .quantity = 40,
    };

    defer for (input_entities) |entry| allocator.destroy(entry);
    // -------------------

    //==== General test ====
    _ = try module.insertToMemTables(io, input_entities);
    _ = try module.insertToMemTables(io, input_entities);
    _ = try module.insertToMemTables(io, input_entities);
    _ = try module.insertToMemTables(io, input_entities);
    _ = try module.insertToMemTables(io, input_entities);

    const lookup_result = module.lookupByOrderId(io, 2200);
    for (lookup_result) |ent| {
        printObj("OrderItem", ent);
    }

    // try testing.expectEqual(2, lookup_result.len);
    // try testing.expectEqualDeep(input_entities[3].*, lookup_result[0]);
    // try testing.expectEqualDeep(input_entities[1].*, lookup_result[1]);
}
