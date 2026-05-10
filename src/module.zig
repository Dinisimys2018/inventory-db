const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Io = std.Io;
const Allocator = std.mem.Allocator;

const log = @import("utils/debug.zig").ModulePrinterType(.module);
pub const mem_tables = @import("mem_table.zig");
pub const index_table = @import("index_table.zig");
pub const storage = @import("storage.zig");
pub const zones_storage = @import("zone_storage.zig");
pub const reader_mem_tables = @import("reader_mem_table.zig");
pub const storage_table = @import("storage_table.zig");
pub const lookup = @import("lookup.zig");
pub const m_entities = @import("entities.zig");
pub const m_sheduler = @import("scheduler.zig");

const OrderItem = @import("order_item_entity.zig").OrderItem;

pub const EntityEnum = enum {
    order_item,
};

pub const ConfigModule = struct {
    entity: EntityEnum,
    mem_tables_blocks_count: u8,
    mem_tables_count_in_block: u16,
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
            pub const Scheduler = m_sheduler.SchedulerType(config);

            // CONSTANTS
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
        scheduler: *Components.Scheduler,

        pub fn init(allocator: Allocator, io: std.Io, storage_base_dir: std.Io.Dir) !*Module {
            var global_zone_storage: *Components.GlobalZoneStorage = try .init(allocator, 0);
            errdefer global_zone_storage.deinit(allocator);

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

            module.scheduler = try .init(allocator);

            try module.scheduler.appendTask(
                allocator,
                Module.flushFilledMemBlocks,
                .{ module, io },
                .{
                    .every_millisec = 5000,
                },
            );

            return module;
        }

        pub fn deinit(module: *Module, allocator: Allocator, io: std.Io) void {
            module.scheduler.deinit(allocator);

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
            var attempts: u8 = 0;
            var batch_offset: mem_tables.BatchOffset = 1;

            const batch_time_label: u64 = @intCast(std.Io.Clock.awake.now(io).toMilliseconds());

            if (batch_time_label == module.prev_insert_batch_time_label) {
                batch_offset = module.prev_insert_batch_offset;
            } else {
                module.prev_insert_batch_time_label = batch_time_label;
            }

            while (inserted_total < entities.len) {
                attempts += 1;
                //TODO: P5 need to research limit (maybe trigger real error in release mode)
                assert(attempts < 20);

                if (module.pool_mem_tables.state != .ready_to_inserts) {
                    io.sleep(std.Io.Duration.fromMilliseconds(1), .awake) catch {};
                    continue;
                }

                inserted = module.pool_mem_tables.insert(
                    entities[inserted_total..],
                    batch_time_label,
                    batch_offset,
                );
                    log.obj("pool state after insert", module.pool_mem_tables.state);

                batch_offset += inserted;
                inserted_total += inserted;
            }

            module.prev_insert_batch_offset = batch_offset;

            return inserted;
        }

        pub fn flushFilledMemBlocks(module: *Module, io: Io) Io.Cancelable!void {
            log.obj("flushFilledMemBlocks",  module.pool_mem_tables.blocks[1]);

            const last_block_ptr = config.mem_tables_blocks_count - 1;
            var block_ptr = module.pool_mem_tables.active_block_ptr;
            var block: *mem_tables.Block = undefined;
            var start_table_ptr: mem_tables.MemTablePtr = 0;
            var end_table_ptr: mem_tables.MemTablePtr = 0;

            var block_ptrs_buffer: [config.mem_tables_blocks_count]mem_tables.MemBlockPtr = undefined;
            var block_ptrs_idx: mem_tables.MemBlockPtr = 0;

            while (block_ptrs_idx < config.mem_tables_blocks_count) {
                if (block_ptr == last_block_ptr) {
                    block_ptr = 0;
                }
                block = module.pool_mem_tables.blocks[block_ptr];
                                    log.obj("check block", block);

                if (block.state == .filled) {

                    block.state = .started_flush;
                    if (block_ptrs_idx == 0) {
                        start_table_ptr = block.start_ptr;
                    }
                    end_table_ptr = block.end_ptr;
                    block_ptrs_buffer[block_ptrs_idx] = block_ptr;
                    block_ptrs_idx += 1;
                } else {
                    break;
                }
                block_ptr += 1;
            }
            log.obj("block_ptrs_idx", block_ptrs_idx);

            log.obj("block_ptrs_buffer", block_ptrs_buffer);
            if (block_ptrs_idx == 0) return;

            var table_ptr: mem_tables.MemTablePtr = undefined;

            inline for (Components.Entity.map_fields_meta.values) |field| {
                table_ptr = start_table_ptr;

                while (table_ptr < end_table_ptr) : (table_ptr += 1) {
                    const field_items_bytes = std.mem.sliceAsBytes(module.pool_mem_tables.tables[table_ptr].entities.items(field.tag));
                     module.storage.writeToZone(io, .tables_level_0, field_items_bytes[0..]) catch |err| {
                    log.err(err);
                    return Io.Cancelable.Canceled;
                };
                }
            }

            table_ptr = start_table_ptr;

            while (table_ptr < end_table_ptr) : (table_ptr += 1) {
                const index = module.pool_mem_tables.getIndex(table_ptr);
                const index_bytes = std.mem.asBytes(index);

                module.storage.writeToZone(io, .indexes_level_0, index_bytes) catch |err| {
                    log.err(err);
                    return Io.Cancelable.Canceled;
                };

                module.level_0_pool_storage_tables.appendTable(index);
                module.pool_mem_tables.clearTable(table_ptr);
            }

            for (block_ptrs_buffer) |block_ptr_clear| {
                module.pool_mem_tables.blocks[block_ptr_clear].state = .empty;
            }
        }

        pub fn lookupByOrderId(module: *Module, io: Io, value: Components.Entity.OrderId) []const Components.Entity {
            return module.lookup.lookupByFirstKey(io, value);
        }
    };
}

// TESTING

const TestEntity = @import("order_item_entity.zig").OrderItem;

fn testPreparingUniqueEntries(allocator: Allocator, entries_total: usize) ![]*TestEntity {
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

test "1Module insert only to memory and lookup" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const config_module: ConfigModule = .{
        .entity = .order_item,
        .mem_tables_blocks_count = 2,
        .mem_tables_count_in_block = 2,
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

    // //==== General test ====
    _ = try module.insertToMemTables(io, input_entities);
    try module.flushFilledMemBlocks(io);
    _ = try module.insertToMemTables(io, input_entities);
        try module.flushFilledMemBlocks(io);
    _ = try module.insertToMemTables(io, input_entities);
        try module.flushFilledMemBlocks(io);

    _ = try module.insertToMemTables(io, input_entities);
        try module.flushFilledMemBlocks(io);

    _ = try module.insertToMemTables(io, input_entities);
        try module.flushFilledMemBlocks(io);


    // const lookup_result = module.lookupByOrderId(io, 2200);
    // for (lookup_result) |ent| {
    //     log.obj("OrderItem", ent);
    // }

    // try testing.expectEqual(2, lookup_result.len);
    // try testing.expectEqualDeep(input_entities[3].*, lookup_result[0]);
    // try testing.expectEqualDeep(input_entities[1].*, lookup_result[1]);
}
