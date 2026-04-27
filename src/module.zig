const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Io = std.Io;

const printObj = @import("utils/debug.zig").printObj;

const mem_tables = @import("mem_table.zig");
const storage = @import("storage.zig");
const reader_mem_tables = @import("reader_mem_table.zig");
const Entity = @import("entities.zig").OrderItem;

const ConfigModule = struct {
    module_name: []const u8,
    mem_tables_max_count: mem_tables.MemTablePtr,
    mem_entities_max_count: mem_tables.MemEntryPtr,
    mem_tables_reader_buffer_size: usize,
};

pub fn ModuleType(comptime config: ConfigModule) type {
    return struct {
        const Module = @This();
        const MemTablesPool = mem_tables.MemTablePoolType(
            config.mem_tables_max_count,
            config.mem_entities_max_count,
        );
        const Storage = storage.StorageType(config.module_name, config.mem_tables_reader_buffer_size);
        const ReaderMemTable = reader_mem_tables.ReaderMemTableType(MemTablesPool.TableList);

        // FIELDS
        config: ConfigModule = config,
        storage: *Storage,
        pool_mem_tables: *MemTablesPool,
        reader_mem_tables: *ReaderMemTable,

        pub fn init(allocator: std.mem.Allocator, io: std.Io, storage_base_dir: std.Io.Dir) !*Module {
            var module = try allocator.create(Module);

            module.pool_mem_tables = try .init(allocator);
            errdefer module.pool_mem_tables.deinit(allocator);

            module.storage = try .init(allocator, io, storage_base_dir);
            errdefer module.storage.deinit(allocator, io);

            module.reader_mem_tables = try .init(allocator, module.pool_mem_tables.tables);
            errdefer module.reader_mem_tables.deinit(allocator);

            return module;
        }

        pub fn deinit(module: *Module, allocator: std.mem.Allocator, io: std.Io) void {
            module.storage.deinit(allocator, io);
            module.pool_mem_tables.deinit(allocator);
            module.reader_mem_tables.deinit(allocator);

            allocator.destroy(module);
        }

        pub fn flushAllFilledMemTables(module: *Module, io: std.Io) !usize {
            assert(module.pool_mem_tables.active_table_ptr > 0);

            module.reader_mem_tables.start(0, module.pool_mem_tables.active_table_ptr);

            return try module.storage.streamFrom(io, module.reader_mem_tables);
        }
    };
}

// TESTING

const TestEntity = @import("entities.zig").OrderItem;

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

test "Module: write pool_mem_tables to storage" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const tmp_dir = testing.tmpDir(.{});

    const config_module: ConfigModule = .{
        .module_name = "order_items",
        .mem_tables_max_count = 5,
        .mem_entities_max_count = 5,
        .mem_tables_reader_buffer_size = 4 * 1024,
    };

    var module: *ModuleType(config_module) = try .init(
        allocator,
        io,
        tmp_dir.dir,
    );
    defer module.deinit(allocator, io);
    // Preparing input data
    const entities_total = config_module.mem_entities_max_count * config_module.mem_tables_max_count - 1;

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

    try module.pool_mem_tables.insert(io, input_entities);

    const total_streamed = try module.flushAllFilledMemTables(io);

    try testing.expectEqual(
        module.pool_mem_tables.calculateFilledTables() * config_module.mem_entities_max_count,
        total_streamed,
    );
}
