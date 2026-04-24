const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Io = std.Io;

const printObj = @import("utils/debug.zig").printObj;

const mem_tables = @import("order_item_mem_table.zig"); 
const storage = @import("storage.zig"); 

const ConfigModule = struct {
    module_name: []const u8,
    mem_tables_max_count: mem_tables.MemTablePtr,
    mem_entities_max_count: mem_tables.MemEntryPtr,
    storage_base_dir: std.Io.Dir,
};

pub fn ModuleType(comptime config: ConfigModule) type {
    return struct {
        const Module = @This();
        const MemTablesPool = mem_tables.MemTablePoolType(config.mem_tables_max_count, config.mem_entities_max_count,);
        const Storage = storage.StorageType(config.module_name);

        // FIELDS
        config: ConfigModule = config,
        mem_table_pool: MemTablesPool,
        storage: Storage,
        // writer: u8,
        // reader: u8,

        pub fn init(allocator: std.mem.Allocator, io: std.Io) !*Module {
            var module = try allocator.create(Module);
            module.mem_table_pool = try .init(allocator);
            module.storage = try .init(allocator, io, config.storage_base_dir);

            return module;
        }

         pub fn deinit(module: *Module, allocator: std.mem.Allocator, io: std.Io) void {
            module.storage.deinit(allocator, io);
            module.mem_table_pool.deinit(allocator);
            allocator.destroy(module);
        }
    };
}