//! Notes:
//! - Порядок создания индексов в IndexPool должен совпадать с порядок создания таблиц в MemTablePool
//! ---- Таким образом мы гарантируем что table_ptr в индексе соответсвует таблице в пулле
//! - Важен порядок вставки entities в MemTable, они должен совпадать с порядком вставки ключей в IndexPool
//! ---- Таким образом мы гарантируем что value_ptr в индексе соответствует entry в таблице

const std = @import("std");
const ArrayList = std.ArrayList;
const assert = std.debug.assert;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const printObj = @import("utils/debug.zig").printObj;
const stdx_sort = @import("sort.zig");
const index_table = @import("index_table.zig");
const module = @import("module.zig");

pub const MemTablePtr = u32;
pub const MemEntryPtr = usize;

const EntitiesRange = struct { usize, usize };

pub const TableLookupResult = struct {
    table_ptr: MemTablePtr,
    entities_range: EntitiesRange,
};

pub const LookupResult = std.ArrayList(TableLookupResult);

pub fn MemTableType(comptime config: *const module.ConfigModule) type {
    const Components = config.Components();

    return struct {
        const MemTable = @This();

        //TODO: Research the feasibility of moving entities into a pool
        // and storing only pointers to a shared buffer in the table

        entities: *Components.Entity.Entities,

        pub fn init(allocator: Allocator) !*MemTable {
            const mem_table = try allocator.create(MemTable);
            mem_table.* = .{
                .entities = try allocator.create(Components.Entity.Entities),
            };

            mem_table.entities.* = try .initCapacity(allocator, config.mem_tables_entities_max_count);
            return mem_table;
        }

        pub fn deinit(mem_table: *MemTable, allocator: Allocator) void {
            mem_table.entities.deinit(allocator);
            allocator.destroy(mem_table.entities);
            allocator.destroy(mem_table);
        }

        /// return unique next time_label
        pub fn insert(mem_table: *MemTable, init_time_label: u64, entities: []*Components.Entity) u64 {
            var time_label = init_time_label;
            for (entities) |entity| {
                entity.time_label = time_label;
                mem_table.entities.appendAssumeCapacity(entity.*);
                time_label += 1;
            }
            return time_label;
        }

        pub fn lookupByOrderId(mem_table: *MemTable, key_value: Components.Entity.OrderId) !EntitiesRange {
            mem_table.primarySort();

            const range = stdx_sort.equalRangeDesc(
                Components.Entity.OrderId,
                mem_table.entities.slice().items(Components.Entity.map_field_tags.get(.order_id)),
                key_value,
                stdx_sort.compareNumberKeys(Components.Entity.OrderId),
            );

            if (range[1] == 0) return error.NotFound;

            return range;
        }

        pub fn clear(mem_table: *MemTable) void {
            mem_table.entities.clearRetainingCapacity();
        }
    };
}

const PoolState = enum {
    empty,
    started_flush,
    finished_flush,
};

pub fn MemTablePoolType(comptime config: *const module.ConfigModule) type {
    const Components = config.Components();
    const last_table_ptr = config.mem_tables_max_count - 1;

    return struct {
        const MemTablePool = @This();
        const MemTable = Components.MemTable;
        const Index = Components.IndexTable;

        // Struct Fields
        tables: []*MemTable,
        indexes: []*Index,
        active_table: *MemTable,
        active_index: *Index,

        sorted_active: bool,
        active_ptr: MemTablePtr,
        start_filled_ptr: MemTablePtr,
        state: PoolState,

        pub fn init(allocator: Allocator) !*MemTablePool {
            var mem_table_pool = try allocator.create(MemTablePool);
            mem_table_pool.tables = try allocator.alloc(*MemTable, config.mem_tables_max_count);
            mem_table_pool.indexes = try allocator.alloc(*Index, config.mem_tables_max_count);
            mem_table_pool.sorted_active = false;
            mem_table_pool.state = .empty;

            var table_ptr: MemTablePtr = 0;

            while (table_ptr < config.mem_tables_max_count) : (table_ptr += 1) {
                mem_table_pool.tables[table_ptr] = try .init(allocator);
                mem_table_pool.indexes[table_ptr] = try .init(allocator);
            }

            mem_table_pool.start_filled_ptr = config.mem_tables_max_count;
            mem_table_pool.active_ptr = last_table_ptr;
            mem_table_pool.active_table = mem_table_pool.tables[mem_table_pool.active_ptr];
            mem_table_pool.active_index = mem_table_pool.indexes[mem_table_pool.active_ptr];

            return mem_table_pool;
        }

        pub fn deinit(table_pool: *MemTablePool, allocator: Allocator) void {
            for (table_pool.indexes) |index| {
                index.deinit(allocator);
            }
            allocator.free(table_pool.indexes);

            for (table_pool.tables) |table| {
                table.deinit(allocator);
            }
            allocator.free(table_pool.tables);

            allocator.destroy(table_pool);
        }

        pub fn getIndex(table_pool: *MemTablePool, table_ptr: MemTablePtr) *Components.IndexTable {
            return table_pool.indexes[table_ptr];
        }

        pub fn insert(table_pool: *MemTablePool, io: std.Io, entities: []*Components.Entity) !usize {
            // TODO: Temporary solution, lock insert in flushing proccess,
            // but not need lock active table for concurrency inserting
            assert(table_pool.state == .finished_flush or table_pool.state == .empty);

            table_pool.sorted_active = false;
            var entries_start: usize = 0;
            var entries_end: usize = 0;
            //TODO: P5 maybe move syscall for generate time_label to high level
            var next_time_label: u64 = @intCast(std.Io.Clock.awake.now(io).toMilliseconds());

            var attempts: usize = 0;

            while (entries_end < entities.len) {
                //TODO: P5 need to research limit (maybe trigger real error in release mode)
                attempts += 1;
                assert(attempts < 50);

                // Получаем количество, которое мы можем вставить в активную таблицу
                const rest = table_pool.active_table.entities.capacity - table_pool.active_table.entities.len;
                entries_end += rest;

                if (entries_end >= entities.len) {
                    entries_end = entities.len;
                }

                const to_insert = entities[entries_start..entries_end];
                next_time_label = table_pool.active_table.insert(next_time_label, to_insert);
                table_pool.sortActive();

                table_pool.active_index.rewriteMin(&table_pool.active_table.entities.get(table_pool.active_table.entities.len - 1));
                table_pool.active_index.rewriteMax(&table_pool.active_table.entities.get(0));

                // Is Active table filled ?
                if (rest == to_insert.len) {
                    table_pool.start_filled_ptr -= 1;

                    // Pool overflow
                    if (table_pool.start_filled_ptr == 0) {
                        return entries_end;
                    }
                    
                    table_pool.sorted_active = false;
                    table_pool.active_ptr -= 1;
                    table_pool.active_table = table_pool.tables[table_pool.active_ptr];
                    table_pool.active_index = table_pool.indexes[table_pool.active_ptr];
                }

                entries_start = entries_end;
            }

            return entries_end;
        }

        pub fn sortActive(table_pool: *MemTablePool) void {
            if (table_pool.sorted_active) return;

            table_pool.active_table.entities.sortUnstable(Components.Entity.SortCtx{ .entities = table_pool.active_table.entities });
            table_pool.sorted_active = true;
        }

        pub fn swapActiveTable(table_pool: *MemTablePool) void {
            const tmp_index = table_pool.indexes[last_table_ptr].*;
            table_pool.indexes[last_table_ptr].* = table_pool.indexes[table_pool.active_ptr].*;
            table_pool.indexes[table_pool.active_ptr].* = tmp_index;

            const tmp_table = table_pool.tables[last_table_ptr].*;
            table_pool.tables[last_table_ptr].* = table_pool.tables[table_pool.active_ptr].*;
            table_pool.tables[table_pool.active_ptr].* = tmp_table;

            table_pool.start_filled_ptr = config.mem_tables_max_count;
            table_pool.active_ptr = last_table_ptr;

            table_pool.state = .finished_flush;
        }

        pub fn clearTable(table_pool: *MemTablePool, table_ptr: MemTablePtr) void {
            table_pool.state = .started_flush;

            table_pool.tables[table_ptr].clear();
            table_pool.indexes[table_ptr].clear();
        }

        pub fn getOneEntity(
            table_pool: *MemTablePool,
            table_ptr: MemTablePtr,
            entity_ptr: MemEntryPtr,
        ) Components.Entity {
            return table_pool.tables[table_ptr].entities.get(entity_ptr);
        }

        pub fn getActualEntities(
            mem_table_pool: *MemTablePool,
            lookup_result: *const LookupResult,
            buffer: []Components.Entity,
        ) usize {
            var current_entity_idx: usize = 0;
            buffer[0] = mem_table_pool.getOneEntity(
                lookup_result.items[0].table_ptr,
                lookup_result.items[0].entities_range[0],
            );

            for (lookup_result.items) |table_res| {
                for (table_res.entities_range[0]..table_res.entities_range[1]) |entity_ptr| {
                    const lookup_entity = mem_table_pool.getOneEntity(table_res.table_ptr, entity_ptr);
                    if (buffer[current_entity_idx].order_id != lookup_entity.order_id and buffer[current_entity_idx].product_id != lookup_entity.product_id) {
                        current_entity_idx += 1;
                        buffer[current_entity_idx] = lookup_entity;
                    }
                }
            }

            current_entity_idx += 1;

            return current_entity_idx;
        }
    };
}
