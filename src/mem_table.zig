//! Notes:
//! - Порядок создания индексов в IndexPool должен совпадать с порядок создания таблиц в MemTablePool
//! ---- Таким образом мы гарантируем что table_ptr в индексе соответсвует таблице в пулле
//! - Важен порядок вставки entities в MemTable, они должен совпадать с порядком вставки ключей в IndexPool
//! ---- Таким образом мы гарантируем что value_ptr в индексе соответствует entry в таблице

const std = @import("std");
const ArrayList = std.ArrayList;
const assert = std.debug.assert;
const testing = std.testing;

const printObj = @import("utils/debug.zig").printObj;
const stdx_sort = @import("sort.zig");

const EntityType = @import("entities.zig").OrderItem;

pub const MemTablePtr = u32;
pub const MemEntryPtr = usize;

pub const Entities = std.MultiArrayList(EntityType);

const SortCtx = struct {
    entities: *Entities,
    pub fn lessThan(ctx: @This(), a_index: usize, b_index: usize) bool {
        const a = ctx.entities.get(a_index);
        const b = ctx.entities.get(b_index);

        if (a.order_id != b.order_id) return a.order_id > b.order_id;

        if (a.product_id != b.product_id) return a.product_id > b.product_id;

        return a.time_label > b.time_label;
    }
};

//TODO P1 move to entities
const FieldEntry = std.MultiArrayList(EntityType).Field;
//TODO P1 move to entities
// Reflection
const IndexFieldTags = struct {
    order_id: FieldEntry,
    product_id: FieldEntry,
    time_label: FieldEntry,
};
//TODO P1 move to entities
const index_field_tags: IndexFieldTags = .{
    .order_id = std.meta.stringToEnum(FieldEntry, "order_id") orelse unreachable,
    .product_id = std.meta.stringToEnum(FieldEntry, "product_id") orelse unreachable,
    .time_label = std.meta.stringToEnum(FieldEntry, "time_label") orelse unreachable,
};

const EntitiesRange = struct { usize, usize };

pub const TableLookupResult = struct {
    table_ptr: MemTablePtr,
    entities_range: EntitiesRange,
};
pub const LookupResult = std.ArrayList(TableLookupResult);

pub const InsertState = enum {
    ReadyToNext,
    NeedToFlush,
    Overflow,
};

pub const MetaMemTable = struct {
    min_order_id: EntityType.OrderId,
    max_order_id: EntityType.OrderId,
    min_product_id: EntityType.ProductId,
    max_product_id: EntityType.ProductId,
};

pub fn MemTableType(entities_max_count: MemEntryPtr) type {
    return struct {
        const MemTable = @This();

        //TODO: Research the feasibility of moving entities into a pool
        // and storing only pointers to a shared buffer in the table

        entities: *Entities,
        primary_sorted: bool,
        meta: MetaMemTable,

        pub fn init(allocator: std.mem.Allocator) !*MemTable {
            const mem_table = try allocator.create(MemTable);

            mem_table.* = .{
                .primary_sorted = false,
                .meta = .{
                    .max_order_id = 0,
                    .min_order_id = 0,
                    .min_product_id = 0,
                    .max_product_id = 0,
                },
                .entities = try allocator.create(Entities),
            };

            mem_table.entities.* = try .initCapacity(allocator, entities_max_count);
            return mem_table;
        }

        pub fn deinit(mem_table: *MemTable, allocator: std.mem.Allocator) void {
            mem_table.entities.deinit(allocator);
            allocator.destroy(mem_table.entities);

            allocator.destroy(mem_table);
        }

        /// return unique next time_label
        pub fn insert(mem_table: *MemTable, init_time_label: u64, entities: []*EntityType) u64 {
            mem_table.primary_sorted = false;

            if (mem_table.entities.len == 0) {
                mem_table.meta.min_order_id = entities[0].order_id;
                mem_table.meta.max_order_id = entities[0].order_id;
                mem_table.meta.min_product_id = entities[0].product_id;
                mem_table.meta.max_product_id = entities[0].product_id;
            }

            var time_label = init_time_label;
            for (entities) |entity| {
                if (entity.order_id < mem_table.meta.min_order_id) {
                    mem_table.meta.min_order_id = entity.order_id;
                } else if (entity.order_id > mem_table.meta.max_order_id) {
                    mem_table.meta.max_order_id = entity.order_id;
                }

                if (entity.product_id < mem_table.meta.min_product_id) {
                    mem_table.meta.min_product_id = entity.product_id;
                } else if (entity.product_id > mem_table.meta.max_product_id) {
                    mem_table.meta.max_product_id = entity.product_id;
                }
                entity.time_label = time_label;
                mem_table.entities.appendAssumeCapacity(entity.*);
                time_label += 1;
            }
            return time_label;
        }

        pub fn primarySort(mem_table: *MemTable) void {
            if (mem_table.primary_sorted) return;

            mem_table.entities.sortUnstable(SortCtx{ .entities = mem_table.entities });
            mem_table.primary_sorted = true;
        }

        pub fn lookupByOrderId(mem_table: *MemTable, key: EntityType.OrderId) !EntitiesRange {
            mem_table.primarySort();
            
            const range = stdx_sort.equalRangeDesc(
                EntityType.OrderId,
                mem_table.entities.slice().items(index_field_tags.order_id),
                key,
                stdx_sort.compareNumberKeys(EntityType.OrderId),
            );

            if (range[1] == 0) return error.NotFound;

            return range;
        }

        pub fn lookupByProductId(mem_table: *MemTable, key: EntityType.ProductId) !EntitiesRange {
            if (! mem_table.primary_sorted) {
                printObj("Sort in lookupByProductId", key);
                mem_table.primarySort();
            }

            const range = stdx_sort.equalRangeDesc(
                EntityType.ProductId,
                mem_table.entities.slice().items(index_field_tags.product_id),
                key,
                stdx_sort.compareNumberKeys(EntityType.ProductId),
            );

            if (range[1] == 0) return error.NotFound;

            return range;
        }

           pub fn clear(mem_table: *MemTable) void {
            mem_table.meta.max_order_id = 0;
            mem_table.meta.min_order_id = 0;
            mem_table.meta.max_product_id = 0;
            mem_table.meta.min_product_id = 0;
            mem_table.primary_sorted = false;
            mem_table.entities.clearRetainingCapacity();

    }

    pub fn getMeta(mem_table: *MemTable) MetaMemTable {
        return mem_table.meta;
    }

    
    pub fn compareByOrderIdAndProductId(first: EntityType, second: EntityType) std.math.Order {
        if (first.order_id < second.order_id) return .lt;
        if (first.order_id > second.order_id) return .gt;
        if (first.order_id == second.order_id) {
            if (first.product_id == second.product_id) return .eq;
            if (first.product_id < second.product_id) return .lt;
            if (first.product_id > second.product_id) return .gt;
        }
        unreachable;
    }

 
    };
}

pub fn MemTablePoolType(
    comptime tables_max_count: MemTablePtr,
    comptime entities_max_count: u32,
) type {
    return struct {
        const MemTablePool = @This();
        pub const MemTable = MemTableType(entities_max_count);
        pub const TableList = []*MemTable;

        // Struct Fields
        tables: TableList,
        filled_table_ptrs: [tables_max_count]bool,
        active_table_ptr: MemTablePtr = 0,
        lookup_result: *LookupResult,

        pub fn init(allocator: std.mem.Allocator) !*MemTablePool {
            var mem_table_pool = try allocator.create(MemTablePool);
            mem_table_pool.* = .{
                .tables = try allocator.alloc(*MemTable, tables_max_count),
                .filled_table_ptrs = .{false} ** tables_max_count,
                .active_table_ptr = 0,
                .lookup_result = try allocator.create(LookupResult),
            };

            mem_table_pool.lookup_result.* = try .initCapacity(allocator, tables_max_count);
            var table_ptr: MemTablePtr = 0;

            while (table_ptr < tables_max_count) : (table_ptr += 1) {
                mem_table_pool.tables[table_ptr] = try .init(allocator);
            }

            return mem_table_pool;
        }

        pub fn deinit(table_pool: *MemTablePool, allocator: std.mem.Allocator) void {
            table_pool.lookup_result.deinit(allocator);
            allocator.destroy(table_pool.lookup_result);

            for (table_pool.tables) |table| {
                table.deinit(allocator);
            }

            allocator.free(table_pool.tables);
            allocator.destroy(table_pool);
        }

        pub fn insert(table_pool: *MemTablePool, io: std.Io, entities: []*EntityType) !usize {
            var entries_start: usize = 0;
            var entries_end: usize = 0;
            //TODO: P5 maybe move syscall for generate time_label to high level
            var next_time_label: u64 = @intCast(std.Io.Clock.awake.now(io).toMilliseconds());

            var active_table = table_pool.tables[table_pool.active_table_ptr];
            var attempts: usize = 0;

            while (entries_end < entities.len) {
                //TODO: P5 need to research limit (maybe trigger real error in release mode)
                attempts += 1;
                assert(attempts < 50);

                // Получаем количество, которое мы можем вставить в активную таблицу
                const rest = active_table.entities.capacity - active_table.entities.len;
                entries_end += rest;

                if (entries_end >= entities.len) {
                    entries_end = entities.len;
                }
        
                const to_insert = entities[entries_start..entries_end];
                next_time_label = active_table.insert(next_time_label, to_insert);

                // Если мы заполнили все свободное место
                // значит перемещаем активную таблицу в filled_table_ptrs
                if (rest == to_insert.len) {
                    active_table.primarySort();

                    table_pool.filled_table_ptrs[table_pool.active_table_ptr] = true;

                    // Если есть еще свободные таблицы,
                    // тогда смещаем индекс для работы с новой активной таблицой
                    if (table_pool.active_table_ptr < table_pool.filled_table_ptrs.len - 1) {
                        table_pool.active_table_ptr += 1;
                        active_table = table_pool.tables[table_pool.active_table_ptr];
                    } else {
                        //Back to start of ring and check table is filled
                        if (table_pool.filled_table_ptrs[0]) {
                            return entries_end;
                        }

                        table_pool.active_table_ptr = 0;
                        active_table = table_pool.tables[table_pool.active_table_ptr];
                    }
                }

                entries_start = entries_end;

            }

            return entries_end;
        }

        pub fn getOne(
            table_pool: *MemTablePool,
            table_ptr: MemTablePtr,
            entity_ptr: MemEntryPtr,
        ) EntityType {
            return table_pool.tables[table_ptr].entities.get(entity_ptr);
        }

        pub fn lookupByOrderId(table_pool: *MemTablePool, key: EntityType.OrderId) !*const LookupResult {
            assert(key != 0);

            //TODO: P3 need to check how we can clear result not before each lookup, but after this
            table_pool.lookup_result.clearRetainingCapacity();

            var last_indx: MemTablePtr = @intCast(table_pool.tables.len);

            while (last_indx > 0) {
                last_indx -= 1;
                if (key >= table_pool.tables[last_indx].min_order_id and key <= table_pool.tables[last_indx].max_order_id) {

                    const entities_range = table_pool.tables[last_indx].lookupByOrderId(key) catch continue;

                    table_pool.lookup_result.appendAssumeCapacity(.{
                        .table_ptr = last_indx,
                        .entities_range = entities_range,
                    });
                }
            }

            if (table_pool.lookup_result.items.len > 0) return table_pool.lookup_result;

            return error.NotFound;
        }


        pub fn lookupByProductId(table_pool: *MemTablePool, key: EntityType.ProductId) !*const LookupResult {
            assert(key != 0);

            //TODO: P3 need to check how we can clear result not before each lookup, but after this
            table_pool.lookup_result.clearRetainingCapacity();

            var last_indx: MemTablePtr = @intCast(table_pool.tables.len);

            while (last_indx > 0) {
                last_indx -= 1;
         
                if (key >= table_pool.tables[last_indx].min_product_id and key <= table_pool.tables[last_indx].max_product_id) {

                    const entities_range = table_pool.tables[last_indx].lookupByProductId(key) catch unreachable;

                    table_pool.lookup_result.appendAssumeCapacity(.{
                        .table_ptr = last_indx,
                        .entities_range = entities_range,
                    });
                }
            }

            if (table_pool.lookup_result.items.len > 0) return table_pool.lookup_result;

            return error.NotFound;
        }


    //    pub fn lookupLastByOrderIdAndProductId(table_pool: *MemTablePool, order_id: EntityType.OrderId, product_id: EntityType.ProductId) !EntityType {
    //         assert(order_id != 0);
    //         assert(product_id != 0);

    //         var last_indx = table_pool.active_table_ptr;

    //         while (last_indx > 0) {
    //             last_indx -= 1;
         
    //             if (
    //                 order_id >= table_pool.tables[last_indx].min_order_id and
    //                 order_id <= table_pool.tables[last_indx].max_order_id and 
    //                 product_id >= table_pool.tables[last_indx].min_product_id and 
    //                 product_id <= table_pool.tables[last_indx].max_product_id 
    //             ) {

    //             }
    //         }

    //         if (table_pool.lookup_result.items.len > 0) return table_pool.lookup_result;

    //         return error.NotFound;
    //     }

        pub fn getLastEntities(
            mem_table_pool: *MemTablePool,
            lookup_result: *const LookupResult,
            buffer: []EntityType,
        ) usize {
            var current_entity_idx: usize = 0;
            buffer[0] = mem_table_pool.getOne(lookup_result.items[0].table_ptr,lookup_result.items[0].entities_range[0],);

            for (lookup_result.items) |table_res| {
                for (table_res.entities_range[0]..table_res.entities_range[1]) |entity_ptr| {
                    const lookup_entity = mem_table_pool.getOne(table_res.table_ptr, entity_ptr);
                    if (buffer[current_entity_idx].order_id != lookup_entity.order_id and buffer[current_entity_idx].product_id != lookup_entity.product_id) {
                        current_entity_idx += 1;
                        buffer[current_entity_idx] = lookup_entity;
                    }
                }
            }

            current_entity_idx += 1;

            return current_entity_idx;
        }

        pub fn freeFilledTables(table_pool: *MemTablePool) void {
            inline for (table_pool.filled_table_ptrs, 0..) |is_filled, table_ptr| {
                if (is_filled) {
                    table_pool.filled_table_ptrs[table_ptr] = false;
                    table_pool.tables[table_ptr].clear();
                }
            }
        }

        //TODO: P5 move to Test scope
        pub fn calculateFreeTables(table_pool: *MemTablePool) MemTablePtr {
            var count_tables: MemTablePtr = 0;
            for (table_pool.filled_table_ptrs) |is_filled| {
                if (! is_filled) {
                    count_tables += 1;
                }
            }

            return count_tables;
        }

        //TODO: P5 move to Test scope
        pub fn calculateFilledTables(table_pool: *MemTablePool) MemTablePtr {
            var count_tables: MemTablePtr = 0;
            for (table_pool.filled_table_ptrs) |is_filled| {
                if (is_filled) {
                    count_tables += 1;
                }
            }

            return count_tables;
        }

        pub fn gen(comptime T: type, val: T) T {
            return val;
        }
    };
}

// ==== Testing ====

const TestEntity = @import("entities.zig").OrderItem;

fn testPreparingUniqueEntries(allocator: std.mem.Allocator, entries_total: usize) ![]*TestEntity {
    var input_entries: []*TestEntity = try allocator.alloc(*TestEntity, entries_total);
    errdefer allocator.free(input_entries);

    var index: MemEntryPtr = 0;
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

test "MemTablePool: (max count entities for all tables in pool) - 1" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const entities_max_count = 8;
    const tables_max_count = 2;
    const MemTablePool = MemTablePoolType(
        tables_max_count,
        entities_max_count,
    );
    var mem_table_pool: *MemTablePool = try .init(allocator);
    defer mem_table_pool.deinit(allocator);

    // Preparing input data

    // Максимальное количество entities,
    // которое может вместить весь pool минус 1,
    // чтобы не заполнить все таблицы
    const entries_total = entities_max_count * tables_max_count - 1;

    const input_entries = try testPreparingUniqueEntries(allocator, entries_total);
    defer {
        for (input_entries) |entry| allocator.destroy(entry);
        allocator.free(input_entries);
    }

    // -------------------

    //==== General test ====

    try mem_table_pool.insert(io, input_entries);

    const count_filled_tables = mem_table_pool.calculateFilledTables();
    const count_free_tables = mem_table_pool.calculateFreeTables();

    try testing.expectEqual(tables_max_count - 1, count_filled_tables);
    try testing.expectEqual(tables_max_count - 1 - count_filled_tables, count_free_tables);
}

test "MemTablePool: lookupByOrderId" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const entities_max_count = 3;
    const tables_max_count = 10;

    const MemTablePool = MemTablePoolType(
        tables_max_count,
        entities_max_count,
    );

    var mem_table_pool: *MemTablePool = try .init(allocator);
    defer mem_table_pool.deinit(allocator);

    // Preparing input data

    const entries_total = 7;

    const input_entries: []*TestEntity = try allocator.alloc(*TestEntity, entries_total);
    defer allocator.free(input_entries);

    defer {
        for (input_entries) |entry| allocator.destroy(entry);
    }

    var entry_ptr: MemEntryPtr = 0;
    while (entry_ptr < entries_total) : (entry_ptr += 1) {
        input_entries[entry_ptr] = try allocator.create(TestEntity);
    }

    input_entries[0].* = .{
        .order_id = 1,
        .product_id = 10,
        .quantity = 1,
    };

    input_entries[1].* = .{
        .order_id = 2,
        .product_id = 10,
        .quantity = 2,
    };

    input_entries[2].* = .{
        .order_id = 1,
        .product_id = 10,
        .quantity = 3,
    };

    input_entries[3].* = .{
        .order_id = 1,
        .product_id = 20,
        .quantity = 4,
    };

    input_entries[4].* = .{
        .order_id = 1,
        .product_id = 10,
        .quantity = 5,
    };

    input_entries[5].* = .{
        .order_id = 1,
        .product_id = 20,
        .quantity = 6,
    };

    input_entries[6].* = .{
        .order_id = 2,
        .product_id = 10,
        .quantity = 7,
    };

    const new_size: u32 = @intCast(input_entries.len);

    var exptected_map: std.AutoHashMapUnmanaged(struct { MemTablePtr, MemEntryPtr }, *EntityType) = .empty;
    try exptected_map.ensureTotalCapacity(allocator, new_size);
    defer exptected_map.deinit(allocator);

    for (input_entries) |entry| {
        exptected_map.putAssumeCapacity(.{ entry.order_id, entry.product_id }, entry);
    }
    // -------------------

    //==== General test ====

    try mem_table_pool.insert(io, input_entries);
 
    for (input_entries) |input_entry| {
        var buffer_lookups: [100]EntityType = undefined;

        const lookup_result = try mem_table_pool.lookupByOrderId(input_entry.order_id);
        const entities_count = mem_table_pool.getLastEntities(lookup_result, &buffer_lookups);

        for(buffer_lookups[0..entities_count]) |lookup_entity| {
            const exptected_entity = exptected_map.get(.{ lookup_entity.order_id, lookup_entity.product_id }) orelse unreachable;   
            try testing.expectEqual(exptected_entity.order_id, lookup_entity.order_id);
            try testing.expectEqual(exptected_entity.product_id, lookup_entity.product_id);
            try testing.expectEqual(exptected_entity.quantity, lookup_entity.quantity);
        }
    }
}

test "MemTablePool: lookupByProductId" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const entities_max_count = 3;
    const tables_max_count = 10;

    const MemTablePool = MemTablePoolType(
        tables_max_count,
        entities_max_count,
    );

    var mem_table_pool: *MemTablePool = try .init(allocator);
    defer mem_table_pool.deinit(allocator);

    // Preparing input data

    const entries_total = 7;

    const input_entries: []*TestEntity = try allocator.alloc(*TestEntity, entries_total);
    defer allocator.free(input_entries);

    defer {
        for (input_entries) |entry| allocator.destroy(entry);
    }

    var entry_ptr: MemEntryPtr = 0;
    while (entry_ptr < entries_total) : (entry_ptr += 1) {
        input_entries[entry_ptr] = try allocator.create(TestEntity);
    }

    input_entries[0].* = .{
        .order_id = 1,
        .product_id = 10,
        .quantity = 1,
    };

    input_entries[1].* = .{
        .order_id = 2,
        .product_id = 10,
        .quantity = 2,
    };

    input_entries[2].* = .{
        .order_id = 1,
        .product_id = 10,
        .quantity = 3,
    };

    input_entries[3].* = .{
        .order_id = 1,
        .product_id = 20,
        .quantity = 4,
    };

    input_entries[4].* = .{
        .order_id = 1,
        .product_id = 10,
        .quantity = 5,
    };

    input_entries[5].* = .{
        .order_id = 1,
        .product_id = 20,
        .quantity = 6,
    };

    input_entries[6].* = .{
        .order_id = 2,
        .product_id = 10,
        .quantity = 7,
    };

    const new_size: u32 = @intCast(input_entries.len);

    var exptected_map: std.AutoHashMapUnmanaged(struct { MemTablePtr, MemEntryPtr }, *EntityType) = .empty;
    try exptected_map.ensureTotalCapacity(allocator, new_size);
    defer exptected_map.deinit(allocator);

    for (input_entries) |entry| {
        exptected_map.putAssumeCapacity(.{ entry.order_id, entry.product_id }, entry);
    }
    // -------------------

    //==== General test ====

    try mem_table_pool.insert(io, input_entries);

    for (input_entries) |input_entry| {
        var buffer_lookups: [100]EntityType = undefined;

        const lookup_result = try mem_table_pool.lookupByProductId(input_entry.product_id);
        const entities_count = mem_table_pool.getLastEntities(lookup_result, &buffer_lookups);

        for(buffer_lookups[0..entities_count]) |lookup_entity| {
            const exptected_entity = exptected_map.get(.{ lookup_entity.order_id, lookup_entity.product_id }) orelse unreachable;   
            try testing.expectEqual(exptected_entity.order_id, lookup_entity.order_id);
            try testing.expectEqual(exptected_entity.product_id, lookup_entity.product_id);
            try testing.expectEqual(exptected_entity.quantity, lookup_entity.quantity);
        }
    }
}

test "benchmark MemTablePool" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const page_cache = 4 * 1024; //4 Kib
    const one_table_memory = page_cache;
    const one_entity_size = @sizeOf(TestEntity);
    const entities_max_count: MemEntryPtr = @intCast(one_table_memory / one_entity_size);
    const filled_tables_count: MemTablePtr = 10_000;
    const tables_max_count: MemTablePtr = filled_tables_count + 1;

    const MemTablePool = MemTablePoolType(
        tables_max_count,
        entities_max_count,
    );
    var mem_table_pool: *MemTablePool = try .init(allocator);
    defer mem_table_pool.deinit(allocator);

    // Preparing data
    const entries_total: usize = @intCast(entities_max_count * filled_tables_count);
    const input_entries = try testPreparingUniqueEntries(allocator, entries_total);
    defer {
        for (input_entries) |entity| allocator.destroy(entity);
        allocator.free(input_entries);
    }

    const usage_memory_b = entries_total * one_entity_size;
    const usage_memory_mib = usage_memory_b / 1024 / 1024;

    // -------------------

    //==== Insert benchmark ====
    const insert_batch = 20;
    var start_ms = std.Io.Clock.awake.now(io).toMilliseconds();

    var start_idx: usize = 0;
    var end_idx: usize = 0;
    while (start_idx < input_entries.len) : (start_idx += insert_batch) {
        end_idx += insert_batch;
        // Check out of bound
        if (end_idx >= input_entries.len) {
            end_idx = input_entries.len;
        }
        const to_insert = input_entries[start_idx..end_idx];
        try mem_table_pool.insert(io, to_insert);
    }

    var diff_ms = std.Io.Clock.awake.now(io).toMilliseconds() - start_ms;
    std.debug.print(
        \\
        \\benchmark: MemPool insert
        \\  .....mem tables: {d}
        \\  .total entities: {d}
        \\  ...insert batch: {d}
        \\  .....total time: {d} ms ({d} s)
        \\  ...total memory: {d} bytes (~{d:.2} MiB)
        \\
    ,
        .{
            filled_tables_count,
            input_entries.len,
            insert_batch,
            diff_ms,
            @divTrunc(diff_ms, 1000),
            usage_memory_b,
            usage_memory_mib,
        },
    );

    const lookups_total = input_entries.len / 16;

    assert(lookups_total <= input_entries.len);
    // ==== lookupByOrderId benchmark ====

    start_ms = std.Io.Clock.awake.now(io).toMilliseconds();
    for (input_entries[0..lookups_total]) |expected_entry| {
        _ = try mem_table_pool.lookupByOrderId(expected_entry.order_id);
        // TODO P2 add testing expect
    }

    diff_ms = std.Io.Clock.awake.now(io).toMilliseconds() - start_ms;

    std.debug.print(
        \\
        \\
        \\benchmark: MemPool lookupByOrderId
        \\  .....mem tables: {d}
        \\  .total entities: {d}
        \\  ..total_lookups: {d}
        \\  .....total time: {d} ms ({d} s)
        \\  ...total memory: {d} bytes (~{d:.2} MiB)
        \\
    ,
        .{
            filled_tables_count,
            input_entries.len,
            lookups_total,
            diff_ms,
            @divTrunc(diff_ms, 1000),
            usage_memory_b,
            usage_memory_mib,
        },
    );

    start_ms = std.Io.Clock.awake.now(io).toMilliseconds();
    for (input_entries[0..lookups_total]) |expected_entry| {
        _ = try mem_table_pool.lookupByProductId(expected_entry.product_id);
        // TODO P2 add testing expect
    }

    diff_ms = std.Io.Clock.awake.now(io).toMilliseconds() - start_ms;

    std.debug.print(
        \\
        \\
        \\benchmark: MemPool lookupByProductId
        \\  .....mem tables: {d}
        \\  .total entities: {d}
        \\  ..total_lookups: {d}
        \\  .....total time: {d} ms ({d} s)
        \\  ...total memory: {d} bytes (~{d:.2} MiB)
        \\
    ,
        .{
            filled_tables_count,
            input_entries.len,
            lookups_total,
            diff_ms,
            @divTrunc(diff_ms, 1000),
            usage_memory_b,
            usage_memory_mib,
        },
    );

    // start_ms = std.Io.Clock.awake.now(io).toMilliseconds();
    // for (input_entries[0..lookups_total]) |expected_entry| {
    //     _ = try mem_table_pool.lookupByOrderIdAndProductId(expected_entry.order_id, expected_entry.product_id);
    //     // TODO P2 add testing expect
    // }

    // diff_ms = std.Io.Clock.awake.now(io).toMilliseconds() - start_ms;

    // std.debug.print(
    //     \\
    //     \\
    //     \\benchmark: MemPool lookupByOrderIdAndProductId
    //     \\  .....mem tables: {d}
    //     \\  .total entities: {d}
    //     \\  ..total_lookups: {d}
    //     \\  .....total time: {d} ms ({d} s)
    //     \\  ...total memory: {d} bytes (~{d:.2} MiB)
    //     \\
    // ,
    //     .{
    //         filled_tables_count,
    //         input_entries.len,
    //         lookups_total,
    //         diff_ms,
    //         @divTrunc(diff_ms, 1000),
    //         usage_memory_b,
    //         usage_memory_mib,
    //     },
    // );
}
