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

//TODO use reflection from entity struct instead of u32
const OrderIdIndex = @import("non_unique_mem_index.zig").NonUniqueMemIndexType(u32);
const ProductIdIndex = @import("non_unique_mem_index.zig").NonUniqueMemIndexType(u32);

const EntityType = @import("entities.zig").OrderItem;

pub const MemTablePtr = u32;
pub const MemEntryPtr = usize;

const UniqueLookupValue = struct {
    table_ptr: MemTablePtr,
    entity_ptr: MemEntryPtr,
};


pub fn MemTableType() type {
    return struct {
        const MemTable = @This();

        start_entity_ptr: MemEntryPtr,
        count_entities: MemEntryPtr,
        
        pub fn init(allocator: std.mem.Allocator, start_entity_ptr: MemEntryPtr, count_entities: MemEntryPtr,) !*MemTable {
            var mem_table = try allocator.create(MemTable);
            mem_table.start_entity_ptr = 

            return mem_table;
        }

        pub fn deinit(mem_table: *MemTable, allocator: std.mem.Allocator) void {
            allocator.destroy(mem_table);
        }

        /// return unique next time_label
        pub fn insert(mem_table: *MemTable, init_time_label: u64, entities: []*EntityType) u64 {
            var time_label = init_time_label;
            for (entities) |entry| {
                entry.time_label = time_label;
                mem_table.entities.appendAssumeCapacity(entry.*);
                time_label += 1;
            }
            return time_label;
        }

        pub fn find(mem_table: *MemTable, entry_ptr: MemEntryPtr) !EntityType {
            return mem_table.entities.get(entry_ptr);
        }
    };
}

pub fn MemTablePoolType(
    comptime tables_max_count: MemTablePtr,
    comptime entities_max_count: u32,
) type {
    const FieldEntry = std.MultiArrayList(EntityType).Field;
    // Reflection
    const IndexFieldTags = struct {
        order_id: FieldEntry,
        product_id: FieldEntry,
    };

    const index_field_tags: IndexFieldTags = .{
        .order_id = std.meta.stringToEnum(FieldEntry, "order_id") orelse unreachable,
        .product_id = std.meta.stringToEnum(FieldEntry, "product_id") orelse unreachable,
    };

    return struct {
        const MemTablePool = @This();
        const MemTable = MemTableType(entities_max_count);
        const TableList = []*MemTable;

        // Struct Fields
        entities: std.MultiArrayList(EntityType) = .empty,
        tables: TableList,
        free_table_ptrs: [tables_max_count]bool,
        filled_table_ptrs: [tables_max_count]bool,
        active_table_ptr: MemTablePtr = 0,
        order_id_index_pool: *OrderIdIndex.IndexPoolType(tables_max_count, entities_max_count),
        product_id_index_pool: *ProductIdIndex.IndexPoolType(tables_max_count, entities_max_count),

        pub fn init(allocator: std.mem.Allocator) !*MemTablePool {
            var mem_table_pool = try allocator.create(MemTablePool);
            mem_table_pool.entities = try .initCapacity(allocator, tables_max_count * entities_max_count);
            mem_table_pool.tables = try allocator.alloc(*MemTable, tables_max_count);
            mem_table_pool.free_table_ptrs = .{true} ** tables_max_count;
            mem_table_pool.filled_table_ptrs = .{false} ** tables_max_count;
            mem_table_pool.active_table_ptr = 0;

            var table_ptr: MemTablePtr = 0;

            while (table_ptr < tables_max_count) : (table_ptr += 1) {
                mem_table_pool.tables[table_ptr] = try .init(allocator);
            }

            mem_table_pool.order_id_index_pool = try .init(allocator, .{ 0, tables_max_count });
            mem_table_pool.product_id_index_pool = try .init(allocator, .{ 0, tables_max_count });

            return mem_table_pool;
        }

        pub fn deinit(table_pool: *MemTablePool, allocator: std.mem.Allocator) void {
            table_pool.entities.deinit(allocator);

            for (table_pool.tables) |table| {
                table.deinit(allocator);
            }

            allocator.free(table_pool.tables);
            table_pool.order_id_index_pool.deinit(allocator);
            table_pool.product_id_index_pool.deinit(allocator);

            allocator.destroy(table_pool);
        }

        pub fn insert(table_pool: *MemTablePool, io: std.Io, entities: []*EntityType) !void {
            var entries_start: usize = 0;
            var entries_end: usize = 0;
            //TODO: maybe move syscall for generate time_label to high level
            var next_time_label: u64 = @intCast(std.Io.Clock.awake.now(io).toMilliseconds());

            while (entries_end < entities.len) {
                var active_table = table_pool.tables[table_pool.active_table_ptr];

                // Move current active table from free table list
                table_pool.free_table_ptrs[table_pool.active_table_ptr] = false;

                // Получаем количество, которое мы можем вставить в активную таблицу
                const rest = active_table.entities.capacity - active_table.entities.len;
                entries_end += rest;

                if (entries_end >= entities.len) {
                    entries_end = entities.len;
                }
                // Order:
                // first step  - insert data
                // second step - insert indexes
                // So, we need save entity count in table for insert indexes before insert data
                const entity_count_before_insert: MemEntryPtr = @intCast(active_table.entities.len);

                const toInsert = entities[entries_start..entries_end];
                next_time_label = active_table.insert(next_time_label, toInsert);
                //TODO: P4 Necessary to check the efficiency of this method .slice().items()
                table_pool.order_id_index_pool.insert(
                    table_pool.active_table_ptr,
                    entity_count_before_insert,
                    active_table.entities.slice().items(index_field_tags.order_id)[entity_count_before_insert..],
                );

                //TODO: P3 need to optimize sort for many records
                // maybe move to high level for one sort call
                table_pool.order_id_index_pool.sort(table_pool.active_table_ptr);

                //TODO: P4 Necessary to check the efficiency of this method .slice().items()
                table_pool.product_id_index_pool.insert(
                    table_pool.active_table_ptr,
                    entity_count_before_insert,
                    active_table.entities.slice().items(index_field_tags.product_id)[entity_count_before_insert..],
                );
                //TODO: P3 need to optimize sort for many records
                // maybe move to high level for one sort call
                table_pool.product_id_index_pool.sort(table_pool.active_table_ptr);

                // Если мы заполнили все свободное место
                // значит перемещаем активную таблицу в filled_table_ptrs
                if (rest == entries_end - entries_start) {
                    table_pool.filled_table_ptrs[table_pool.active_table_ptr] = true;

                    // Если есть еще свободные таблицы,
                    // тогда смещаем индекс для работы с новой активной таблицой
                    if (table_pool.active_table_ptr < table_pool.filled_table_ptrs.len - 1) {
                        table_pool.active_table_ptr += 1;
                    } else {
                        //TODO: P1 full-filled_table_ptrs all mem_tables
                        // Надо придумать механизм работы с перезаполненным пулом
                        // Возможно реализовать ожидание через IO sleep
                        // или принудительно скидывать таблицы на диск и освобождать
                        unreachable;
                    }
                }

                entries_start = entries_end;
            }
        }

        pub fn lookupOneEntity(table_pool: *MemTablePool, table_ptr: MemTablePtr, entity_ptr: MemEntryPtr) *EntityType {
            return table_pool.tables[table_ptr].entities.get(entity_ptr);
        }

        pub fn lookupByOrderId(table_pool: *MemTablePool, key: OrderIdIndex.Key) !*const OrderIdIndex.LookupResult {
            return table_pool.order_id_index_pool.lookup(key);
        }

        pub fn lookupByProductId(table_pool: *MemTablePool, key: ProductIdIndex.Key) !*const OrderIdIndex.LookupResult {
            return table_pool.product_id_index_pool.lookup(key);
        }

        pub fn lookupByOrderIdAndProductId(
            table_pool: *MemTablePool,
            first_key: OrderIdIndex.Key,
            second_key: ProductIdIndex.Key,
        ) !*const EntityType {
            const res_by_first_key = try table_pool.order_id_index_pool.lookup(first_key);

            // TODO: P4 need to optimize lookup by complex index
            // First, we need to evaluate the feasibility of implementing a separate index and its impact on insert performance.

            for (res_by_first_key.items) |res_block| {
                for (res_block.entity_ptr_list) |entity_ptr| {
                    const entity = table_pool.tables[res_block.table_ptr].entities.get(entity_ptr);
                    if (entity.product_id == second_key) return entity;
                }
            }

            return error.NotFound;
        }

        //TODO: P5 move to Test scope
        pub fn calculateFreeTables(table_pool: *MemTablePool) MemTablePtr {
            var count_tables: MemTablePtr = 0;
            for (table_pool.free_table_ptrs) |is_free| {
                if (is_free) {
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
            .order_id = index + 1,
            .product_id = index + 2,
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

    // for (input_entries) |expected_entry| {
    //     const find_entry_by_order_id = try mem_table_pool.find("order_id", expected_entry.order_id);
    //     try testing.expectEqual(expected_entry.product_id, find_entry_by_order_id.product_id);

    //     const find_entry_by_product_id = try mem_table_pool.find("product_id", expected_entry.product_id);
    //     try testing.expectEqual(expected_entry.order_id, find_entry_by_product_id.order_id);
    // }
}

test "MemTablePool: insert repeatable entities" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const entities_max_count = 1;
    const tables_max_count = 6;

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

    const input_entries: []*TestEntity = try allocator.alloc(*TestEntity, entries_total);
    defer allocator.free(input_entries);

    defer {
        for (input_entries) |entry| allocator.destroy(entry);
    }

    input_entries[0] = try allocator.create(TestEntity);
    input_entries[1] = try allocator.create(TestEntity);
    input_entries[2] = try allocator.create(TestEntity);
    input_entries[3] = try allocator.create(TestEntity);
    input_entries[4] = try allocator.create(TestEntity);

    input_entries[0].* = .{
        .order_id = 1,
        .product_id = 10,
    };

    input_entries[1].* = .{
        .order_id = 1,
        .product_id = 20,
    };

    input_entries[2].* = .{
        .order_id = 1,
        .product_id = 30,
    };

    input_entries[3].* = .{
        .order_id = 1,
        .product_id = 40,
    };

    input_entries[4].* = .{
        .order_id = 1,
        .product_id = 50,
    };

    // -------------------

    //==== General test ====

    try mem_table_pool.insert(io, input_entries);
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

}
