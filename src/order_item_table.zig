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

const OrderIdIndexPoolType = @import("order_id_index.zig").IndexPoolType;

const EntityType = @import("entities.zig").OrderItem;

pub const MemTablePtr = u32;
pub const MemEntryPtr = u32;

pub fn MemTableType(entities_max_count: MemEntryPtr) type {
    return struct {
        const MemTable = @This();

        entities: std.MultiArrayList(EntityType) = .empty,

        pub fn init(allocator: std.mem.Allocator) !*MemTable {
            var mem_table = try allocator.create(MemTable);
            mem_table.entities = try .initCapacity(allocator, entities_max_count);

            return mem_table;
        }

        pub fn deinit(mem_table: *MemTable, allocator: std.mem.Allocator) void {
            mem_table.entities.deinit(allocator);
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

    const index_field_tags: [2]FieldEntry = blk: {
        var tmp_index_field_tags: [2]FieldEntry = undefined;
        tmp_index_field_tags[0] = std.meta.stringToEnum(FieldEntry, "order_id") orelse unreachable;
        tmp_index_field_tags[1] = std.meta.stringToEnum(FieldEntry, "product_id") orelse unreachable;
        break :blk tmp_index_field_tags;
    };

    return struct {
        const MemTablePool = @This();
        const MemTable = MemTableType(entities_max_count);
        const TableList = []*MemTable;
        const OrderIdIndexPool = OrderIdIndexPoolType(tables_max_count, entities_max_count);

        // Struct Fields
        tables: TableList,
        free_table_ptrs: [tables_max_count]bool,
        filled_table_ptrs: [tables_max_count]bool,
        active_table_ptr: MemTablePtr = 0,
        order_id_index_pool: *OrderIdIndexPool,

        pub fn init(allocator: std.mem.Allocator) !*MemTablePool {
            var mem_table_pool = try allocator.create(MemTablePool);
            mem_table_pool.tables = try allocator.alloc(*MemTable, tables_max_count);
            mem_table_pool.free_table_ptrs = .{true} ** tables_max_count;
            mem_table_pool.filled_table_ptrs = .{false} ** tables_max_count;
            mem_table_pool.active_table_ptr = 0;

           var table_ptr: MemTablePtr = 0;

            while (table_ptr < tables_max_count): (table_ptr += 1) {
                mem_table_pool.tables[table_ptr] = try .init(allocator);
            }

            mem_table_pool.order_id_index_pool = try .init(allocator, .{0, tables_max_count});

            return mem_table_pool;
        }

        pub fn deinit(mem_tables_pool: *MemTablePool, allocator: std.mem.Allocator) void {
            for (mem_tables_pool.tables) |table| {
                table.deinit(allocator);
            }

            allocator.free(mem_tables_pool.tables);
            mem_tables_pool.order_id_index_pool.deinit(allocator);
            allocator.destroy(mem_tables_pool);
        }

        pub fn insert(mem_tables_pool: *MemTablePool, io: std.Io, entities: []*EntityType) !void {
            var entries_start: usize = 0;
            var entries_end: usize = 0;
            //TODO: maybe move syscall for generate time_label to high level
            var init_time_label: u64 = @intCast(std.Io.Clock.awake.now(io).toMilliseconds());

            while (entries_end < entities.len) {
                var active_table = mem_tables_pool.tables[mem_tables_pool.active_table_ptr];

                // Move current active table from free table list
                mem_tables_pool.free_table_ptrs[mem_tables_pool.active_table_ptr] = false;

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
                init_time_label = active_table.insert(init_time_label, toInsert);
                //TODO: Necessary to check the efficiency of this method .slice().items()
                mem_tables_pool.order_id_index_pool.insert(
                    mem_tables_pool.active_table_ptr,
                    entity_count_before_insert,
                    active_table.entities.slice().items(index_field_tags[0]),
                );

                // Если мы заполнили все свободное место
                // значит перемещаем активную таблицу в filled_table_ptrs
                if (rest == entries_end - entries_start) {
                    mem_tables_pool.filled_table_ptrs[mem_tables_pool.active_table_ptr] = true;

                    // Если есть еще свободные таблицы,
                    // тогда смещаем индекс для работы с новой активной таблицой
                    if (mem_tables_pool.active_table_ptr < mem_tables_pool.filled_table_ptrs.len - 1) {
                        mem_tables_pool.active_table_ptr += 1;
                    } else {
                        //TODO full-filled_table_ptrs all mem_tables
                        // Надо придумать механизм работы с перезаполненным пулом
                        // Возможно реализовать ожидание через IO sleep
                        // или принудительно скидывать таблицы на диск и освобождать
                        unreachable;
                    }
                }

                entries_start = entries_end;
            }
        }

        // pub fn find(mem_tables_pool: *MemTablePool, field_name: []const u8, field_value: anytype) !EntityType {
        //     const lookup_value = try mem_tables_pool.index_pool.find(field_name, field_value);

        //     return try mem_tables_pool.tables[lookup_value.table_ptr].find(lookup_value.value_ptr);
        // }

        pub fn calculateFreeTables(mem_tables_pool: *MemTablePool) MemTablePtr {
            var count_tables: MemTablePtr = 0;
            for (mem_tables_pool.free_table_ptrs) |is_free| {
                if (is_free) {
                    count_tables += 1;
                }
            }

            return count_tables;
        }

        pub fn calculateFilledTables(mem_tables_pool: *MemTablePool) MemTablePtr {
            var count_tables: MemTablePtr = 0;
            for (mem_tables_pool.filled_table_ptrs) |is_filled| {
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
        for(input_entries) |entity| {
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
        TestEntity,
        TestEntity.indexes_meta,
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
    const page_cache = 4 * 1024;
    const one_entity_size = @sizeOf(TestEntity);
    const entities_max_count: MemEntryPtr = @intCast(page_cache / one_entity_size);
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
        for(input_entries) |entity| allocator.destroy(entity);
        allocator.free(input_entries);
    }
    const usage_memory_b = entries_total * one_entity_size;
    const usage_memory_mib = usage_memory_b / 1024 / 1024;

    // -------------------

    //==== Insert benchmark ====
    const start_ms = std.Io.Clock.awake.now(io).toMilliseconds();

    try mem_table_pool.insert(io, input_entries);

    const diff_ms = std.Io.Clock.awake.now(io).toMilliseconds() - start_ms;
        std.debug.print(
        \\
        \\benchmark: MemPool insert
        \\  total entities: {d}
        \\  ..........time: {d} ms ({d} s)
        \\  ........memory: {d} bytes (~{d:.2} MiB)
        \\
    ,
        .{
            input_entries.len,
            diff_ms,
            @divTrunc(diff_ms, 1000),
            usage_memory_b,
            usage_memory_mib,
        },
    );
}
