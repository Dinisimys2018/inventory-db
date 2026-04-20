//! Notes:
//! - Порядок создания индексов в IndexPool должен совпадать с порядок создания таблиц в MemTablePool
//! ---- Таким образом мы гарантируем что table_ptr в индексе соответсвует таблице в пулле
//! - Важен порядок вставки entries в MemTable, они должен совпадать с порядком вставки ключей в IndexPool
//! ---- Таким образом мы гарантируем что value_ptr в индексе соответствует entry в таблице

const std = @import("std");
const ArrayList = std.ArrayList;
const assert = std.debug.assert;
const testing = std.testing;

const printObj = @import("utils/debug.zig").printObj;

const IndexPoolType = @import("index_table.zig").IndexPoolType;

pub const MemTablePtr = u32;
pub const MemEntryPtr = u32;
// TODO: возможно MemTableType можно перенести внутри MemTablePoolType
pub fn MemTableType(comptime EntryType: type, comptime entries_max_count: MemEntryPtr) type {
    if (!@hasField(EntryType, "time_label")) {
        @compileError("EntryType must have time_label field");
    }

    return struct {
        const MemTable = @This();

        entries: std.MultiArrayList(EntryType) = .empty,

        pub fn init(allocator: std.mem.Allocator) !*MemTable {
            var mem_table = try allocator.create(MemTable);
            mem_table.entries = try .initCapacity(allocator, entries_max_count);

            return mem_table;
        }

        pub fn deinit(mem_table: *MemTable, allocator: std.mem.Allocator) void {
            mem_table.entries.deinit(allocator);
            allocator.destroy(mem_table);
        }

        fn rdtsc() u64 {
            var lo: u32 = undefined;
            var hi: u32 = undefined;
            asm volatile ("rdtsc"
                : [lo] "={eax}" (lo),
                  [hi] "={edx}" (hi),
            );
            return (@as(u64, hi) << 32) | lo;
        }

        pub fn insert(mem_table: *MemTable, io: std.Io, entries: []*EntryType) void {
            const time_label: u64 = @intCast(std.Io.Clock.awake.now(io).toMilliseconds());

            for (entries) |entry| {
                entry.time_label = time_label;
                mem_table.entries.appendAssumeCapacity(entry.*);
            }
        }

        pub fn find(mem_table: *MemTable, entry_ptr: MemEntryPtr) !EntryType {
            return mem_table.entries.get(entry_ptr);
        }
    };
}

pub fn MemTablePoolType(
    comptime EntryType: type,
    comptime indexes_meta: anytype,
    comptime mem_tables_max_count: MemTablePtr,
    comptime entries_max_count: u32,
) type {
    return struct {
        const FieldEntry = std.MultiArrayList(EntryType).Field;
        const entry_field_tags: [indexes_meta.len]FieldEntry = blk: {
            var tmp_entry_field_tags: [indexes_meta.len]FieldEntry = undefined;
            var i: usize = 0;
            while (i < indexes_meta.len) : (i += 1) {
                tmp_entry_field_tags[i] = std.meta.stringToEnum(FieldEntry, indexes_meta[i].field_name).?;
            }
            break :blk tmp_entry_field_tags;
        };

        const MemTablePool = @This();
        const MemTable = MemTableType(EntryType, entries_max_count);
        const TableList = []*MemTable;
        const MemIndexPool = IndexPoolType(
            MemTablePtr,
            MemEntryPtr,
            indexes_meta,
            mem_tables_max_count,
            entries_max_count,
        );

        // Struct Fields
        tables: TableList,
        free_table_ptrs: [mem_tables_max_count]bool,
        filled_table_ptrs: [mem_tables_max_count]bool,
        active_table_ptr: MemTablePtr = 0,
        index_pool: *MemIndexPool,

        pub fn init(allocator: std.mem.Allocator) !*MemTablePool {
            var mem_table_pool = try allocator.create(MemTablePool);
            mem_table_pool.tables = try allocator.alloc(*MemTable, mem_tables_max_count);
            mem_table_pool.free_table_ptrs = .{true} ** mem_tables_max_count;
            mem_table_pool.filled_table_ptrs = .{false} ** mem_tables_max_count;
            mem_table_pool.active_table_ptr = 0;
            mem_table_pool.index_pool = try .init(allocator);

            var mem_table_ptr: MemTablePtr = 0;

            while (mem_table_ptr < mem_tables_max_count) : (mem_table_ptr += 1) {
                mem_table_pool.tables[mem_table_ptr] = try .init(allocator);
            }

            return mem_table_pool;
        }

        pub fn deinit(mem_tables_pool: *MemTablePool, allocator: std.mem.Allocator) void {
            for (mem_tables_pool.tables) |table| {
                table.deinit(allocator);
            }
            allocator.free(mem_tables_pool.tables);
            mem_tables_pool.index_pool.deinit(allocator);
            allocator.destroy(mem_tables_pool);
        }

        pub fn insert(mem_tables_pool: *MemTablePool, io: std.Io, entries: []*EntryType) !void {
            var entries_start: usize = 0;
            var entries_end: usize = 0;

            while (entries_end < entries.len) {
                // Сразу убираем активную таблицу из свободных,
                // чтобы другие вызовы не получили доступ к ней
                mem_tables_pool.free_table_ptrs[mem_tables_pool.active_table_ptr] = false;

                var active_table = mem_tables_pool.tables[mem_tables_pool.active_table_ptr];

                // Получаем количество, которое мы можем вставить в активную таблицу
                const rest = active_table.entries.capacity - active_table.entries.len;
                entries_end += rest;

                // Контролируем границу
                if (entries_end >= entries.len) {
                    entries_end = entries.len;
                }
                const toInsert = entries[entries_start..entries_end];
                active_table.insert(io, toInsert);

                comptime var field_meta_index: u8 = 0;

                inline while (field_meta_index < indexes_meta.len) : (field_meta_index += 1) {
                    //TODO: Necessary to check the efficiency of this method .slice().items()
                    try mem_tables_pool.index_pool.insert(
                        mem_tables_pool.active_table_ptr,
                        indexes_meta[field_meta_index].field_name,
                        active_table.entries.slice().items(entry_field_tags[field_meta_index]),
                    );
                }

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

        pub fn find(mem_tables_pool: *MemTablePool, field_name: []const u8, field_value: anytype) !EntryType {
            const lookup_value = try mem_tables_pool.index_pool.find(field_name, field_value);

            return try mem_tables_pool.tables[lookup_value.table_ptr].find(lookup_value.value_ptr);
        }

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
const EntityFieldIndexListType = @import("index_table.zig").EntityFieldIndexListType;
const fields_count = 2;

const TestEntity = struct {
    pub const OrderId = u32;
    pub const ProductId = u32;

    time_label: u64 = 0,
    order_id: OrderId,
    product_id: ProductId,

    pub const IndexesMeta = EntityFieldIndexListType(fields_count);

    pub const indexes_meta: IndexesMeta = .{
        .{
            .field_name = "order_id",
            .index_strategy = .indexes_u32,
        },
        .{
            .field_name = "product_id",
            .index_strategy = .indexes_u32,
        },
    };
};

fn testPreparingUniqueEntries(allocator: std.mem.Allocator, entries_total: usize) ![]*TestEntity {
    var input_entries: []*TestEntity = try allocator.alloc(*TestEntity, entries_total);
    var index: MemEntryPtr = 0;

    while (index < entries_total) : (index += 1) {
        const entity = try allocator.create(TestEntity);
        entity.* = .{
            .order_id = index + 1,
            .product_id = index + 2,
        };
        input_entries[index] = entity;
    }

    return input_entries;
}

test "MemTablePool: (max count entries for all tables in pool) - 1" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const entries_max_count = 5;
    const mem_tables_max_count = 5;
    const MemTablePool = MemTablePoolType(
        TestEntity,
        TestEntity.indexes_meta,
        mem_tables_max_count,
        entries_max_count,
    );
    var mem_table_pool: *MemTablePool = try .init(allocator);
    defer mem_table_pool.deinit(allocator);

    // Preparing input data

    // Максимальное количество entries,
    // которое может вместить весь pool минус 1,
    // чтобы не заполнить все таблицы
    const entries_total = entries_max_count * mem_tables_max_count - 1;

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

    try testing.expectEqual(mem_tables_max_count - 1, count_filled_tables);
    try testing.expectEqual(mem_tables_max_count - 1 - count_filled_tables, count_free_tables);

    for (input_entries) |expected_entry| {
        const find_entry_by_order_id = try mem_table_pool.find("order_id", expected_entry.order_id);
        try testing.expectEqual(expected_entry.product_id, find_entry_by_order_id.product_id);

        const find_entry_by_product_id = try mem_table_pool.find("product_id", expected_entry.product_id);
        try testing.expectEqual(expected_entry.order_id, find_entry_by_product_id.order_id);
    }
}

test "MemTablePool: insert repeatable entries" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const entries_max_count = 1;
    const mem_tables_max_count = 6;
    const MemTablePool = MemTablePoolType(
        TestEntity,
        TestEntity.indexes_meta,
        mem_tables_max_count,
        entries_max_count,
    );
    var mem_table_pool: *MemTablePool = try .init(allocator);
    defer mem_table_pool.deinit(allocator);

    // Preparing input data

    // Максимальное количество entries,
    // которое может вместить весь pool минус 1,
    // чтобы не заполнить все таблицы
    const entries_total = entries_max_count * mem_tables_max_count - 1;

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

    printObj("search", mem_table_pool.find("order_id", 1));
}

//TODO: chenge benchmark
test "benchmark MemPool" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const usage_memory_mib: usize = 100;
    const usage_memory_bytes: usize = usage_memory_mib * 1024 * 1024;
    const mem_tables_max_count: MemTablePtr = 5;
    const filled_tables_count: usize = 3;
    const entries_max_count: MemEntryPtr = @intCast(usage_memory_bytes / filled_tables_count / @sizeOf(TestEntity)); //100MiB

    const MemTablePool = MemTablePoolType(
        TestEntity,
        TestEntity.indexes_meta,
        mem_tables_max_count,
        entries_max_count,
    );

    var mem_table_pool: *MemTablePool = try .init(allocator);
    defer mem_table_pool.deinit(allocator);

    // Preparing data
    const entries_total = entries_max_count * filled_tables_count;
    const input_entries = try testPreparingUniqueEntries(allocator, entries_total);
    defer allocator.free(input_entries);
    // -------------------

    //==== Insert benchmark ====
    var start_ms = std.Io.Clock.awake.now(io).toMilliseconds();
    try mem_table_pool.insert(io, input_entries);
    var diff_ms = std.Io.Clock.awake.now(io).toMilliseconds() - start_ms;
    std.debug.print(
        \\
        \\benchmark: MemPool insert
        \\  entries: {d}
        \\  time:    {d} ms ({d} s)
        \\  data:    {d} bytes (~{d:.2} MiB)
        \\
    ,
        .{
            input_entries.len,
            diff_ms,
            @divTrunc(diff_ms, 1000),
            usage_memory_bytes,
            usage_memory_mib,
        },
    );
    try testing.expect(diff_ms < 4000); //TODO: Need to research

    // ==== Find benchmark ====
    const lookups_total = 100_000;

    start_ms = std.Io.Clock.awake.now(io).toMilliseconds();
    for (input_entries[0..lookups_total]) |expected_entry| {
        const entry_by_order_id = try mem_table_pool.find("order_id", expected_entry.order_id);
        const entry_by_product_id = try mem_table_pool.find("product_id", expected_entry.product_id);
        try testing.expectEqual(entry_by_product_id.product_id, entry_by_order_id.product_id);
        try testing.expectEqual(entry_by_product_id.order_id, entry_by_order_id.order_id);
    }

    diff_ms = std.Io.Clock.awake.now(io).toMilliseconds() - start_ms;

    std.debug.print(
        \\
        \\ benchmark: MemPool find
        \\  entries: {d}
        \\  lookups: {d}
        \\  time:    {d} ms ({d} s)
        \\  data:    {d} bytes (~{d:.2} MiB)
        \\
    ,
        .{
            input_entries.len,
            lookups_total,
            diff_ms,
            @divTrunc(diff_ms, 1000),
            usage_memory_bytes,
            usage_memory_mib,
        },
    );

    try testing.expect(diff_ms < 1000); //TODO: Need to research
}
