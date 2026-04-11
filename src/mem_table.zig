const std = @import("std");
const ArrayList = std.ArrayList;
const assert = std.debug.assert;
const testing = std.testing;

const printObj = @import("utils/debug.zig").printObj;

const EntityFieldIndex = @import("index_mem_table.zig").EntityFieldIndex;
const IndexTableStrategy = @import("index_mem_table.zig").IndexTableStrategy;

pub const MemTablePtr = u8;
pub const MemEntryPtr = u32;

// TODO: возможно MemTableType можно перенести внутри MemTablePoolType
pub fn MemTableType(comptime EntryType: type) type {
    return struct {
        const MemTable = @This();

        entries: std.MultiArrayList(EntryType) = .empty,

        pub fn init(allocator: std.mem.Allocator, comptime entries_max_count: MemEntryPtr) !*MemTable {
            var mem_table = try allocator.create(MemTable);
            mem_table.entries = try .initCapacity(allocator, entries_max_count);

            return mem_table;
        }

        pub fn deinit(mem_table: *MemTable, allocator: std.mem.Allocator) void {
            mem_table.entries.deinit(allocator);
            allocator.destroy(mem_table);
        }

        pub fn insert(mem_table: *MemTable, entries: []EntryType) void {
            for (entries) |entry| {
                mem_table.entries.appendAssumeCapacity(entry);
            }
        }
    };
}

pub fn MemTablePoolType(
    comptime EntryType: type,
    comptime indexes_meta: anytype,
    comptime mem_tables_max_count: MemTablePtr,
) type {
    return struct {
        const MemTablePool = @This();
        const MemTable = MemTableType(EntryType);
        const TableList = []*MemTable;
        const IndexMap = std.StringArrayHashMapUnmanaged(IndexTableStrategy);

        // Struct Fields
        tables: TableList,
        free_table_ptrs: [mem_tables_max_count]bool,
        filled_table_ptrs: [mem_tables_max_count]bool,
        active_table_ptr: MemTablePtr = 0,

        indexes: IndexMap,

        pub fn init(allocator: std.mem.Allocator, comptime entries_max_count: u32) !MemTablePool {
            var tables: TableList = try allocator.alloc(*MemTable, entries_max_count);
            errdefer allocator.free(tables);

            var indexes: IndexMap = .empty;
            try indexes.ensureTotalCapacity(allocator, indexes_meta.len);

            var mem_table_ptr: MemTablePtr = 0;

            errdefer {
                for (tables) |table| {
                    table.deinit(allocator);
                }
            }

            while (mem_table_ptr < mem_tables_max_count) : (mem_table_ptr += 1) {
                tables[mem_table_ptr] = try .init(allocator, entries_max_count);
            }

            inline for (0..mem_tables_max_count) |table_ptr| {
                inline for (EntryType.indexes_meta) |index_meta| {
                    const index_table: index_meta.index_strategy.index_u32.Generic(MemTablePtr, MemEntryPtr) = try .init(allocator, table_ptr, entries_max_count);
                    indexes.putAssumeCapacity(index_meta.field_name, index_table);
                }
            }

            return .{
                .tables = tables,
                .free_table_ptrs = .{true} ** mem_tables_max_count,
                .filled_table_ptrs = .{false} ** mem_tables_max_count,
                .active_table_ptr = 0,
                .indexes = indexes,
            };
        }

        pub fn deinit(mem_tables_pool: *MemTablePool, allocator: std.mem.Allocator) void {
            for (mem_tables_pool.tables) |table| {
                table.deinit(allocator);
            }
            allocator.free(mem_tables_pool.tables);
        }

        pub fn insert(mem_tables_pool: *MemTablePool, entries: []EntryType) void {
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

                active_table.insert(entries[entries_start..entries_end]);

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
const OrderItemRow = @import("entities.zig").OrderItemRow;

test "MemTablePool: (max count entries for all tables in pool) - 1" {
    const allocator = std.testing.allocator;

    const Entity = OrderItemRow;
    const entries_max_count = 5;
    const mem_tables_max_count = 5;
    const MemTablePool = MemTablePoolType(Entity, Entity.indexes_meta, mem_tables_max_count);
    var mem_table_pool: MemTablePool = try .init(
        allocator,
        entries_max_count,
    );
    defer mem_table_pool.deinit(allocator);

    // Максимальное количество entries,
    // которое может вместить весь pool минус 1,
    // чтобы не заполнить все таблицы
    const entries_count = entries_max_count * mem_tables_max_count - 1;
    var input_entries: std.ArrayList(Entity) = try .initCapacity(allocator, entries_count);
    defer input_entries.deinit(allocator);

    var index: u8 = 0;

    while (index < entries_count) : (index += 1) {
        input_entries.appendAssumeCapacity(.{ .order_id = index * 2, .product_id = index * 2 + 1 });
    }

    //==== General test ====

    mem_table_pool.insert(input_entries.items);

    const count_filled_tables = mem_table_pool.calculateFilledTables();
    const count_free_tables = mem_table_pool.calculateFreeTables();

    try testing.expectEqual(mem_tables_max_count - 1, count_filled_tables);
    try testing.expectEqual(mem_tables_max_count - 1 - count_filled_tables, count_free_tables);
}
