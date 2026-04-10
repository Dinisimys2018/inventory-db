const std = @import("std");
const ArrayList = std.ArrayList;
const assert = std.debug.assert;
const testing = std.testing;
pub const MemTablePtr = u8;

// TODO: возможно MemTableType можно перенести внутри MemTablePoolType
pub fn MemTableType(comptime EntryType: type) type {
    return struct {
        const MemTable = @This();

        entries: std.MultiArrayList(EntryType) = .empty,

        pub fn init(mem_table: *MemTable, allocator: std.mem.Allocator, entries_max_count: u32) !void {
            mem_table.entries = try .initCapacity(allocator, entries_max_count);
        }

        pub fn deinit(mem_table: *MemTable, allocator: std.mem.Allocator) void {
            mem_table.entries.deinit(allocator);
            allocator.destroy(mem_table);
        }

        pub fn insert(mem_table: *MemTable, entries: []EntryType) void {
            std.debug.print("\nBefore insert\ntable len={d}|table cap={d}|input len={d}|\n============\n", .{ mem_table.entries.len, mem_table.entries.capacity, entries.len });
            for (entries) |entry| {
                mem_table.entries.appendAssumeCapacity(entry);
            }
        }
    };
}

pub fn MemTablePoolType(comptime EntryType: type, mem_tables_max_count: comptime_int) type {
    return struct {
        comptime {
            assert(mem_tables_max_count <= std.math.maxInt(MemTablePtr));
        }

        const MemTablePool = @This();
        const MemTable = MemTableType(EntryType);
        const TableList = ArrayList(*MemTable);

        // Struct Fields
        tables: TableList,
        free_table_ptrs: [mem_tables_max_count]bool,
        filled_table_ptrs: [mem_tables_max_count]bool,
        active_table_ptr: MemTablePtr = 0,

        pub fn init(allocator: std.mem.Allocator, entries_max_count: u32) !MemTablePool {
            var tables: TableList = try .initCapacity(allocator, mem_tables_max_count);
            errdefer tables.deinit(allocator);

            var index: MemTablePtr = 0;
            const free_table_ptrs: [mem_tables_max_count]bool = .{true} ** mem_tables_max_count;
            const filled_table_ptrs: [mem_tables_max_count]bool = .{false} ** mem_tables_max_count;

            errdefer {
                for (tables.items) |table| {
                    table.deinit(allocator);
                }
            }

            while (index < mem_tables_max_count) : (index += 1) {
                var table = try allocator.create(MemTable);
                try table.init(allocator, entries_max_count);
                tables.appendAssumeCapacity(table);
            }

            return .{
                .tables = tables,
                .free_table_ptrs = free_table_ptrs,
                .filled_table_ptrs = filled_table_ptrs,
                .active_table_ptr = 0,
            };
        }

        pub fn deinit(mem_tables_pool: *MemTablePool, allocator: std.mem.Allocator) void {
            for (mem_tables_pool.tables.items) |table| {
                table.deinit(allocator);
            }
            mem_tables_pool.tables.deinit(allocator);
            mem_tables_pool.* = undefined;
        }

        pub fn insert(mem_tables_pool: *MemTablePool, entries: []EntryType) void {
            var entries_start: usize = 0;
            var entries_end: usize = 0;

            while (entries_end < entries.len) {
                // Сразу убираем активную таблицу из свободных,
                // чтобы другие вызовы не получили доступ к ней
                mem_tables_pool.free_table_ptrs[mem_tables_pool.active_table_ptr] = false;

                var active_table = mem_tables_pool.tables.items[mem_tables_pool.active_table_ptr];

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
    const MemTablePool = MemTablePoolType(Entity, mem_tables_max_count);
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

    std.debug.print("\nCHECK\n{any}\n============\n", .{mem_table_pool.tables.items});

    mem_table_pool.insert(input_entries.items);

    const count_filled_tables = mem_table_pool.calculateFilledTables();
    const count_free_tables = mem_table_pool.calculateFreeTables();

    try testing.expectEqual(mem_tables_max_count - 1, count_filled_tables);
    try testing.expectEqual(mem_tables_max_count - 1 - count_filled_tables, count_free_tables);
}
