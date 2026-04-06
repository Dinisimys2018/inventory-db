const std = @import("std");
const ArrayList = std.ArrayList;
const assert = std.debug.assert;
const testing = std.testing;

const ImmMemTablePoolType = @import("imm_mem_table.zig").ImmMemTablePoolType;

pub fn MemTableType(comptime EntryType: type) type {
    return struct {
        const MemTable = @This();

        entries: std.MultiArrayList(EntryType),

        pub fn init(allocator: std.mem.Allocator, entries_max_count: usize) !MemTable {
            return .{
                .entries = try .initCapacity(allocator, entries_max_count),
            };
        }

        pub fn deinit(mem_table: *MemTable, allocator: std.mem.Allocator) void {
            mem_table.entries.deinit(allocator);
            mem_table.* = undefined;
        }

        pub fn fill(mem_table: *MemTable, entries: []EntryType) void {
            for (entries) |entry| {
                mem_table.entries.appendAssumeCapacity(entry);
            }
        }
    };
}

pub fn MemTablePoolType(comptime EntryType: type, mem_tables_max_count: comptime_int) type {
    const Index = u8;

    comptime {
        assert(mem_tables_max_count <= std.math.maxInt(Index));
    }

    return struct {
        const MemTablePool = @This();
        const MemTable = MemTableType(EntryType);

        tables: ArrayList(*MemTable),
        free: [mem_tables_max_count]bool,
        filled: [mem_tables_max_count]bool,
        active_table_index: Index = 0,

        pub fn init(allocator: std.mem.Allocator, entries_max_count: usize) !MemTablePool {
            var tables: ArrayList(*MemTable) = try .initCapacity(allocator, mem_tables_max_count);
            errdefer tables.deinit(allocator);

            var index: Index = 0;
            const free: [mem_tables_max_count]bool = .{true} ** mem_tables_max_count;
            const filled: [mem_tables_max_count]bool = .{false} ** mem_tables_max_count;

            while (index < mem_tables_max_count) : (index += 1) {
                var table = try allocator.create(MemTable);
                errdefer allocator.destroy(table);
                table.* = try .init(allocator, entries_max_count);
                errdefer table.deinit(allocator);

                tables.appendAssumeCapacity(table);
            }

            return .{
                .tables = tables,
                .free = free,
                .filled = filled,
                .active_table_index = 0,
            };
        }

        pub fn deinit(mem_tables_pool: *MemTablePool, allocator: std.mem.Allocator) void {
            for (mem_tables_pool.tables.items) |table| {
                table.deinit(allocator);
                allocator.destroy(table);
            }
            mem_tables_pool.tables.deinit(allocator);
            mem_tables_pool.* = undefined;
        }

        pub fn addEntries(mem_tables_pool: *MemTablePool, entries: []EntryType) void {
            var entries_start: usize = 0;
            var entries_end: usize = 0;

            while (entries_end < entries.len) {
                // Сразу убираем активную таблицу из свободных,
                // чтобы другие вызовы не получили доступ к ней
                mem_tables_pool.free[mem_tables_pool.active_table_index] = false;
                var active_table = mem_tables_pool.tables.items[mem_tables_pool.active_table_index];
                // Получаем количество, которое мы можем вставить в активную таблицу
                const rest = active_table.entries.capacity - active_table.entries.len;
                entries_end += rest;

                if (entries_end >= entries.len) {
                    entries_end = entries.len;
                }

                std.debug.print("\nentries_start = {d}| entries_end = {d}| rest = {d}\n", .{entries_start, entries_end , rest});
                std.debug.print("\ninput entries = \n{any}\n======\n", .{entries[entries_start..entries_end]});

                active_table.fill(entries[entries_start..entries_end]);

                // Если мы заполнили все свободное место
                // значит перемещаем активную таблицу в filled 
                if (rest == entries_end - entries_start) {
                    mem_tables_pool.filled[mem_tables_pool.active_table_index] = true;

                    // Если есть еще свободные таблицы,
                    // тогда смещаем индекс для работы с новой активной таблицой
                    if (mem_tables_pool.active_table_index < mem_tables_pool.filled.len - 1) {
                        mem_tables_pool.active_table_index += 1;
                    } else {
                        //TODO full-filled all mem_tables
                        unreachable;
                    }
                }

                entries_start = entries_end;
            }
        }
    };
}

const OrderItemRow = @import("entities.zig").OrderItemRow;

test "mem_table" {
    const allocator = std.testing.allocator;
    const Entity = OrderItemRow;
    const entries_max_count = 2;
    const mem_tables_max_count = 2;
    const MemTablePool = MemTablePoolType(Entity, mem_tables_max_count);
    var mem_table_pool:MemTablePool = try .init(
        allocator,
        entries_max_count,
    );
    defer mem_table_pool.deinit(allocator);
    const entries_count = entries_max_count * mem_tables_max_count - 1;
    var input_entries: std.ArrayList(Entity) = try .initCapacity(allocator, entries_count);
    defer input_entries.deinit(allocator);

    var index: u8 = 0;

    while(index < entries_count): (index += 1) {
        input_entries.appendAssumeCapacity(.{
            .id = index,
            .order_id = index * 2,
        });
    }

    mem_table_pool.addEntries(input_entries.items);
    std.debug.print("free: {any}\n", .{mem_table_pool.free}); 
    std.debug.print("filled: {any}\n", .{mem_table_pool.filled}); 
}
