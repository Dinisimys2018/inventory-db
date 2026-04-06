const std = @import("std");
const ArrayList = std.ArrayList;
const assert = std.testing.assert;

const ImmMemTablePoolType = @import("imm_mem_table.zig").ImmMemTablePoolType;
const testing = std.testing;

pub fn MemTableType(comptime EntryType: type) type {
    return struct {
        const MemTable = @This();

        entries: std.MultiArrayList(EntryType) = .empty,

        pub fn init(allocator: std.mem.Allocator, entries_max_count: usize) !MemTable {
            return .{
                .entries = try .initCapacity(allocator, entries_max_count),
            };
        }

        pub fn deinit(mem_table: *MemTable, allocator: std.mem.Allocator) void {
            mem_table.entries.deinit(allocator);
            mem_table.* = undefined;
        }

        pub fn fill(mem_table: *MemTable, entries: []*EntryType) void {
            assert(mem_table.entries.len <= mem_table.entries.capacity);

            for (entries) |entry| {
                mem_table.entries.appendAssumeCapacity(entry);
            }
        }
    };
}

pub fn MemTablePoolType(comptime EntryType: type) type {
    return struct {
        const MemTablePool = @This();
        const MemTable = MemTableType(EntryType);

        tables: ArrayList(MemTable),
        active_table_index: u6,
        free: []u6,
        filled: []u6,

        pub fn init(allocator: std.mem.Allocator, mem_tables_max_count: u8, entries_max_count: usize) !MemTablePool {
            var tables: ArrayList(MemTable) = try .initCapacity(allocator, mem_tables_max_count);

            const free = try allocator.alloc(u6, mem_tables_max_count);
            const filled = try allocator.alloc(u6, mem_tables_max_count);

            for (0..mem_tables_max_count) |index| {
                const table: MemTable = try .init(allocator, entries_max_count);
                tables.appendAssumeCapacity(table, mem_tables_max_count);
                free[index] = index;
            }

            return .{
                .tables = tables,
                .active_table_index = 0,
                .free = free,
                .filled = filled,
            };
        }

        pub fn deinit(mem_tables_pool: *MemTablePool, allocator: std.mem.Allocator) void {
            mem_tables_pool.tables.deinit(allocator);
            mem_tables_pool.* = undefined;
        }

        pub fn addEntries(mem_tables_pool: *MemTablePool, entries: []*EntryType) void {
            var entries_count = 0;
            var entries_offset: usize = 0;
            var input_entries = entries;

            while (entries_count <= entries.len) {
                const active_table = mem_tables_pool.tables.items[mem_tables_pool.active_table_index];

                entries_offset = active_table.entries.capacity - active_table.entries.len;
                entries_count += entries_offset;
                active_table.fill(input_entries);

                if (entries_offset <= input_entries.len) {
                    mem_tables_pool.filled[mem_tables_pool.filled.len + 1] = mem_tables_pool.active_table_index;
                    mem_tables_pool.active_table_index = mem_tables_pool.getFreeTableIndex();
                }

                input_entries = input_entries[entries_offset..];
            }
        }

        fn getFreeTableIndex(mem_tables_pool: *MemTablePool) usize {
            assert(mem_tables_pool.free.len > 0);
            //Get element from end free list
            const free_table_index = mem_tables_pool.free.len - 1;
            mem_tables_pool.free.len -= 1;

            return free_table_index;
        }
    };
}

const OrderItemRow = @import("entities.zig").OrderItemRow;

test "mem_table" {
    const allocator = std.testing.allocator;
    const Entity = OrderItemRow;
    const entries_max_count = 2;
    const mem_tables_max_count = 2;

    const mem_table_pool: MemTablePoolType(Entity) = try .init(
        allocator,
        mem_tables_max_count,
        entries_max_count,
    );
    defer mem_table_pool.deinit(allocator);

    const input_entries: std.ArrayList(Entity) = try .initCapacity(allocator, entries_max_count);

    for (0..entries_max_count) |i| {
        input_entries.appendAssumeCapacity(.{
            .id = i,
            .order_id = i * 2,
        });
    }

    mem_table_pool.addEntries(input_entries);
}
