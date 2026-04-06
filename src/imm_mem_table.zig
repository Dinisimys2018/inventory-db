const std = @import("std");
const assert = std.testing.assert;
const ArrayList = std.ArrayList;
const MemTableType = @import("mem_table.zig").MemTableType;

/// Immutable Memory Table
pub fn ImmMemTableType(comptime EntryType: type) type {
    return struct {
        const ImmMemTable = @This();

        entries: std.MultiArrayList(EntryType),

        /// entries_max_count - common between ImmMemTable and MemTable
        pub fn init(allocator: std.mem.Allocator, entries_max_count: usize) !ImmMemTable {
            return .{
                .entries = try .initCapacity(allocator, entries_max_count),
            };
        }

        pub fn fill(imm_mem_table: *ImmMemTable, mem_table: *MemTableType(EntryType)) void {
            assert(mem_table.entries.len <= imm_mem_table.entries.capacity);

            imm_mem_table.* = .{
                .entries = mem_table.entries,
            };
        }
    };
}

pub fn ImmMemTablePoolType(comptime EntryType: type) type {
    return struct {
        const ImmMemTablePool = @This();
        const ImmMemTable = ImmMemTableType(EntryType);
        tables: ArrayList(ImmMemTable),
        free: []u6,
        filled: []u6,

        pub fn init(allocator: std.mem.Allocator, max_count_mem_tables: u8, max_count_entries: usize) !ImmMemTablePool {
            var tables: ArrayList(ImmMemTable) =  try .initCapacity(allocator, max_count_mem_tables);

            const free = try allocator.alloc(u6, max_count_mem_tables);
            const filled = try allocator.alloc(u6, max_count_mem_tables);
            var index: u6 = 0;
            while (index < max_count_mem_tables): (index += 1) {
                const table: ImmMemTable = try .init(allocator, max_count_entries);
                tables.appendAssumeCapacity(table);
                free[index] = index;
            }

            return .{
                .tables = tables,
                .free = free,
                .filled = filled,
            };
        }

        pub fn deinit(imm_mem_tables_pool: *ImmMemTablePool, allocator: std.mem.Allocator) void {
            imm_mem_tables_pool.tables.deinit(allocator);
            imm_mem_tables_pool.* = undefined;
        }

        pub fn fillOneFreeTable(imm_mem_tables_pool: *ImmMemTablePool, mem_table: *MemTableType(EntryType)) void {
            const free_table_index = imm_mem_tables_pool.getFreeTableIndex();
            
            assert(imm_mem_tables_pool.tables.items[free_table_index].entries.capacity == mem_table.entries.capacity);

            // flush entries from mem_table to imm_mem_table via index
            imm_mem_tables_pool.tables.items[free_table_index].fill(mem_table);
            // move imm_mem_table from free list to filled list
            imm_mem_tables_pool.filled[imm_mem_tables_pool.filled + 1] = free_table_index;
        }

        fn getFreeTableIndex(imm_mem_tables_pool: *ImmMemTablePool) usize {
            assert(imm_mem_tables_pool.free.len > 0);
            //Get element from end free list
            const free_table_index = imm_mem_tables_pool.free.len - 1;
            imm_mem_tables_pool.free.len -= 1;

            return free_table_index;
        }
    };
}
