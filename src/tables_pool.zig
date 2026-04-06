const std = @import("std");
const assert = std.testing.assert;
const ArrayList = std.ArrayList;


pub fn TablesPoolType(comptime Table: type) type {
    return struct {
        const TablesPool = @This();

        tables: ArrayList(Table),
        free: []u6,
        filled: []u6,

        pub fn init(allocator: std.mem.Allocator, max_count_mem_tables: u8, max_count_entries: usize) !TablesPool {
            var tables: ArrayList(Table) =  try .initCapacity(allocator, max_count_mem_tables);

            const free = try allocator.alloc(u6, max_count_mem_tables);
            const filled = try allocator.alloc(u6, max_count_mem_tables);

            for (0..max_count_mem_tables) |index| {
                const table: Table = try .init(allocator, max_count_entries);
                tables.appendAssumeCapacity(table, max_count_mem_tables);
                free[index] = index;
            }

            return .{
                .tables = tables,
                .free = free,
                .filled = filled,
            };
        }

        pub fn deinit(tables_pool: *TablesPool, allocator: std.mem.Allocator) void {
            tables_pool.tables.deinit(allocator);
            tables_pool.* = undefined;
        }

        pub fn getFreeTable(tables_pool: *TablesPool) *Table {
            assert(tables_pool.free.len > 0);
            //Get element from end free list
            const free_table_index = tables_pool.free.len - 1;
            tables_pool.free.len -= 1;
            tables_pool.filled[tables_pool.filled.len + 1] = free_table_index;
            return free_table_index;
        }

        // pub fn fillOneFreeTable(tables_pool: *TablesPool, source_table: *MemTableType(EntryType)) void {
        //     const free_table_index = tables_pool.getFreeTableIndex();
            
        //     assert(tables_pool.tables.items[free_table_index].entries.capacity == mem_table.entries.capacity);

        //     // flush entries from mem_table to imm_mem_table via index
        //     tables_pool.tables.items[free_table_index].fill(mem_table);
        //     // move imm_mem_table from free list to filled list
        //     tables_pool.filled[tables_pool.filled + 1] = free_table_index;
        // }

        // fn getFreeTableIndex(tables_pool: *ImmMemTablePool) usize {
        //     assert(tables_pool.free.len > 0);
        //     //Get element from end free list
        //     const free_table_index = tables_pool.free.len - 1;
        //     tables_pool.free.len -= 1;

        //     return free_table_index;
        // }
    };
}
