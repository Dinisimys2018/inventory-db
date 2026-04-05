const std = @import("std");
const assert = std.testing.assert;
const ArrayList = std.ArrayList;
const MemTableType = @import("mem_table.zig").MemTableType;

/// Immutable Memory Table
pub fn ImmMemTableType(comptime EntryType: type) type {
    return struct {
        const ImmMemTable = @This();

        entries: std.MultiArrayList(EntryType),

        pub fn init(allocator: std.mem.Allocator, max_count: usize) !ImmMemTable {
            return .{
                .entries = try .initCapacity(allocator, max_count),
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

        tables: ArrayList(ImmMemTableType(EntryType)),
        free: []u6,
        filled: []u6,

        pub fn init(allocator: std.mem.Allocator, max_count_tables: u8, max_count_entries: usize) !ImmMemTablePool {
            var tables: ArrayList(ImmMemTableType(EntryType)) =  try .initCapacity(allocator, max_count_tables);

            const free = try allocator.alloc(u6, max_count_tables);
            const filled = try allocator.alloc(u6, max_count_tables);

            for (0..max_count_tables) |index| {
                const table: ImmMemTableType(EntryType) = try .init(allocator, max_count_entries);
                tables.appendAssumeCapacity(table, max_count_tables);
                free[index] = index;
            }

            return .{
                .tables = tables,
                .free = free,
                .filled = filled,
            };
        }

        pub fn fillOneTable(imm_mem_table_pool: ImmMemTablePool, mem_table: *MemTableType(EntryType)) void {
            const free_table_index = imm_mem_table_pool.free.len - 1;
            imm_mem_table_pool.free.len -= 1;
            assert(imm_mem_table_pool.free.lenimm_mem_table_pool.free.len > 0);
            
            imm_mem_table_pool.tables.items[free_table_index].fill(mem_table);
            imm_mem_table_pool.filled[imm_mem_table_pool.filled + 1] = free_table_index;
        }
    };
}
