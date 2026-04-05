const std = @import("std");
const ImmMemTablePoolType = @import("imm_mem_table.zig").ImmMemTablePoolType;

pub fn MemTableType(comptime EntryType: type) type {
    return struct {
        const MemTable = @This();

        entries: std.MultiArrayList(EntryType),
        max_count: usize,

        pub fn init(allocator: std.mem.Allocator, max_count: usize) !MemTable {
            return .{
                .entries = try .initCapacity(allocator, max_count),
                .size = 0,
                .max_count = max_count,
            };
        }

        pub fn addEntries(mem_table: *MemTable, entries: []*EntryType, imm_mem_table_pool: ImmMemTablePoolType(EntryType)) void {
            for (entries) |entry| {
                if (mem_table.entries.len == mem_table.max_count) {
                    mem_table.flush(imm_mem_table_pool);
                }
                mem_table.entries.appendAssumeCapacity(entry);
            }
        }

        fn flush(mem_table: *MemTable, imm_mem_table_pool: ImmMemTablePoolType(EntryType)) void {
            imm_mem_table_pool.fillOneTable(mem_table);
        }
    };
}
