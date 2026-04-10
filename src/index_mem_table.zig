const std = @import("std");
const ArrayList = std.array_list;
const assert = std.debug.assert;

pub fn IndexMemTableType(
    comptime Key: type,
    comptime TablePtr: type,
    comptime ValuePtr: type,
) type {
    return struct {
        const IndexMemTable = @This();

        const IndexValue = struct {
            value_ptr: ValuePtr,
        };

        const LookupValue = struct {
            table_ptr: TablePtr,
            value_ptr: ValuePtr,
        };

        const Entries = std.AutoHashMapUnmanaged(Key, IndexValue);

        // Struct Fields
        mem_table_ptr: TablePtr,
        entries: Entries,

        pub fn init(allocator: std.mem.Allocator, mem_table_ptr: TablePtr, entries_max_count: ValuePtr) !IndexMemTable {
            var entries: Entries = .empty;
            try entries.ensureTotalCapacity(allocator, entries_max_count);

            return .{
                .mem_table_ptr = mem_table_ptr,
                .entries = entries,
            };
        }

        pub fn deinit(index_mem_table: *IndexMemTable, allocator: std.mem.Allocator) void {
            index_mem_table.entries.deinit(allocator);
            index_mem_table.* = undefined;
        }

        pub fn insert(index_mem_table: *IndexMemTable, keys: []Key) void {
            var value_ptr: ValuePtr = 0;

            while (value_ptr < keys.len) : (value_ptr += 1) {
                index_mem_table.entries.putAssumeCapacity(keys[value_ptr], value_ptr);
            }
        }

        pub fn find(index_mem_table: *IndexMemTable, key: Key) ?LookupValue {
            const value_ptr_res = index_mem_table.entries.get(key);
            if (value_ptr_res) |value_ptr| {
                return .{
                    .table_ptr = index_mem_table.mem_table_ptr,
                    .value_ptr = value_ptr,
                };
            }

            return null;
        }
    };
}
