const std = @import("std");
const ArrayList = std.array_list;
const assert = std.debug.assert;

pub const IndexTableStrategy = union(enum) {
    const Self = @This();

    index_u32: IndexTableType(u32),
};

pub const EntityFieldIndex = struct {
    field_name: []const u8,
    index_strategy: IndexTableStrategy,
};

pub fn EntityFieldIndexListType(comptime count: u4) type {
    return [count]EntityFieldIndex;
}

pub fn IndexTableType(comptime Key: type) type {
    return struct {
        const Wraper = @This();

        ff: u8 = 99,

        pub fn Generic(
            wraper: *const Wraper,
            comptime TablePtr: type,
            comptime ValuePtr: type,
        ) type {
            _ = wraper;

            return struct {
                const IndexTable = @This();

                const IndexValue = struct {
                    value_ptr: ValuePtr,
                };

                const LookupValue = struct {
                    table_ptr: TablePtr,
                    value_ptr: ValuePtr,
                };

                const Entries = std.AutoHashMapUnmanaged(Key, IndexValue);

                // Struct Fields
                table_ptr: TablePtr,
                entries: Entries,

                pub fn init(allocator: std.mem.Allocator, table_ptr: TablePtr, entries_max_count: ValuePtr) !IndexTable {
                    var entries: Entries = .empty;
                    try entries.ensureTotalCapacity(allocator, entries_max_count);

                    return .{
                        .table_ptr = table_ptr,
                        .entries = entries,
                    };
                }

                pub fn deinit(index_table: *IndexTable, allocator: std.mem.Allocator) void {
                    index_table.entries.deinit(allocator);
                    index_table.* = undefined;
                }

                pub fn insert(index_table: *IndexTable, keys: []Key) void {
                    var value_ptr: ValuePtr = 0;

                    while (value_ptr < keys.len) : (value_ptr += 1) {
                        index_table.entries.putAssumeCapacity(keys[value_ptr], value_ptr);
                    }
                }

                pub fn find(index_table: *IndexTable, key: Key) ?LookupValue {
                    const value_ptr_res = index_table.entries.get(key);
                    if (value_ptr_res) |value_ptr| {
                        return .{
                            .table_ptr = index_table.table_ptr,
                            .value_ptr = value_ptr,
                        };
                    }

                    return null;
                }
            };
        }
    };
}
