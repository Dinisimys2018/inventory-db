const std = @import("std");
const ArrayList = std.array_list;
const assert = std.debug.assert;
const testing = std.testing;

const printObj = @import("utils/debug.zig").printObj;

const IndexTableStrategy = enum {
    indexes_u32,
};

//TODO: change type to smaller
const IndexPtr = usize;

pub const EntityFieldIndex = struct {
    field_name: []const u8,
    index_strategy: IndexTableStrategy,
};

pub fn EntityFieldIndexListType(comptime indexes_count: u8) type {
    return [indexes_count]EntityFieldIndex;
}

pub fn IndexTableType(
    comptime Key: type,
    comptime TablePtr: type,
    comptime ValuePtr: type,
    comptime LookupValue: type,
) type {
    return struct {
        const IndexTable = @This();

        const Entries = std.AutoHashMapUnmanaged(Key, ValuePtr);

        // Struct Fields
        table_ptr: TablePtr,
        entries: Entries,
        min_key: Key,
        max_key: Key,

        pub fn init(allocator: std.mem.Allocator, table_ptr: TablePtr, entries_max_count: ValuePtr) !*IndexTable {
            var index_table = try allocator.create(IndexTable);
            index_table.table_ptr = table_ptr;
            index_table.entries = .empty;
            index_table.max_key = 0;
            index_table.min_key = 0;

            try index_table.entries.ensureTotalCapacity(allocator, entries_max_count);

            return index_table;
        }

        pub fn deinit(index_table: *IndexTable, allocator: std.mem.Allocator) void {
            index_table.entries.deinit(allocator);
            allocator.destroy(index_table);
        }

        pub fn insert(index_table: *IndexTable, keys: []Key) void {
            var value_ptr: ValuePtr = 0;

            if (index_table.entries.size == 0) {
                index_table.min_key = keys[0];
                index_table.max_key = keys[0];
            }

            while (value_ptr < keys.len) : (value_ptr += 1) {
                if (keys[value_ptr] < index_table.min_key) {
                    index_table.min_key = keys[value_ptr];
                } else if (keys[value_ptr] > index_table.max_key) {
                    index_table.max_key = keys[value_ptr];
                }
                index_table.entries.putAssumeCapacity(keys[value_ptr], value_ptr);
            }


        }

        pub fn find(index_table: *IndexTable, key: Key) !LookupValue {
            if (index_table.entries.size == 0) {
                return error.NotFound;
            }

            if (key > index_table.max_key or key < index_table.min_key) {
                return error.NotFound;
            }

            const value_ptr = index_table.entries.get(key) orelse return error.NotFound;
            return .{
                .table_ptr = index_table.table_ptr,
                .value_ptr = value_ptr,
            };
        }
    };
}

pub fn IndexPoolType(
    comptime TablePtr: type,
    comptime ValuePtr: type,
    comptime indexes_meta: anytype,
    comptime tables_max_count: TablePtr,
    comptime entries_max_count: ValuePtr,
) type {
    const total_fields = indexes_meta.len;

    const LookupValue = struct {
        table_ptr: TablePtr,
        value_ptr: ValuePtr,
    };

    const IndexTableU32 = IndexTableType(u32, TablePtr, ValuePtr, LookupValue);

    const IndexTableUnion = union(IndexTableStrategy) {
        indexes_u32: *IndexTableU32,
    };

    const TableIndexList = []*IndexTableUnion;

    const FieldIndexList = []TableIndexList;

    return struct {
        const IndexPool = @This();

        //Struct fields
        fields_list: [][]const u8,
        indexes_map: FieldIndexList,

        pub fn init(allocator: std.mem.Allocator) !*IndexPool {
            var index_pool = try allocator.create(IndexPool);

            index_pool.indexes_map = try allocator.alloc(TableIndexList, total_fields);
            index_pool.fields_list = try allocator.alloc([]const u8, total_fields);

            var table_ptr: TablePtr = 0;
            var field_index: u8 = 0;

            while (field_index < indexes_meta.len) : (field_index += 1) {
                table_ptr = 0;
                index_pool.indexes_map[field_index] = try allocator.alloc(*IndexTableUnion, tables_max_count);
                index_pool.fields_list[field_index] = indexes_meta[field_index].field_name;

                while (table_ptr < tables_max_count) : (table_ptr += 1) {
                    switch (indexes_meta[field_index].index_strategy) {
                        IndexTableStrategy.indexes_u32 => {
                            index_pool.indexes_map[field_index][table_ptr] = try allocator.create(IndexTableUnion);
                            index_pool.indexes_map[field_index][table_ptr].* = .{ .indexes_u32 = try .init(allocator, table_ptr, entries_max_count) };
                        },
                    }
                }
            }

            return index_pool;
        }

        pub fn deinit(index_pool: *IndexPool, allocator: std.mem.Allocator) void {
            for (index_pool.indexes_map) |field_indexes| {
                for (field_indexes) |table_index| {
                    switch (table_index.*) {
                        .indexes_u32 => |t| t.deinit(allocator),
                    }
                    allocator.destroy(table_index);
                }
                allocator.free(field_indexes);
            }

            allocator.free(index_pool.indexes_map);
            allocator.free(index_pool.fields_list);
            allocator.destroy(index_pool);
        }

        pub fn getIndexTable(index_pool: *IndexPool, table_ptr: TablePtr, field_name: []const u8) !*IndexTableUnion {
            assert(table_ptr < tables_max_count);

            var field_index: u8 = 0;
            while (field_index < index_pool.fields_list.len) : (field_index += 1) {
                if (std.mem.eql(u8, index_pool.fields_list[field_index], field_name)) {
                    return index_pool.indexes_map[field_index][table_ptr];
                }
            }

            return error.NotFound;
        }

        pub fn insert(index_pool: *IndexPool, table_ptr: TablePtr, field_name: []const u8, field_values: []ValuePtr) !void {
            const index_union = try index_pool.getIndexTable(table_ptr, field_name);
            switch (index_union.*) {
                .indexes_u32 => |index_table| {
                    return index_table.insert(field_values);
                },
            }
        }

        pub fn find(index_pool: *IndexPool, field_name: []const u8, field_value: anytype) !LookupValue {
            var field_index: u8 = 0;
            while (field_index < index_pool.fields_list.len) : (field_index += 1) {
                if (std.mem.eql(u8, index_pool.fields_list[field_index], field_name)) {
                    for (index_pool.indexes_map[field_index]) |index_table_union| {
                        switch (index_table_union.*) {
                            .indexes_u32 => |index_table| {
                                return index_table.find(field_value) catch continue;
                            },
                        }
                    }
                    return error.NotFound;
                }
            }

            return error.NotFound;
        }
    };
}

// ==== Testing ====
const fields_count = 2;

const TestEntity = struct {
    pub const IndexesMeta = EntityFieldIndexListType(fields_count);

    pub const indexes_meta: IndexesMeta = .{
        .{
            .field_name = "order_id",
            .index_strategy = .indexes_u32,
        },
        .{
            .field_name = "product_id",
            .index_strategy = .indexes_u32,
        },
    };
};

test "IndexPool" {
    const allocator = std.testing.allocator;
    const tables_max_count = 5;
    const entries_max_count = 5;

    const TestTablePtr = u32;
    const TestValuePtr = u32;

    const IndexPool = IndexPoolType(
        TestTablePtr,
        TestValuePtr,
        TestEntity.indexes_meta,
        tables_max_count,
        entries_max_count,
    );
    var index_pool: *IndexPool = try .init(allocator);
    defer index_pool.deinit(allocator);

    try testing.expect(index_pool.fields_list.len == fields_count);

    try testing.expectEqualStrings(index_pool.fields_list[0], "order_id");
    try testing.expectEqualStrings(index_pool.fields_list[1], "product_id");

    var table_ptr: TestTablePtr = 0;

    // Preparing input data
    const not_found_input = 1000;
    const input = [_]TestValuePtr{ 100, 200, 300, 400 };
    var input_table = try allocator.alloc(TestValuePtr, input.len);
    defer allocator.free(input_table);
    // -------------------

    for (0..index_pool.fields_list.len) |field_index| {
        try testing.expect(index_pool.indexes_map[field_index].len == tables_max_count);
        const field_name = index_pool.fields_list[field_index];

        while (table_ptr < tables_max_count) : (table_ptr += 1) {
            for (input, 0..) |value, i| {
                input_table[i] = value + table_ptr;
            }

            //==== Insert to Index Pool ====
            try index_pool.insert(table_ptr, field_name, input_table);

            var value_ptr: TestValuePtr = 0;

            while (value_ptr < input.len) : (value_ptr += 1) {
                
                //==== Find on Index Pool ====
                const find_lookup_value = try index_pool.find(field_name, input_table[value_ptr]);
                try testing.expect(find_lookup_value.table_ptr == table_ptr);
                try testing.expect(find_lookup_value.value_ptr == value_ptr);
            }

            const not_found_value = index_pool.find(field_name, not_found_input + table_ptr);

            try testing.expectError(error.NotFound, not_found_value);
        }
    }
}
