//TODO: - change all errors to Error Enum Type

const std = @import("std");
const ArrayList = std.array_list;
const assert = std.debug.assert;
const testing = std.testing;

const printObj = @import("utils/debug.zig").printObj;

const TablePtr = @import("order_item_mem_table.zig").MemTablePtr;
const EntityPtr = @import("order_item_mem_table.zig").MemEntryPtr;

pub fn NonUniqueMemIndexType(comptime KeyType: type) type {
    return struct {
        pub const Key = KeyType;

        const IndexValue = struct {
            key: Key,
            entity_ptr: EntityPtr,

            pub fn compareKeys(target_key: Key, index_value: *IndexValue) std.math.Order {
                return std.math.order(target_key, index_value.key);
            }

            pub fn lessThan(_: @TypeOf(.{}), a: *IndexValue, b: *IndexValue) bool {
                return a.key < b.key;
            }
        };

        pub const BlockLookupValue = struct {
            table_ptr: TablePtr,
            entity_ptr_list: []*IndexValue,
        };

        pub const PoolLookupResult = std.ArrayList(BlockLookupValue);

        pub fn IndexBlockType(comptime keys_max_count: EntityPtr) type {
            return struct {
                const IndexBlock = @This();

                table_ptr: TablePtr,
                entries: []*IndexValue,
                next_entry_index: EntityPtr,
                has_keys: bool,
                min_key: Key,
                max_key: Key,
                sorted: bool,

                pub fn init(allocator: std.mem.Allocator, table_ptr: TablePtr) !*IndexBlock {
                    var index_block = try allocator.create(IndexBlock);
                    index_block.table_ptr = table_ptr;
                    index_block.has_keys = false;
                    index_block.next_entry_index = 0;
                    index_block.max_key = 0;
                    index_block.min_key = 0;
                    index_block.sorted = false;

                    index_block.entries = try allocator.alloc(*IndexValue, keys_max_count);
                    var entry_idx: EntityPtr = 0;
                    while (entry_idx < keys_max_count) : (entry_idx += 1) {
                        index_block.entries[entry_idx] = try allocator.create(IndexValue);
                    }

                    return index_block;
                }

                pub fn deinit(index_block: *IndexBlock, allocator: std.mem.Allocator) void {
                    for (index_block.entries) |entry| allocator.destroy(entry);
                    allocator.free(index_block.entries);
                    allocator.destroy(index_block);
                }

                pub fn insert(
                    index_block: *IndexBlock,
                    entry_table_count: EntityPtr,
                    keys: []Key,
                ) void {
                    assert(keys.len > 0);
                    // Expected that the Index capacity is equal to the Table capacity.
                    assert((index_block.next_entry_index + keys.len) <= index_block.entries.len);

                    index_block.sorted = false;

                    if (!index_block.has_keys) {
                        index_block.min_key = keys[0];
                        index_block.max_key = keys[0];
                    }

                    var next_entry_ptr: EntityPtr = entry_table_count;

                    for (keys) |key| {
                        if (key < index_block.min_key) {
                            index_block.min_key = key;
                        } else if (key > index_block.max_key) {
                            index_block.max_key = key;
                        }

                        index_block.entries[index_block.next_entry_index].key = key;
                        index_block.entries[index_block.next_entry_index].entity_ptr = next_entry_ptr;

                        index_block.next_entry_index += 1;
                        next_entry_ptr += 1;
                    }
                    index_block.has_keys = true;
                }

                pub fn sort(index_block: *IndexBlock) void {
                    if (index_block.sorted) return;

                    std.sort.block(*IndexValue, index_block.entries, .{}, IndexValue.lessThan);
                    index_block.sorted = true;
                }

                pub fn lookup(index_block: *IndexBlock, key: anytype) !BlockLookupValue {
                    assert(index_block.sorted);

                    const range = std.sort.equalRange(*IndexValue, index_block.entries, key, IndexValue.compareKeys);
                    if (range[1] == 0) return error.NotFound;

                    return .{
                        .table_ptr = index_block.table_ptr,
                        .entity_ptr_list = index_block.entries[range[0]..range[1]],
                    };
                }
            };
        }

        pub fn IndexPoolType(
            comptime index_block_max_count: TablePtr,
            comptime keys_per_block: EntityPtr,
        ) type {
            return struct {
                const IndexPool = @This();
                const IndexBlock = IndexBlockType(keys_per_block);

                // Fields
                blocks: []*IndexBlock,
                sorted: bool,
                //only for sigle-thread mode, use many result instances in multi-thread mode
                lookup_result: PoolLookupResult,

                pub fn init(allocator: std.mem.Allocator, table_ptr_range: struct { TablePtr, TablePtr }) !*IndexPool {
                    var index_pool = try allocator.create(IndexPool);
                    index_pool.sorted = false;

                    index_pool.blocks = try allocator.alloc(*IndexBlock, index_block_max_count);
                    var table_ptr: TablePtr = table_ptr_range[0];

                    while (table_ptr < table_ptr_range[1]) : (table_ptr += 1) {
                        index_pool.blocks[table_ptr] = try .init(allocator, table_ptr);
                    }

                    index_pool.lookup_result = try .initCapacity(allocator, index_block_max_count);

                    return index_pool;
                }

                pub fn deinit(index_pool: *IndexPool, allocator: std.mem.Allocator) void {
                    index_pool.lookup_result.deinit(allocator);
                    for (index_pool.blocks) |index_block| {
                        index_block.deinit(allocator);
                    }
                    allocator.free(index_pool.blocks);
                    allocator.destroy(index_pool);
                }

                pub fn insert(index_pool: *IndexPool, table_ptr: TablePtr, entry_table_count: EntityPtr, keys: []Key) void {
                    index_pool.blocks[table_ptr].insert(entry_table_count, keys);
                }

                pub fn sort(index_pool: *IndexPool, table_ptr: TablePtr) void {
                    index_pool.blocks[table_ptr].sort();
                }

                pub fn lookup(index_pool: *IndexPool, key: Key) !*PoolLookupResult {
                    index_pool.lookup_result.clearRetainingCapacity();

                    for (index_pool.blocks) |index_block| {
                        if (key >= index_block.min_key and key <= index_block.max_key) {
                            const block_result = index_block.lookup(key) catch unreachable;
                            index_pool.lookup_result.appendAssumeCapacity(block_result);
                        }
                    }

                    if (index_pool.lookup_result.items.len != 0) {
                        return &index_pool.lookup_result;
                    }

                    return error.NotFound;
                }
            };
        }
    };
}

test "NonUniqueMemIndexType: pool: insert and lookup" {
    const allocator = std.testing.allocator;

    const table_ptr_list = [_]TablePtr{ 0, 1 };

    const keys_per_block: EntityPtr = 5;
    const Key = u32;
    const NonUniqueMemIndexU32 = NonUniqueMemIndexType(Key);

    const index_pool: *NonUniqueMemIndexU32.IndexPoolType(table_ptr_list.len, keys_per_block) = try .init(allocator, .{0,2});
    defer index_pool.deinit(allocator);

    //                     entity ptrs => |0, 1, 2, 3, 4|
    var keys_first_table = [_]Key{ 1, 1, 3, 3, 2 };

    //                      entity ptrs => |0, 1, 2, 3, 4|
    var keys_second_table = [_]Key{ 2, 1, 9, 2, 2 };

    const Case = struct {
        key: Key,
        table_entity_ptrs: []const []const EntityPtr,
    };

    const cases = [_]Case{
        .{
            .key = 1,
            .table_entity_ptrs = &[_][]const EntityPtr{
                &[_]EntityPtr{ 0, 1 },
                &[_]EntityPtr{1},
            },
        },
        .{
            .key = 2,
            .table_entity_ptrs = &[_][]const EntityPtr{
                &[_]EntityPtr{4},
                &[_]EntityPtr{ 0, 3, 4 },
            },
        },
        .{
            .key = 3,
            .table_entity_ptrs = &[_][]const EntityPtr{
                &[_]EntityPtr{ 2, 3 },
            },
        },
        .{
            .key = 9,
            .table_entity_ptrs = &[_][]const EntityPtr{
                &[_]EntityPtr{2},
            },
        },
    };
    index_pool.insert(table_ptr_list[0], 0, keys_first_table[0..]);
    index_pool.blocks[table_ptr_list[0]].sort();

    index_pool.insert(table_ptr_list[1], 0, keys_second_table[0..]);
    index_pool.blocks[table_ptr_list[1]].sort();

    for (cases) |case| {
        const lookup_result = try index_pool.lookup(case.key);
        for (case.table_entity_ptrs, 0..) |entity_ptrs, table_indx| {
            if (entity_ptrs.len > 0) {
                for (entity_ptrs, 0..) |entity_ptr, entity_indx| {
                    try testing.expectEqual(entity_ptr, lookup_result.items[table_indx].entity_ptr_list[entity_indx].entity_ptr);
                }
            }
        }
    }
}

test "NonUniqueMemIndexType: pool: many entities" {
    const allocator = std.testing.allocator;

    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    const keys_max_count = 10_000;

    const table_ptr_list = [_]TablePtr{ 0, 1, 2, 3, 4 };

    const keys_per_block: EntityPtr = @intCast(keys_max_count / table_ptr_list.len);

    const Key = u32;
    const NonUniqueMemIndexU32 = NonUniqueMemIndexType(Key);

    const index_pool: *NonUniqueMemIndexU32.IndexPoolType(table_ptr_list.len, keys_per_block) = try .init(allocator, .{0,table_ptr_list.len});
    defer index_pool.deinit(allocator);

    // Preparing test input
    const entity_keys_unique_count = 1000;

    assert(entity_keys_unique_count <= keys_max_count);

    var entity_keys_unique = try allocator.alloc(Key, entity_keys_unique_count);
    defer allocator.free(entity_keys_unique);

    for (0..entity_keys_unique_count) |key_idx| {
        entity_keys_unique[key_idx] = random.int(Key);
    }

    var entity_keys = try allocator.alloc(Key, keys_max_count);
    defer allocator.free(entity_keys);

    var entity_keys_unique_idx: EntityPtr = 0;

    for (0..keys_max_count) |key_idx| {
        entity_keys[key_idx] = entity_keys_unique[entity_keys_unique_idx];
        entity_keys_unique_idx += 1;
        if (entity_keys_unique_idx == entity_keys_unique_count) entity_keys_unique_idx = 0;
    }

    const one_insert_count = 100;

    for (table_ptr_list) |table_ptr| {
        var entry_table_count: EntityPtr = 0;
        var insert_key_idx: EntityPtr = 0;

        while (insert_key_idx < keys_per_block) : (insert_key_idx += one_insert_count) {
            const insert = entity_keys[insert_key_idx .. insert_key_idx + one_insert_count];
            index_pool.insert(table_ptr, entry_table_count, insert);
            entry_table_count += one_insert_count;
        }

        index_pool.blocks[table_ptr].sort();
    }

    for (entity_keys_unique) |key| {
        const lookup_result = try index_pool.lookup(key);

        try testing.expectEqual(table_ptr_list.len, lookup_result.items.len);
        for (lookup_result.items, 0..) |block, i| {
            try testing.expectEqual(table_ptr_list[i], block.table_ptr);
            try testing.expect(block.entity_ptr_list.len > 0);
        }
    }
}
