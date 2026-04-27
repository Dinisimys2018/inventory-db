//TODO: - change all errors to Error Enum Type

///
///
///  DEPRECATED
///
///
///
const std = @import("std");
const ArrayList = std.array_list;
const assert = std.debug.assert;
const testing = std.testing;

const printObj = @import("utils/debug.zig").printObj;
const stdx_sort = @import("sort.zig");

const TablePtr = @import("mem_table.zig").MemTablePtr;
const EntityPtr = @import("mem_table.zig").MemEntryPtr;
const Entity = @import("entities.zig").OrderItem;
const OrderId = Entity.OrderId;
const ProductId = Entity.ProductId;

pub fn MemIndexType() type {
    return struct {
        const IndexValue = struct {
            order_id: OrderId,
            product_id: ProductId,
            entity_ptr: EntityPtr,

            pub fn compareKeys(order_id: OrderId, index_value: *IndexValue) std.math.Order {
                if (order_id == index_value.order_id) {
                    return .eq;
                } else if (order_id < index_value.order_id) {
                    return .lt;
                } else if (order_id > index_value.order_id) {
                    return .gt;
                } else {
                    unreachable;
                }
            }

            pub fn lessThan(_: @TypeOf(.{}), a: *IndexValue, b: *IndexValue) bool {
                if (a.order_id == b.order_id and a.product_id == b.product_id) {
                    return a.entity_ptr > b.entity_ptr;
                }
                if (a.order_id == b.order_id) return a.product_id > b.product_id;
                return a.order_id > b.order_id;
            }
        };

        pub const BlockLookupValue = struct {
            table_ptr: TablePtr,
            entries: []*IndexValue,
        };

        pub const LookupResult = std.ArrayList(BlockLookupValue);

        pub fn IndexBlockType(comptime keys_max_count: EntityPtr) type {
            return struct {
                const IndexBlock = @This();

                table_ptr: TablePtr,
                entries: []*IndexValue,
                next_entry_index: EntityPtr,
                has_keys: bool,
                min_order_id: OrderId,
                max_order_id: OrderId,
                min_product_id: ProductId,
                max_product_id: ProductId,
                sorted: bool,

                pub fn init(allocator: std.mem.Allocator, table_ptr: TablePtr) !*IndexBlock {
                    var index_block = try allocator.create(IndexBlock);

                    index_block.* = .{
                        .entries = try allocator.alloc(*IndexValue, keys_max_count),
                        .table_ptr = table_ptr,
                        .has_keys = false,
                        .next_entry_index = 0,
                        .max_order_id = 0,
                        .min_order_id = 0,
                        .min_product_id = 0,
                        .max_product_id = 0,
                        .sorted = false,
                    };

                    var entry_idx: EntityPtr = 0;

                    while (entry_idx < keys_max_count) : (entry_idx += 1) {
                        index_block.entries[entry_idx] = try allocator.create(IndexValue);
                        index_block.entries[entry_idx].* = .{
                            .order_id = 0,
                            .product_id = 0,
                            .entity_ptr = 0,
                        };
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
                    entities: []*Entity,
                ) void {
                    assert(entities.len > 0);
                    // Expected that the Index capacity is equal to the Table capacity.
                    assert((index_block.next_entry_index + entities.len) <= index_block.entries.len);

                    index_block.sorted = false;

                    if (!index_block.has_keys) {
                        index_block.min_order_id = entities[0].order_id;
                        index_block.max_order_id = entities[0].order_id;
                        index_block.min_product_id = entities[0].product_id;
                        index_block.max_product_id = entities[0].product_id;
                    }

                    var next_entry_ptr: EntityPtr = entry_table_count;

                    for (entities) |entity| {
                        if (entity.order_id < index_block.min_order_id) {
                            index_block.min_order_id = entity.order_id;
                        } else if (entity.order_id > index_block.max_order_id) {
                            index_block.max_order_id = entity.order_id;
                        }

                        if (entity.product_id < index_block.min_product_id) {
                            index_block.min_product_id = entity.product_id;
                        } else if (entity.product_id > index_block.max_product_id) {
                            index_block.max_product_id = entity.product_id;
                        }

                        index_block.entries[index_block.next_entry_index].order_id = entity.order_id;
                        index_block.entries[index_block.next_entry_index].product_id = entity.product_id;
                        //TODO: P1 Maybe fill field "entity_ptr" in each entity in MemTable instead of calculate
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

                pub fn lookupByOrderId(index_block: *IndexBlock, key: anytype) !BlockLookupValue {
                    assert(index_block.sorted);
                    const range = stdx_sort.equalRangeDesc(*IndexValue, index_block.entries, key, IndexValue.compareKeys);

                    if (range[1] == 0) return error.NotFound;

                    return .{
                        .table_ptr = index_block.table_ptr,
                        .entries = index_block.entries[range[0]..range[1]],
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
                // !!! only for sigle-thread mode
                lookup_result: LookupResult,

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

                pub fn insert(
                    index_pool: *IndexPool,
                    table_ptr: TablePtr,
                    entry_table_count: EntityPtr,
                    entities: []*Entity,
                ) void {
                    index_pool.blocks[table_ptr].insert(entry_table_count, entities);
                }

                pub fn sort(index_pool: *IndexPool, table_ptr: TablePtr) void {
                    index_pool.blocks[table_ptr].sort();
                }

                pub fn lookupByOrderId(index_pool: *IndexPool, key: OrderId) !*const LookupResult {
                    //TODO: P3 need to check how we can clear result not before lookupByOrderId, but after this
                    index_pool.lookup_result.clearRetainingCapacity();

                    var last_indx = index_pool.blocks.len;

                    while (last_indx > 0) {
                        last_indx -= 1;
                        if (key >= index_pool.blocks[last_indx].min_order_id and key <= index_pool.blocks[last_indx].max_order_id) {
                            const block_result = index_pool.blocks[last_indx].lookupByOrderId(key) catch continue;

                            index_pool.lookup_result.appendAssumeCapacity(block_result);
                        }
                    }

                    if (index_pool.lookup_result.items.len > 0) return &index_pool.lookup_result;

                    return error.NotFound;
                }
            };
        }
    };
}

// test "NonUniqueMemIndexType: pool: insert and lookupByOrderId" {
//     const allocator = std.testing.allocator;

//     const table_ptr_list = [_]TablePtr{ 0, 1 };

//     const keys_per_block: EntityPtr = 5;
//     const Key = u32;
//     const NonUniqueMemIndexU32 = MemIndexType(Key);

//     const index_pool: *NonUniqueMemIndexU32.IndexPoolType(table_ptr_list.len, keys_per_block) = try .init(allocator, .{ 0, 2 });
//     defer index_pool.deinit(allocator);

//     //                     entity ptrs => |0, 1, 2, 3, 4 |
//     var keys_first_table = [_]Key{ 1, 1, 3, 3, 2 };

//     //                      entity ptrs => |0, 1, 2, 3, 4 |
//     var keys_second_table = [_]Key{ 2, 1, 9, 2, 2 };

//     const ExpectedLookupResult = struct {
//         table_ptr: TablePtr,
//         entity_ptrs: []const EntityPtr,
//     };

//     const Case = struct {
//         key: Key,
//         expected_lookup_result: []const ExpectedLookupResult,
//     };

//     const cases = [_]Case{
//         .{
//             .key = 1,
//             .expected_lookup_result = &[_]ExpectedLookupResult{

//                 //     ptrs indexes  =>      0          <-- desc ordering
//                 //                           ↓
//                 //      entity ptrs  => | 0, 1, 2, 3, 4 |
//                 // keys_second_table => { 2, 1, 9, 2, 2 }
//                 //
//                 // ⬇ desc ordering |idx = 0, table_ptr = second|
//                 .{ .table_ptr = 1, .entity_ptrs = &[_]EntityPtr{1} }, // |idx = 0, ptr = 1|

//                 //     ptrs indexes =>   1  0          <-- desc ordering
//                 //                       ↓  ↓
//                 //      entity ptrs => | 0, 1, 2, 3, 4 |
//                 // keys_first_table => { 1, 1, 3, 3, 2 }
//                 //
//                 // ⬇ desc ordering |idx = 1, table_ptr = first|
//                 .{ .table_ptr = 0, .entity_ptrs = &[_]EntityPtr{ 1, 0 } }, // |idx = 0, ptr = 1| , |idx = 1 , ptr = 0|
//             },
//         },
//         .{
//             .key = 2,
//             .expected_lookup_result = &[_]ExpectedLookupResult{
//                 .{
//                     .table_ptr = 1,
//                     .entity_ptrs = &[_]EntityPtr{ 4, 3, 0 },
//                 },
//                 .{
//                     .table_ptr = 0,
//                     .entity_ptrs = &[_]EntityPtr{4},
//                 },
//             },
//         },
//         .{
//             .key = 3,
//             .expected_lookup_result = &[_]ExpectedLookupResult{
//                 .{
//                     .table_ptr = 0,
//                     .entity_ptrs = &[_]EntityPtr{ 3, 2 },
//                 },
//             },
//         },
//         .{
//             .key = 9,
//             .expected_lookup_result = &[_]ExpectedLookupResult{
//                 .{
//                     .table_ptr = 1,
//                     .entity_ptrs = &[_]EntityPtr{2},
//                 },
//             },
//         },
//     };
//     index_pool.insert(table_ptr_list[0], 0, keys_first_table[0..]);
//     index_pool.blocks[table_ptr_list[0]].sort();

//     index_pool.insert(table_ptr_list[1], 0, keys_second_table[0..]);
//     index_pool.blocks[table_ptr_list[1]].sort();

//     for (cases) |case| {
//         const lookup_result = try index_pool.lookupByOrderId(case.key);

//         for (case.expected_lookup_result, 0..) |expected_res, table_idx| {
//             for (expected_res.entity_ptrs, 0..) |entity_ptr, entity_idx| {
//                 try testing.expectEqual(
//                     entity_ptr,
//                     lookup_result.items[table_idx].entries[entity_idx].entity_ptr,
//                 );
//             }
//         }
//     }
// }

// test "NonUniqueMemIndexType: pool: many entities" {
//     const allocator = std.testing.allocator;

//     var prng = std.Random.DefaultPrng.init(0);
//     const random = prng.random();

//     const keys_max_count = 10_000;

//     const table_ptr_list = [_]TablePtr{ 0, 1, 2, 3, 4 };

//     const keys_per_block: EntityPtr = @intCast(keys_max_count / table_ptr_list.len);

//     const Key = u32;
//     const NonUniqueMemIndexU32 = NonUniqueMemIndexType(Key);

//     const index_pool: *NonUniqueMemIndexU32.IndexPoolType(table_ptr_list.len, keys_per_block) = try .init(allocator, .{ 0, table_ptr_list.len });
//     defer index_pool.deinit(allocator);

//     // Preparing test input
//     const entity_keys_unique_count = 1000;

//     assert(entity_keys_unique_count <= keys_max_count);

//     var entity_keys_unique = try allocator.alloc(Key, entity_keys_unique_count);
//     defer allocator.free(entity_keys_unique);

//     for (0..entity_keys_unique_count) |key_idx| {
//         entity_keys_unique[key_idx] = random.int(Key);
//     }

//     var entity_keys = try allocator.alloc(Key, keys_max_count);
//     defer allocator.free(entity_keys);

//     var entity_keys_unique_idx: EntityPtr = 0;

//     for (0..keys_max_count) |key_idx| {
//         entity_keys[key_idx] = entity_keys_unique[entity_keys_unique_idx];
//         entity_keys_unique_idx += 1;
//         if (entity_keys_unique_idx == entity_keys_unique_count) entity_keys_unique_idx = 0;
//     }

//     const one_insert_count = 100;

//     for (table_ptr_list) |table_ptr| {
//         var entry_table_count: EntityPtr = 0;
//         var insert_key_idx: EntityPtr = 0;

//         while (insert_key_idx < keys_per_block) : (insert_key_idx += one_insert_count) {
//             const insert = entity_keys[insert_key_idx .. insert_key_idx + one_insert_count];
//             index_pool.insert(table_ptr, entry_table_count, insert);
//             entry_table_count += one_insert_count;
//         }

//         index_pool.blocks[table_ptr].sort();
//     }

//     for (entity_keys_unique) |key| {
//         const lookup_result = try index_pool.lookupByOrderId(key);

//         try testing.expectEqual(table_ptr_list.len, lookup_result.items.len);
//         //Desc ordering on results
//         var table_idx_expect: u32 = table_ptr_list.len;

//         for (lookup_result.items) |block| {
//             table_idx_expect -= 1;

//             try testing.expectEqual(table_ptr_list[table_idx_expect], block.table_ptr);
//             try testing.expect(block.entries.len > 0);
//         }
//     }
// }
