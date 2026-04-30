const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Writer = std.Io.Writer;
const Error = Writer.FileError;

const printObj = @import("utils/debug.zig").printObj;

const Entity = @import("order_item_entity.zig").OrderItem;
const index_table = @import("index_table.zig");

      const IndexTable = index_table.IndexTableWithTwoKeysType(
            Entity,
            Entity.OrderId,
            Entity.ProductId,
            "order_id",
            "product_id",
        );
pub fn StorageTableType() type {

    return struct {
        const StorageTable = @This();

        // FIELDS
        index: *IndexTable,

        pub fn init(allocator: std.mem.Allocator) !*StorageTable {
            const storage_table = try allocator.create(StorageTable);
            storage_table.index = try .init(allocator);
            return storage_table;
        }

        pub fn deinit(storage_table: *StorageTable, allocator: std.mem.Allocator) void {
            storage_table.index.deinit(allocator);
            allocator.destroy(storage_table);
        }
    };
}
