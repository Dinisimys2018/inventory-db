const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Writer = std.Io.Writer;
const Error = Writer.FileError;

const printObj = @import("utils/debug.zig").printObj;

const MemTablePtr = @import("mem_table.zig").MemTablePtr;
const MemEntryPtr = @import("mem_table.zig").MemEntryPtr;

pub fn StorageTableType() type {
    return struct {
        const StorageTable = @This();

        // FIELDS

        pub fn init(allocator: std.mem.Allocator) !*StorageTable {
            const storage_table = try allocator.create(StorageTable);

            return storage_table;
        }

        pub fn deinit(storage_table: *StorageTable, allocator: std.mem.Allocator) void {
            allocator.destroy(storage_table);
        }
    };
}
