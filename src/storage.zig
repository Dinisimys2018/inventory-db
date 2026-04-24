const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const Io = std.Io;
const File = std.Io.File;

const printObj = @import("utils/debug.zig").printObj;

const MemTablePoolType = @import("order_item_mem_table.zig").MemTablePoolType;

pub fn StorageType(
    comptime io: Io,
    comptime base_dir_path: []const u8,
) type {
    return struct {
        const Storage = @This();

        // FIELDS
        base_dir: Io.Dir,

        pub fn init(allocator: std.mem.Allocator) *Storage {
            const storage = try allocator.create(Storage);

            return storage;
        }

        pub fn deinit(storage: *Storage, allocator: std.mem.Allocator) void {
            allocator.destroy(storage);
        }

        pub fn write(storage: *Storage) void {
            
        }
    };
}
