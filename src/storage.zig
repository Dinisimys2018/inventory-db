const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Io = std.Io;

const printObj = @import("utils/debug.zig").printObj;

pub fn StorageType(comptime module: []const u8) type {
    return struct {
        const Storage = @This();

        // FIELDS
        file: Io.File,

        pub fn init(allocator: std.mem.Allocator, io: Io, base_dir: Io.Dir,) !*Storage {
            const storage = try allocator.create(Storage);
            errdefer allocator.destroy(storage);
            const file_open = base_dir.openFile(io, module, .{});
            
            // !!! Data file can't be rewrited
            if (file_open) |_| {
                return error.DataFileExists;
            } else |err| {
                if (err != std.Io.File.OpenError.FileNotFound) {
                    return err;
                }
            }

            storage.file = try base_dir.createFile(io, module, .{});

            errdefer storage.file.close(io);

            return storage;
        }

        pub fn deinit(storage: *Storage, allocator: std.mem.Allocator, io: Io,) void {
            storage.file.close(io);
            allocator.destroy(storage);
        }

        pub fn write(storage: *Storage, io: Io) void {
            storage.
        }
    };
}

// TESTING

const OrderStorage = StorageType("orders");

test "Storage: check exists data file" {
    const allocator = testing.allocator;
    const io = testing.io;

    const tmp_dir = testing.tmpDir(.{});

    const storage: *OrderStorage  = try .init(allocator, io, tmp_dir.dir,);
    defer storage.deinit(allocator, io);

    //Can't create storage with same module names
    const storageDuplicateResult = OrderStorage.init(allocator, io,tmp_dir.dir,);
    try testing.expectError(error.DataFileExists, storageDuplicateResult);
}

test "Storage: check exists data file1" {
    const allocator = testing.allocator;
    const io = testing.io;

    const tmp_dir = testing.tmpDir(.{});

    const storage: *OrderStorage  = try .init(allocator, io, tmp_dir.dir,);
    defer storage.deinit(allocator, io);

}

