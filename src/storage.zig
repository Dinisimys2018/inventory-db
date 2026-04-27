const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Io = std.Io;
const Writer = std.Io.Writer;
const Error = Writer.FileError;

const printObj = @import("utils/debug.zig").printObj;

pub fn StorageType(comptime module: []const u8, comptime buffer_size: usize) type {
    return struct {
        const Storage = @This();

        // FIELDS
        file: Io.File,
        buffer: []u8,

        pub fn init(allocator: std.mem.Allocator, io: Io, base_dir: Io.Dir) !*Storage {
            const storage = try allocator.create(Storage);
            const file_open = base_dir.openFile(io, module, .{});

            // !!! Data file can't be rewrited
            if (file_open) |existing| {
                existing.close(io);
                return error.DataFileExists;
            } else |err| {
                if (err != std.Io.File.OpenError.FileNotFound) {
                    return err;
                }
            }

            storage.file = try base_dir.createFile(io, module, .{});
            errdefer storage.file.close(io);

            storage.buffer = try allocator.alloc(u8, buffer_size);

            return storage;
        }

        pub fn deinit(
            storage: *Storage,
            allocator: std.mem.Allocator,
            io: Io,
        ) void {
            allocator.free(storage.buffer);
            storage.file.close(io);
            allocator.destroy(storage);
        }

        pub fn streamFrom(storage: *Storage, io: Io, reader: anytype) Error!usize {
            var file_writer = storage.file.writerStreaming(io, storage.buffer);

            var total_streamed: usize = 0;
            while (true) {
                const n = reader.stream(&file_writer.interface) catch |err| switch (err) {
                    error.EndOfStream => break,
                    else => return err,
                };
                total_streamed += n;
            }

            return total_streamed;
        }
    };
}

// TESTING

const OrderStorage = StorageType("orders");

test "Storage: check exists data file" {
    const allocator = testing.allocator;
    const io = testing.io;

    const tmp_dir = testing.tmpDir(.{});

    const storage: *OrderStorage = try .init(
        allocator,
        io,
        tmp_dir.dir,
    );
    defer storage.deinit(allocator, io);

    //Can't create storage with same module names
    const storageDuplicateResult = OrderStorage.init(
        allocator,
        io,
        tmp_dir.dir,
    );
    try testing.expectError(error.DataFileExists, storageDuplicateResult);
}

test "Storage: check exists data file1" {
    const allocator = testing.allocator;
    const io = testing.io;

    const tmp_dir = testing.tmpDir(.{});

    const storage: *OrderStorage = try .init(
        allocator,
        io,
        tmp_dir.dir,
    );
    defer storage.deinit(allocator, io);
}
