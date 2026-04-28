const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Io = std.Io;
const Writer = std.Io.Writer;
const Error = Writer.FileError;

const printObj = @import("utils/debug.zig").printObj;

const Zone = struct {
    child: []*Zone,
    offset: usize,
    max_size: usize,

    pub fn init(allocator: std.mem.Allocator, offset: usize , child_len: usize) !*Zone {
        const zone = try allocator.create(Zone);
        zone.child.len = child_len;
        zone.offset = offset;
        zone.max_size = 0;

        if (child_len > 0) {
            zone.child = try allocator.alloc(*Zone, child_len);
        }
        return zone;
    }

    pub fn deinit(zone: *Zone, allocator: std.mem.Allocator) void {
        for (zone.child) |child_zone| {
            child_zone.deinit(allocator);
        }
        allocator.free(zone.child);
        allocator.destroy(zone);
    }
};

pub const TableLevel = struct {
    meta_zone_max_size: usize,
    data_zone_maz_size: usize,
};

pub fn StorageType(
    comptime table_levels: []const TableLevel,
    comptime module: []const u8,
    comptime buffer_size: usize,
) type {
    return struct {
        const Storage = @This();

        // FIELDS
        file: Io.File,
        buffer: []u8,
        zone: *Zone,

        pub fn init(allocator: std.mem.Allocator, io: Io, base_dir: Io.Dir) !*Storage {
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

            const file = try base_dir.createFile(io, module, .{});
            errdefer file.close(io);

            const storage = try allocator.create(Storage);
            storage.file = file;

            storage.buffer = try allocator.alloc(u8, buffer_size);

            storage.zone = try .init(allocator, 0,table_levels.len);

            var global_zone_ptr: usize = 0;

            inline for (table_levels) |table_level| {
                storage.zone.child[global_zone_ptr] = try .init(allocator, storage.zone.offset + storage.zone.max_size, 2,);

                var table_level_zone = storage.zone.child[global_zone_ptr];

                table_level_zone.child[0] = try .init(allocator, table_level_zone.offset + table_level_zone.max_size, 0,);
                var meta_zone = table_level_zone.child[0];

                meta_zone.max_size = table_level.meta_zone_max_size;
                table_level_zone.max_size += meta_zone.max_size;

                table_level_zone.child[1] = try .init(allocator, table_level_zone.offset + table_level_zone.max_size, 0,);
                var data_zone = table_level_zone.child[1];

                data_zone.max_size = table_level.meta_zone_max_size;
                table_level_zone.max_size += data_zone.max_size;

                storage.zone.max_size += table_level_zone.max_size;
                global_zone_ptr += 1;
            }

            return storage;
        }

        pub fn deinit(
            storage: *Storage,
            allocator: std.mem.Allocator,
            io: Io,
        ) void {
            storage.zone.deinit(allocator);
            allocator.free(storage.buffer);
            storage.file.close(io);
            allocator.destroy(storage);
        }

        pub fn streamMeta(storage: *Storage, io: Io, reader: anytype) Error!usize {
            var file_writer = storage.file.writerStreaming(io, storage.buffer);

            var total_streamed: usize = 0;
            while (true) {
                const n = reader.streamMeta(&file_writer.interface) catch |err| switch (err) {
                    error.EndOfStream => break,
                    else => return err,
                };
                total_streamed += n;
            }

            return total_streamed;
        }

        pub fn streamData(storage: *Storage, io: Io, reader: anytype) Error!usize {
            var file_writer = storage.file.writerStreaming(io, storage.buffer);

            var total_streamed: usize = 0;
            while (true) {
                const n = reader.streamData(&file_writer.interface) catch |err| switch (err) {
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

const test_table_levels = [_]TableLevel{
    .{
        .meta_zone_max_size = 100, //random
        .data_zone_maz_size = 100, //random
    },
};

const OrderStorage = StorageType(test_table_levels, "orders", 4 * 1024);

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
