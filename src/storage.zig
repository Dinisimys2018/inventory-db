const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Io = std.Io;
const Writer = std.Io.Writer;
const Error = Writer.FileError;

const printObj = @import("utils/debug.zig").printObj;
pub const zone_storage = @import("zone_storage.zig");

pub fn StorageType(
    comptime GlobalZone: type,
    comptime module_name: []const u8,
    comptime buffer_size: usize,
) type {
    return struct {
        const Storage = @This();

        // FIELDS
        file: Io.File,
        buffer: []u8,
        global_zone: *GlobalZone,

        pub fn init(allocator: std.mem.Allocator, io: Io, base_dir: Io.Dir, global_zone: *GlobalZone) !*Storage {
            const file_open = base_dir.openFile(io, module_name, .{});

            // !!! Data file can't be rewrited
            if (file_open) |existing| {
                existing.close(io);
                return error.DataFileExists;
            } else |err| {
                if (err != std.Io.File.OpenError.FileNotFound) {
                    return err;
                }
            }

            const file = try base_dir.createFile(io, module_name, .{});
            errdefer file.close(io);

            const storage = try allocator.create(Storage);
            storage.file = file;
            storage.global_zone = global_zone;

            storage.buffer = try allocator.alloc(u8, buffer_size);
            return storage;
        }

        pub fn deinit(
            storage: *Storage,
            allocator: std.mem.Allocator,
            io: Io,
        ) void {
            storage.global_zone.deinit(allocator);
            allocator.free(storage.buffer);
            storage.file.close(io);
            allocator.destroy(storage);
        }

        pub fn streamToZone(storage: *Storage, io: Io, zone_key: zone_storage.ZoneKey, reader: anytype) !usize {
            var zone: *zone_storage.Zone = storage.global_zone.getZone(zone_key);

            var file_writer = storage.file.writerStreaming(io, storage.buffer);

            try file_writer.seekTo(zone.offset + zone.position);

            var total_streamed_bytes: usize = 0;
            while (true) {
                const n = reader.stream(&file_writer.interface) catch |err| switch (err) {
                    error.EndOfStream => break,
                    else => return 0,
                };
                total_streamed_bytes += n;
            }

            zone.position += total_streamed_bytes;

            return total_streamed_bytes;
        }
    };
}

// TESTING

fn testRenderMapZones(allocator: std.mem.Allocator) !zone_storage.MapZones {
    var map_zones: zone_storage.MapZones = .init(.{});
    var global_offset: usize = 0;

    const meta_tables_level_0: *zone_storage.Zone = try .init(allocator, global_offset , 100);
    map_zones.put(.meta_tables_level_0, meta_tables_level_0);

    global_offset += meta_tables_level_0.max_size;
    
    const data_tables_level_0: *zone_storage.Zone = try .init(allocator, global_offset , 100);
    map_zones.put(.data_tables_level_0, data_tables_level_0);
    
    global_offset += data_tables_level_0.max_size;

    return map_zones;
}

const OrderStorage =  StorageType("orders", 4 * 1024);


test "Storage: check exists data file" {
    const allocator = testing.allocator;
    const io = testing.io;

    const tmp_dir = testing.tmpDir(.{});
    const map_zones = try testRenderMapZones(allocator);
    defer {
        for(map_zones.values) |zone| {
            allocator.destroy(zone);
        }
    }

    const storage: *OrderStorage = try .init(
        allocator,
        io,
        tmp_dir.dir,
        map_zones,
    );
    defer storage.deinit(allocator, io);

    //Can't create storage with same module names
    const storageDuplicateResult = OrderStorage.init(
        allocator,
        io,
        tmp_dir.dir,
        map_zones,
    );
    try testing.expectError(error.DataFileExists, storageDuplicateResult);
}
