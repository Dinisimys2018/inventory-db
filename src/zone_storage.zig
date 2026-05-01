const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Io = std.Io;

const module = @import("module.zig");

pub const Zone = struct {
    offset: usize,
    position: usize,
    max_size: usize,

    pub fn init(allocator: Allocator, offset: usize, max_size: usize) !*Zone {
        const zone = try allocator.create(Zone);
        zone.offset = offset;
        zone.max_size = max_size;
        zone.position = 0;
        return zone;
    }

    pub fn deinit(zone: *Zone, allocator: Allocator) void {
        allocator.destroy(zone);
    }
};

pub const ZoneKey = enum {
    headers,
    index_tables_level_0,
    data_tables_level_0,
};

pub fn GlobalZoneType(comptime config: *const module.ConfigModule) type {
    _ = config;

    return struct {
        const GlobalZone = @This();

        // FIELDS
        map_zones: std.EnumMap(ZoneKey, *Zone),
        offset: usize,
        max_size: usize,
        next_zone_offset: usize,

        pub fn init(allocator: Allocator, offset: usize) !*GlobalZone {
            var global_zone = try allocator.create(GlobalZone);
            global_zone.offset = offset;
            global_zone.max_size = 0;
            global_zone.next_zone_offset = 0;
            global_zone.map_zones = .init(.{});
            
            return global_zone;
        }

        pub fn deinit(global_zone: *GlobalZone, allocator: Allocator) void {
            var map_zones_iter = global_zone.map_zones.iterator();
            while(map_zones_iter.next()) |entry| {
                entry.value.*.deinit(allocator);
            }
            allocator.destroy(global_zone);
        }

        pub fn initZone(global_zone: *GlobalZone, allocator: Allocator, key: ZoneKey, max_size: usize,) !void {
            var zone = try allocator.create(Zone);
            global_zone.map_zones.put(key, zone);
            zone.max_size = max_size;
            zone.offset = global_zone.next_zone_offset;
            zone.position = 0;

            global_zone.next_zone_offset += max_size;
        }

        pub fn getZone(global_zone: *GlobalZone, key: ZoneKey) *Zone {
            return global_zone.map_zones.get(key) orelse unreachable;
        }
    };
}
