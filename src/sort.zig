const std = @import("std");
const assert = std.debug.assert;

pub fn sort(
    comptime T: type,
    values: []T,
    comptime lessThanFn: fn (_: void, lhs: T, rhs: T) bool,
) void {
    return std.sort.pdq(T, values, {}, lessThanFn);
}


test "sort: structs with key u32" {
    const Entity = struct {
        const EntityType = @This();
        field_key: u32,
        field_value: u8,

        pub fn lessThan(_: void, a: EntityType, b: EntityType) bool {
            return a.field_key < b.field_key;
        }
    };

    var actual = [_]Entity{
        .{
            .field_key = 10,
            .field_value = 1,
        },
        .{
            .field_key = 2,
            .field_value = 1,
        },
        .{
            .field_key = 256,
            .field_value = 1,
        },
        .{
            .field_key = 15,
            .field_value = 1,
        },
        .{
            .field_key = 1501,
            .field_value = 1,
        },
    };

    var expected = [_]Entity{
        .{
            .field_key = 2,
            .field_value = 1,
        },
        .{
            .field_key = 10,
            .field_value = 1,
        },
        .{
            .field_key = 15,
            .field_value = 1,
        },
        .{
            .field_key = 256,
            .field_value = 1,
        },
        .{
            .field_key = 1501,
            .field_value = 1,
        },
    };

    sort(Entity, actual[0..], Entity.lessThan);

    try std.testing.expectEqualSlices(Entity, expected[0..], actual[0..]);
}

test "benchmark: sort large struct array" {

    const Entity = struct {
        const Self = @This();
        key: u32,
        value: u32,

        pub fn lessThan(_: void, a: Self, b: Self) bool {
            return a.key < b.key;
        }
    };

    const count: usize = 10_000_000;
    const allocator = std.testing.allocator;

    var items = try allocator.alloc(Entity, count);
    defer allocator.free(items);

    var prng = std.Random.DefaultPrng.init(std.testing.random_seed);
    const random = prng.random();
    for (items, 0..) |*item, idx| {
        item.* = .{
            .key = random.int(u32),
            .value = @intCast(idx),
        };
    }

    const Io = std.Io;
    const io = std.testing.io;
    const start_ns = Io.Clock.awake.now(io).nanoseconds;
    sort(Entity, items,  Entity.lessThan);
    const end_ns = Io.Clock.awake.now(io).nanoseconds;
    const elapsed_ns = end_ns - start_ns;
    const elapsed_ms: u64 = @intCast(@divTrunc(elapsed_ns, std.time.ns_per_ms));
    
    std.debug.print(
        "benchmark: sorted {d} entities in {d} ms\n",
        .{ count, elapsed_ms },
    );

    for (items[1..], 0..) |curr, i| {
        const prev = items[i];
        try std.testing.expect(prev.key <= curr.key);
    }

  
}