const std = @import("std");

pub fn SkipListType(
    comptime EntryType: type,
    comptime KeyType: type,
    comptime keyOf: fn (*const EntryType) KeyType,
    comptime lessThan: fn (KeyType, KeyType) bool,
) type {
    return struct {
        const Self = @This();

        const max_level: usize = 16;
        const p: f32 = 0.5;

        const Node = struct {
            value: EntryType,
            next: []?*Node,
            level: usize,
        };

        allocator: std.mem.Allocator,
        head: *Node,
        level: usize = 1,
        len: usize = 0,
        prng: std.Random.DefaultPrng,

        pub fn init(allocator: std.mem.Allocator, seed: u64) !Self {
            const head_node = try allocator.create(Node);
            errdefer allocator.destroy(head_node);

            const next = try allocator.alloc(?*Node, max_level);
            errdefer allocator.free(next);

            for (next) |*slot| slot.* = null;

            head_node.* = .{
                .value = undefined,
                .next = next,
                .level = max_level,
            };

            return .{
                .allocator = allocator,
                .head = head_node,
                .level = 1,
                .len = 0,
                .prng = std.Random.DefaultPrng.init(seed),
            };
        }

        pub fn deinit(self: *Self) void {
            self.clear();
            self.allocator.free(self.head.next);
            self.allocator.destroy(self.head);
            self.* = undefined;
        }

        pub fn clear(self: *Self) void {
            var node_opt = self.head.next[0];
            while (node_opt) |node| {
                const next = node.next[0];
                self.allocator.free(node.next);
                self.allocator.destroy(node);
                node_opt = next;
            }

            for (self.head.next) |*slot| slot.* = null;
            self.level = 1;
            self.len = 0;
        }

        pub fn count(self: *const Self) usize {
            return self.len;
        }

        pub fn contains(self: *Self, key: KeyType) bool {
            return self.get(key) != null;
        }

        pub fn get(self: *Self, key: KeyType) ?*EntryType {
            var current = self.head;
            var lvl: isize = @as(isize, @intCast(self.level)) - 1;
            while (lvl >= 0) : (lvl -= 1) {
                const i: usize = @intCast(lvl);
                while (current.next[i]) |next| {
                    const next_key = keyOf(&next.value);
                    if (compareKeys(next_key, key) < 0) {
                        current = next;
                    } else {
                        break;
                    }
                }
            }

            if (current.next[0]) |next| {
                if (compareKeys(keyOf(&next.value), key) == 0) {
                    return &next.value;
                }
            }
            return null;
        }

        pub fn insert(self: *Self, value: EntryType) !*EntryType {
            var update: [max_level]?*Node = undefined;
            var current = self.head;

            var lvl: isize = @as(isize, @intCast(self.level)) - 1;
            while (lvl >= 0) : (lvl -= 1) {
                const i: usize = @intCast(lvl);
                while (current.next[i]) |next| {
                    const next_key = keyOf(&next.value);
                    if (compareKeys(next_key, keyOf(&value)) < 0) {
                        current = next;
                    } else {
                        break;
                    }
                }
                update[i] = current;
            }

            if (current.next[0]) |next| {
                if (compareKeys(keyOf(&next.value), keyOf(&value)) == 0) {
                    return error.DuplicateKey;
                }
            }

            const node_level = self.randomLevel();
            if (node_level > self.level) {
                for (self.level..node_level) |i| {
                    update[i] = self.head;
                }
                self.level = node_level;
            }

            var node = try self.allocator.create(Node);
            errdefer self.allocator.destroy(node);
            const next = try self.allocator.alloc(?*Node, node_level);
            errdefer self.allocator.free(next);
            for (next) |*slot| slot.* = null;

            node.* = .{
                .value = value,
                .next = next,
                .level = node_level,
            };

            for (0..node_level) |i| {
                node.next[i] = update[i].?.next[i];
                update[i].?.next[i] = node;
            }

            self.len += 1;
            return &node.value;
        }

        pub fn insertOrReplace(self: *Self, value: EntryType) !*EntryType {
            var update: [max_level]?*Node = undefined;
            var current = self.head;

            var lvl: isize = @as(isize, @intCast(self.level)) - 1;
            while (lvl >= 0) : (lvl -= 1) {
                const i: usize = @intCast(lvl);
                while (current.next[i]) |next| {
                    const next_key = keyOf(&next.value);
                    if (compareKeys(next_key, keyOf(&value)) < 0) {
                        current = next;
                    } else {
                        break;
                    }
                }
                update[i] = current;
            }

            if (current.next[0]) |next| {
                if (compareKeys(keyOf(&next.value), keyOf(&value)) == 0) {
                    next.value = value;
                    return &next.value;
                }
            }

            const node_level = self.randomLevel();
            if (node_level > self.level) {
                for (self.level..node_level) |i| {
                    update[i] = self.head;
                }
                self.level = node_level;
            }

            var node = try self.allocator.create(Node);
            errdefer self.allocator.destroy(node);
            const next = try self.allocator.alloc(?*Node, node_level);
            errdefer self.allocator.free(next);
            for (next) |*slot| slot.* = null;

            node.* = .{
                .value = value,
                .next = next,
                .level = node_level,
            };

            for (0..node_level) |i| {
                node.next[i] = update[i].?.next[i];
                update[i].?.next[i] = node;
            }

            self.len += 1;
            return &node.value;
        }

        pub fn remove(self: *Self, key: KeyType) bool {
            var update: [max_level]?*Node = undefined;
            var current = self.head;

            var lvl: isize = @as(isize, @intCast(self.level)) - 1;
            while (lvl >= 0) : (lvl -= 1) {
                const i: usize = @intCast(lvl);
                while (current.next[i]) |next| {
                    const next_key = keyOf(&next.value);
                    if (compareKeys(next_key, key) < 0) {
                        current = next;
                    } else {
                        break;
                    }
                }
                update[i] = current;
            }

            const target = current.next[0] orelse return false;
            if (compareKeys(keyOf(&target.value), key) != 0) return false;

            for (0..self.level) |i| {
                if (update[i].?.next[i] == target) {
                    update[i].?.next[i] = target.next[i];
                }
            }

            self.allocator.free(target.next);
            self.allocator.destroy(target);
            self.len -= 1;

            while (self.level > 1 and self.head.next[self.level - 1] == null) {
                self.level -= 1;
            }

            return true;
        }

        fn compareKeys(a: KeyType, b: KeyType) i8 {
            if (lessThan(a, b)) return -1;
            if (lessThan(b, a)) return 1;
            return 0;
        }

        fn randomLevel(self: *Self) usize {
            var lvl: usize = 1;
            const rng = self.prng.random();
            while (lvl < max_level and rng.float(f32) < p) {
                lvl += 1;
            }
            return lvl;
        }
    };
}

pub fn OrderIdSkipListType(comptime EntryType: type) type {
    comptime {
        if (!@hasField(EntryType, "order_id")) {
            @compileError("EntryType must have field 'order_id'");
        }
    }

    const KeyType = @TypeOf(@field(@as(EntryType, undefined), "order_id"));
    const Helpers = struct {
        fn key(entry: *const EntryType) KeyType {
            return entry.order_id;
        }

        fn lessThan(a: KeyType, b: KeyType) bool {
            return a < b;
        }
    };

    return SkipListType(EntryType, KeyType, Helpers.key, Helpers.lessThan);
}

// ==== Tests ====
const OrderItemRow = @import("entities.zig").OrderItemRow;

test "skip_list: insert/find/remove by order_id" {
    const allocator = std.testing.allocator;
    const List = OrderIdSkipListType(OrderItemRow);
    var list = try List.init(allocator, 12345);
    defer list.deinit();

    _ = try list.insert(.{
        .time_label = 1,
        .order_id = 10,
        .product_id = 1,
        .quantity = 2,
        .price = 100,
        .total = 200,
    });
    _ = try list.insert(.{
        .time_label = 1,
        .order_id = 5,
        .product_id = 2,
        .quantity = 1,
        .price = 150,
        .total = 150,
    });
    _ = try list.insert(.{
        .time_label = 1,
        .order_id = 20,
        .product_id = 3,
        .quantity = 3,
        .price = 50,
        .total = 150,
    });

    const found = list.get(10) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 10), found.order_id);
    std.debug.print("\n{any}\n", .{list.head.next});
    try std.testing.expect(list.remove(10));
    try std.testing.expect(list.get(10) == null);
    try std.testing.expectEqual(@as(usize, 2), list.count());
}

test "skip_list: duplicate key rejected" {
    const allocator = std.testing.allocator;
    const List = OrderIdSkipListType(OrderItemRow);
    var list = try List.init(allocator, 7);
    defer list.deinit();

    _ = try list.insert(.{
        .time_label = 1,
        .order_id = 1,
        .product_id = 1,
        .quantity = 1,
        .price = 100,
        .total = 100,
    });
    try std.testing.expectError(error.DuplicateKey, list.insert(.{
        .time_label = 2,
        .order_id = 1,
        .product_id = 2,
        .quantity = 2,
        .price = 200,
        .total = 400,
    }));
}
