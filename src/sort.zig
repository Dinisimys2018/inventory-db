const std = @import("std");
const assert = std.debug.assert;

pub fn equalRangeDesc(
    comptime T: type,
    items: []const T,
    context: anytype,
    comptime compareFn: fn (@TypeOf(context), T) std.math.Order,
) struct { usize, usize } {
    // NOTE: Масив `items` відсортований за спаданням ключа (desc).
    // Для відсутнього ключа ця функція повертає `{0, 0}` (порожній діапазон),
    // а не позицію вставки.

    if (items.len == 0) return .{ 0, 0 };

    // lower bound (desc): перший індекс, де key == context (якщо існує).
    var left: usize = 0;
    var right: usize = items.len;
    while (left < right) {
        const mid = left + (right - left) / 2;
        switch (compareFn(context, items[mid])) {
            .gt => right = mid, // context > item => шукаємо лівіше
            .lt => left = mid + 1, // context < item => шукаємо правіше
            .eq => right = mid, // звужуємо вліво до першого входження
        }
    }
    const start = left;
    if (start >= items.len) return .{ 0, 0 };
    if (compareFn(context, items[start]) != .eq) return .{ 0, 0 };

    // upper bound (desc): перший індекс після останнього `== context`.
    left = start;
    right = items.len;
    while (left < right) {
        const mid = left + (right - left) / 2;
        switch (compareFn(context, items[mid])) {
            .gt => right = mid,
            .lt => left = mid + 1,
            .eq => left = mid + 1,
        }
    }

    return .{ start, left };
}


test "equalRangeDesc with struct" {
    const TestValue = struct {
        key: u32,
        value: u32,
    };

    const items = [_]TestValue{
        .{ .key = 10, .value = 1 },
        .{ .key = 10, .value = 2 },
        .{ .key = 10, .value = 3 },
        .{ .key = 8,  .value = 4 },
        .{ .key = 8,  .value = 5 },
        .{ .key = 8,  .value = 6 },
        .{ .key = 7,  .value = 7 },
        .{ .key = 7,  .value = 8 },
        .{ .key = 7,  .value = 9 },
    };

    const compareFn = struct {
        pub fn compare(ctx: u32, item: TestValue) std.math.Order {
            return std.math.order(ctx, item.key);
        }
    }.compare;

    // key = 10 → {0, 3}
    const r1 = equalRangeDesc(TestValue, &items, @as(u32, 10), compareFn);
    try std.testing.expectEqual(@as(usize, 0), r1[0]);
    try std.testing.expectEqual(@as(usize, 3), r1[1]);

    // key = 8 → {3, 6}
    const r2 = equalRangeDesc(TestValue, &items, @as(u32, 8), compareFn);
    try std.testing.expectEqual(@as(usize, 3), r2[0]);
    try std.testing.expectEqual(@as(usize, 6), r2[1]);

    // key = 7 → {6, 9}
    const r3 = equalRangeDesc(TestValue, &items, @as(u32, 7), compareFn);
    try std.testing.expectEqual(@as(usize, 6), r3[0]);
    try std.testing.expectEqual(@as(usize, 9), r3[1]);

    // key відсутній (9)
    const r4 = equalRangeDesc(TestValue, &items, @as(u32, 9), compareFn);
    try std.testing.expectEqual(0, r4[0]);
    try std.testing.expectEqual(0, r4[1]);


    // key менший за всі (5) → відсутній
    const r5 = equalRangeDesc(TestValue, &items, @as(u32, 5), compareFn);
    try std.testing.expectEqual(0, r5[0]);
    try std.testing.expectEqual(0, r5[1]);

    // key більший за всі (11) → відсутній
    const r6 = equalRangeDesc(TestValue, &items, @as(u32, 11), compareFn);
    try std.testing.expectEqual(0, r6[0]);
    try std.testing.expectEqual(0, r6[1]);
}
