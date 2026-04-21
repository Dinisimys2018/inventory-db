const std = @import("std");
/// TODO: P3
/// - write as JSON to filesystem
pub fn SpeedBenchmarkType(
    comptime module: []const u8,
    comptime action: []const u8,
    comptime max_points: u8,
    comptime Value: type,
    comptime value_name: []const u8,
) type {
    return struct {
        const SpeedBenchmark = @This();
        const Point = struct {
            time_ms: i64,
            value: Value,
        };

        //Fields
        module: []u8,
        action: []u8,
        value_name: []const u8,
        points: []*Point,

        pub fn init(allocator: std.mem.Allocator) !*SpeedBenchmark {
            var benchmark = try allocator.create(SpeedBenchmark);
            benchmark.module = module;
            benchmark.action = action;
            benchmark.value_name = value_name;
            benchmark.points = try allocator.alloc(*Point, max_points);

            var point_idx: usize = 0;
            while(point_idx < max_points): (point_idx +=1) {
                benchmark.points[point_idx] = try allocator.create(Point);
            }

            return benchmark;
        }

        pub fn to_json(benchmark: *SpeedBenchmark) std.json.Formatter(SpeedBenchmark) {
            return std.json.fmt(benchmark, .{});
        }
    };
}
