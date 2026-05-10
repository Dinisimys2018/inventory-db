const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const assert = std.debug.assert;

const log = @import("utils/debug.zig").ModulePrinterType(.scheduler);
const module = @import("module.zig");


pub fn SchedulerType(comptime config: *const module.ConfigModule) type {
    const Components = config.Components();
    _ = Components;

    return struct {
        const Scheduler = @This();

        const Task = struct {
            ptr: *anyopaque,
            vtable: *const TaskInterface,

            pub const TaskInterface = struct {
                deinit: *const fn (ptr: *anyopaque, allocator: Allocator) void,
                tick: *const fn (ptr: *anyopaque, now_ms: u64) void,
            };
        };

        const Mode = union(enum) {
            every_millisec: u64,
            every_sec: u64,
        };

        const State = enum {
            initied,
            started,
            to_stop,
            stopped,
        };

        // FIELDS
        state: State,
        tasks: std.ArrayList(Task),

        pub fn init(allocator: Allocator) !*Scheduler {
            const scheduler = try allocator.create(Scheduler);
            scheduler.state = .initied;
            scheduler.tasks = .empty;

            return scheduler;
        }

        pub fn deinit(scheduler: *Scheduler, allocator: Allocator) void {
            scheduler.state = .to_stop;

            for (scheduler.tasks.items) |task| {
                task.vtable.deinit(task.ptr, allocator); // знає реальний тип
            }
            scheduler.tasks.deinit(allocator);
            allocator.destroy(scheduler);
        }

        pub fn appendTask(
            scheduler: *Scheduler,
            allocator: Allocator,
            function: anytype,
            args: std.meta.ArgsTuple(@TypeOf(function)),
            mode: Mode,
        ) !void {
            assert(scheduler.state == .initied);

            const interval_ms = switch (mode) {
                .every_millisec => |ms| ms,
                .every_sec => |s| s * 1000,
            };

            const Args = @TypeOf(args);

            const TaskEarsed = struct {
                interval_ms: u64,
                prev_run: u64,
                args: Args,

                pub fn taskTick(ptr: *anyopaque, now_ms: u64) void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
     if (now_ms - self.prev_run >= self.interval_ms) {
                     _ = @as(Io.Cancelable!void, @call(.auto, function, self.args)) catch {};                        self.prev_run = now_ms;
                    }                }

                pub fn taskDeinit(ptr: *anyopaque, alloc: Allocator) void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    alloc.destroy(self); // тут тип відомий!
                }

            };

            const task = try allocator.create(TaskEarsed);
            task.* = .{ .interval_ms = interval_ms, .prev_run = 0, .args = args };

            try scheduler.tasks.append(allocator, .{
                .ptr = task,
                .vtable = &.{
                    .deinit = TaskEarsed.taskDeinit,
                    .tick = TaskEarsed.taskTick,
                }
            });
        }
        pub fn start(scheduler: *Scheduler, io: Io) void {
            scheduler.state = .started;

            while (scheduler.state == .started) {
                var tick_future = io.async(Scheduler.tick, .{scheduler, io});
                _ = tick_future.await(io);
                io.sleep(std.Io.Duration.fromMilliseconds(5), .awake) catch return;
            }
        }

        pub fn tick(
            scheduler: *Scheduler,
            io: Io,
        ) void {
            const now_ms: u64 = @intCast(Io.Clock.awake.now(io).toMilliseconds());
            log.obj("RUN tick now_ms", now_ms);
            assert(scheduler.state == .started);

            for (scheduler.tasks.items) |task| {
                task.vtable.tick(task.ptr, now_ms);
            }

        }
    };
}
