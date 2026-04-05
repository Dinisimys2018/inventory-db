const std = @import("std");
const Io = std.Io;
const net = Io.net;
const some_db = @import("some_db");
const Config = struct {
    port: u16 = 8019,
};

pub fn main(init: std.process.Init) !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const gpa = debug_allocator.allocator();


    // Accessing command line arguments:
    const args = try init.minimal.args.toSlice(gpa);
    for (args) |arg| {
        std.log.info("arg: {s}", .{arg});
    }

    const io = init.io;
    const config: Config = .{};

    try command_start(io, config);
}


fn command_start(io: std.Io, config: Config) !void {
    const address = try net.IpAddress.parse("0.0.0.0", config.port);
    var server = try net.IpAddress.listen(address, io, .{ .reuse_address = true });
    defer server.deinit(io);
}