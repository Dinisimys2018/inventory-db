const std = @import("std");
const Io = std.Io;
const net = Io.net;
const Allocator = std.mem.Allocator;
const posix = std.posix;

const Config = struct {
    port: u16 = 8019,
};

pub fn main(init: std.process.Init) !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    defer _ = debug_allocator.deinit();
    const gpa = debug_allocator.allocator();

    // Accessing command line arguments:
    const args = try init.minimal.args.toSlice(gpa);
    defer gpa.free(args);
    for (args) |arg| {
        std.log.info("arg: {s}", .{arg});
    }

    const io = init.io;
    const config: Config = .{};

    try command_start(io, gpa, config);
}

fn command_start(io: Io, allocator: Allocator, config: Config) !void {
    const address = try net.IpAddress.parse("0.0.0.0", config.port);
    var pool: PoolMsg = .init();
    defer pool.deinit(allocator, io);

    try listenAndPool(io, allocator, &pool, address);
}

fn listenAndPool(io: Io, allocator: Allocator, pool: *PoolMsg, address: net.IpAddress) !void {
    var server = try address.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    while (true) {
        const stream = try server.accept(io);
        const reader = try SocketReader.create(allocator, io, stream);
        pool.putOne(io, reader) catch |err| {
            reader.destroy(allocator, io);
            return err;
        };
    }
}

const PoolMsg = struct {
    const ReaderPtr = *SocketReader;
    const capacity = 64;

    queue: Io.Queue(ReaderPtr),
    buffer: [capacity]ReaderPtr = undefined,

    pub fn init() PoolMsg {
        var self: PoolMsg = .{
            .queue = undefined,
            .buffer = undefined,
        };
        self.queue = Io.Queue(ReaderPtr).init(self.buffer[0..]);
        return self;
    }

    pub fn deinit(self: *PoolMsg, allocator: Allocator, io: Io) void {
        self.queue.close(io);
        while (true) {
            const reader = self.queue.getOneUncancelable(io) catch |err| switch (err) {
                error.Closed => break,
            };
            reader.destroy(allocator, io);
        }
        self.* = undefined;
    }

    pub fn putOne(self: *PoolMsg, io: Io, reader: ReaderPtr) (Io.QueueClosedError || Io.Cancelable)!void {
        return self.queue.putOne(io, reader);
    }

    pub fn getOne(self: *PoolMsg, io: Io) (Io.QueueClosedError || Io.Cancelable)!ReaderPtr {
        return self.queue.getOne(io);
    }
};

const SocketReader = struct {
    stream: net.Stream,
    buf: []u8,
    reader: net.Stream.Reader,

    const buf_size = 4096;

    pub fn create(allocator: Allocator, io: Io, stream: net.Stream) !*SocketReader {
        const self = try allocator.create(SocketReader);
        errdefer allocator.destroy(self);

        const buf = try allocator.alloc(u8, buf_size);
        errdefer allocator.free(buf);

        self.* = .{
            .stream = stream,
            .buf = buf,
            .reader = stream.reader(io, buf),
        };
        return self;
    }

    pub fn destroy(self: *SocketReader, allocator: Allocator, io: Io) void {
        self.stream.close(io);
        allocator.free(self.buf);
        allocator.destroy(self);
    }
};

fn acceptOneIntoPool(io: Io, allocator: Allocator, server: *net.Server, pool: *PoolMsg) !void {
    const stream = try server.accept(io);
    const reader = try SocketReader.create(allocator, io, stream);
    pool.putOne(io, reader) catch |err| {
        reader.destroy(allocator, io);
        return err;
    };
}

fn createUnixSocketPairStreams() ![2]net.Stream {
    var fds: [2]posix.socket_t = undefined;
    while (true) switch (posix.errno(posix.system.socketpair(
        posix.AF.UNIX,
        posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
        0,
        &fds,
    ))) {
        .SUCCESS => break,
        .INTR => continue,
        .ACCES => return error.AccessDenied,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOBUFS, .NOMEM => return error.SystemResources,
        else => return error.Unexpected,
    };

    const dummy_addr: net.IpAddress = .{ .ip4 = net.Ip4Address.unspecified(0) };
    return .{
        .{ .socket = .{ .handle = fds[0], .address = dummy_addr } },
        .{ .socket = .{ .handle = fds[1], .address = dummy_addr } },
    };
}

test "PoolMsg: reader sees sent socket bytes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var pool: PoolMsg = .init();
    defer pool.deinit(allocator, io);

    const streams = try createUnixSocketPairStreams();
    const server_stream = streams[0];
    const client_stream = streams[1];
    defer client_stream.close(io);

    const payload = "hello from zig";
    var w_buf: [128]u8 = undefined;
    var w = client_stream.writer(io, w_buf[0..]);
    try w.interface.writeAll(payload);
    try w.interface.flush();
    
    const queued_reader = try SocketReader.create(allocator, io, server_stream);
    try pool.putOne(io, queued_reader);

    const pooled = try pool.getOne(io);
    defer pooled.destroy(allocator, io);

    var got: [payload.len]u8 = undefined;
    try pooled.reader.interface.readSliceAll(got[0..]);
    try std.testing.expectEqualStrings(payload, got[0..]);
}
