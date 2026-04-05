const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const net = Io.net;
const linux = std.os.linux;

pub const Journal = struct {
    file: Io.File,
    write_offset: u64,
    
    pub fn init(io: Io, dir: Io.Dir, path: []const u8) !Journal {
        var file = try dir.createFile(io, path, .{
            .read = true,
            .truncate = false,
        });
        const stat = try file.stat(io);

        return .{
            .file = file,
            .write_offset = stat.size,
        };
    }

    pub fn deinit(self: *Journal, io: Io) void {
        self.file.close(io);
        self.* = undefined;
    }

    /// Stream bytes from a socket directly into the file at the current end.
    /// Uses io_uring + splice for zero-copy transfer.
    pub fn write(self: *Journal, io: Io, stream: net.Stream) !u64 {
        return writeStream(self, io, stream);
    }

    fn writeStream(self: *Journal, io: Io, stream: net.Stream) !u64 {
        const stat = try self.file.stat(io);
        const start_offset = stat.size;
        if (builtin.os.tag != .linux) return error.OperationUnsupported;

        var ring = try linux.IoUring.init(8, 0);
        defer ring.deinit();

        var pipe_fds: [2]i32 = undefined;
        const pipe_res = linux.pipe2(&pipe_fds, .{ .CLOEXEC = true });
        switch (linux.errno(pipe_res)) {
            .SUCCESS => {},
            else => |err| return std.posix.unexpectedErrno(err),
        }
        defer {
            _ = linux.close(@intCast(pipe_fds[0]));
            _ = linux.close(@intCast(pipe_fds[1]));
        }

        const socket_fd: linux.fd_t = @intCast(stream.socket.handle);
        const file_fd: linux.fd_t = @intCast(self.file.handle);
        const pipe_read: linux.fd_t = @intCast(pipe_fds[0]);
        const pipe_write: linux.fd_t = @intCast(pipe_fds[1]);
        const no_offset: u64 = std.math.maxInt(u64);
        const chunk_size: usize = 64 * 1024;

        var total: u64 = 0;
        var write_offset = start_offset;

        while (true) {
            const n_in = try spliceOnce(&ring, socket_fd, no_offset, pipe_write, no_offset, chunk_size);
            if (n_in == 0) break;

            var remaining = n_in;
            while (remaining > 0) {
                const n_out = try spliceOnce(&ring, pipe_read, no_offset, file_fd, write_offset, remaining);
                if (n_out == 0) return error.UnexpectedEndOfStream;
                remaining -= n_out;
                total += @intCast(n_out);
                write_offset += @intCast(n_out);
            }
        }

        const nl: [1]u8 = .{'\n'};
        const nl_written = try writeOnce(&ring, file_fd, nl[0..], write_offset);
        if (nl_written != nl.len) return error.UnexpectedShortWrite;
        write_offset += nl_written;
        total += nl_written;

        self.write_offset = write_offset;
        return total;
    }

    pub fn sync(self: *Journal, io: Io) !void {
        try self.file.sync(io);
    }
};

fn spliceOnce(
    ring: *linux.IoUring,
    fd_in: linux.fd_t,
    off_in: u64,
    fd_out: linux.fd_t,
    off_out: u64,
    len: usize,
) !usize {
    if (len == 0) return 0;
    var sqe = try ring.get_sqe();
    sqe.prep_splice(fd_in, off_in, fd_out, off_out, len);
    sqe.rw_flags = 0;

    _ = try ring.submit_and_wait(1);
    const cqe = try ring.copy_cqe();
    const err = cqe.err();
    if (err != .SUCCESS) return std.posix.unexpectedErrno(err);
    if (cqe.res < 0) return std.posix.unexpectedErrno(@enumFromInt(-cqe.res));
    return @intCast(cqe.res);
}

fn writeOnce(ring: *linux.IoUring, fd: linux.fd_t, buffer: []const u8, offset: u64) !usize {
    if (buffer.len == 0) return 0;
    var sqe = try ring.get_sqe();
    sqe.prep_write(fd, buffer, offset);

    _ = try ring.submit_and_wait(1);
    const cqe = try ring.copy_cqe();
    const err = cqe.err();
    if (err != .SUCCESS) return std.posix.unexpectedErrno(err);
    if (cqe.res < 0) return std.posix.unexpectedErrno(@enumFromInt(-cqe.res));
    return @intCast(cqe.res);
}

const ClientCtx = struct {
    io: Io,
    port: u16,
    data: []const u8,
    err: ?anyerror = null,
};

fn clientThread(ctx: *ClientCtx) void {
    var addr = net.IpAddress.parse("127.0.0.1", ctx.port) catch |err| {
        ctx.err = err;
        return;
    };
    var stream = connectIp(ctx.io, &addr, .{ .mode = .stream, .protocol = .tcp }) catch |err| {
        ctx.err = err;
        return;
    };
    defer stream.close(ctx.io);

    var write_buf: [256]u8 = undefined;
    var writer = net.Stream.writer(stream, ctx.io, &write_buf);
    writer.interface.writeAll(ctx.data) catch |err| {
        ctx.err = err;
        return;
    };
    writer.interface.flush() catch |err| {
        ctx.err = err;
        return;
    };
    _ = stream.shutdown(ctx.io, .send) catch {};
}

fn connectIp(
    io: Io,
    address: *net.IpAddress,
    options: net.IpAddress.ConnectOptions,
) !net.Stream {
    const param = @typeInfo(@TypeOf(net.IpAddress.connect)).@"fn".params[0].type orelse net.IpAddress;
    if (param == net.IpAddress) {
        return net.IpAddress.connect(address.*, io, options);
    }
    return net.IpAddress.connect(address, io, options);
}

fn listenIp(
    io: Io,
    address: *net.IpAddress,
    options: net.IpAddress.ListenOptions,
) !net.Server {
    const param = @typeInfo(@TypeOf(net.IpAddress.listen)).@"fn".params[0].type orelse net.IpAddress;
    if (param == net.IpAddress) {
        return net.IpAddress.listen(address.*, io, options);
    }
    return net.IpAddress.listen(address, io, options);
}

test "journal write appends stream" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    //defer tmp.cleanup();

    const prefix = "prefix:";
    const payloads = [_][]const u8{
        "socket-data-1",
        "socket-data-2",
        "socket-data-3",
        "socket-data-4",
        "socket-data-5",
        "socket-data-6",
        "socket-data-7",
        "socket-data-8",
        "socket-data-9",
        "socket-data-10",
    };
    var expected_list = std.ArrayList(u8).empty;
    defer expected_list.deinit(allocator);
    try expected_list.appendSlice(allocator, prefix);
    for (payloads) |payload| {
        try expected_list.appendSlice(allocator, payload);
    }

    {
        var file = try tmp.dir.createFile(io, "journal.log", .{ .read = true, .truncate = true });
        defer file.close(io);
        try file.writeStreamingAll(io, prefix);
    }

    var journal = try Journal.init(io, tmp.dir, "journal.log");
    defer journal.deinit(io);

    var addr = try net.IpAddress.parse("127.0.0.1", 0);
    var server = try listenIp(io, &addr, .{ .reuse_address = true });
    defer server.deinit(io);

    const port = server.socket.address.getPort();

    var ctx = ClientCtx{
        .io = io,
        .port = port,
        .data = payloads[0],
    };

    var total_written: u64 = 0;
    for (payloads) |payload| {
        ctx.data = payload;
        ctx.err = null;
        var thread = try std.Thread.spawn(.{}, clientThread, .{&ctx});

        var stream = try server.accept(io);
        defer stream.close(io);

        const written = try journal.write(io, stream);
        try std.testing.expectEqual(@as(u64, payload.len), written);
        total_written += written;

        thread.join();
        if (ctx.err) |err| return err;
    }

    const expected = expected_list.items;
    try std.testing.expectEqual(@as(u64, expected.len), journal.write_offset);

    var read_file = try tmp.dir.openFile(io, "journal.log", .{ .mode = .read_only });
    defer read_file.close(io);
    var buf = try allocator.alloc(u8, expected.len);
    defer allocator.free(buf);
    const n = try read_file.readPositionalAll(io, buf, 0);
    try std.testing.expectEqual(expected.len, n);
    try std.testing.expectEqualStrings(expected, buf[0..n]);
}
