const std = @import("std");
const Io = std.Io;

const log = @import("utils/debug.zig").ModulePrinterType(.order_item);

const service = @import("inventory.zig");

const MyUserData = struct {
    allocator: std.mem.Allocator,
    connection_id: u64,
};

fn defaultInsert(
    userdata: *MyUserData,
    request: service.Request,
    writer_queue: *std.Io.Queue(service.Response),
) MyErrors!void {
    // Stream multiple responses
    for (0..5) |i| {
        const response = service.Response{
            .result = try std.fmt.allocPrint(
                userdata.allocator,
                "Stream item {}: {s}",
                .{ i, request.query },
            ),
        };

        // Write response to queue
        try writer_queue.putOne(response);
    }
}

const MyErrors = error{
    InvalidRequest,
    ServiceUnavailable,
};

const MyServiceVTable = service.OrderItemModule(MyUserData, MyErrors);

const myServiceVTable: MyServiceVTable = .{ .insert = defaultInsert };

fn createUnixSocketPairStreams() ![2]std.Io.net.Stream {
    var fds: [2]std.posix.socket_t = undefined;
    while (true) switch (std.posix.errno(std.posix.system.socketpair(
        std.posix.AF.UNIX,
        std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC,
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

    const dummy_addr: std.Io.net.IpAddress = .{ .ip4 = std.Io.net.Ip4Address.unspecified(0) };
    return .{
        .{ .socket = .{ .handle = fds[0], .address = dummy_addr } },
        .{ .socket = .{ .handle = fds[1], .address = dummy_addr } },
    };
}

test "OrderItemService: encode-decode" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const order_item: service.OrderItem = .{
        .batch_offset = 0,
        .time_label = 0,
        .order_id = 1100,
        .product_id = 110,
        .quantity = 10,
    };

    const streams: [2]Io.net.Stream = try createUnixSocketPairStreams();
    const reader_stream = streams[0];
    const writer_stream = streams[1];
    defer writer_stream.close(io);

    var w_buf: [128]u8 = undefined;
    var w = writer_stream.writer(io, w_buf[0..]);

    try order_item.encode(&w.interface, allocator);
    const order_item_decoded = try service.OrderItem.decode(reader_stream, allocator);
    log.obj("order_item_decoded", order_item_decoded);
}
