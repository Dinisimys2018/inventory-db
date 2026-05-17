const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const net = Io.net;

const assert = std.debug.assert;

const log = @import("utils/debug.zig").ModulePrinterType(.queue);
const module = @import("module.zig");

pub const SocketReader = struct {
    stream: net.Stream,
    buf: []u8,
    reader: net.Stream.Reader,

    const buf_size = 4096;

    pub fn init(allocator: Allocator, io: Io, stream: net.Stream) !*SocketReader {
        const reader = try allocator.create(SocketReader);
        const buf = try allocator.alloc(u8, buf_size);

        reader.* = .{
            .stream = stream,
            .buf = buf,
            .reader = stream.reader(io, buf),
        };
        return reader;
    }

    pub fn deinit(reader: *SocketReader, allocator: Allocator, io: Io) void {
        reader.stream.close(io);
        allocator.free(reader.buf);
        allocator.destroy(reader);
    }
};

pub const Command = union(enum) {
    insert: *SocketReader,
    flush_mem_tables: void,
};

const Message = struct {
    const State = enum {
        new,
    };

    // FIELDS
    id: u32,
    command: Command,
    base_ms: u32,
    pushed_ms: u32,
    planned_ms: u32,

    pub fn deinitContents(message: *Message, allocator: Allocator, io: Io) void {
        switch (message.command) {
            Command.insert => message.command.insert.deinit(allocator, io),
            Command.flush_mem_tables => {},
        }
        // Avoid double-free if a pooled message gets cleaned up more than once.
        message.command = .{ .flush_mem_tables = {} };
    }

    pub fn deinit(message: *Message, allocator: Allocator, io: Io) void {
        message.deinitContents(allocator, io);
        allocator.destroy(message);
    }
};

pub fn QueueType(comptime config: *const module.ConfigModule) type {
    _ = config.Components();

    return struct {
        const Queue = @This();

        // FIELDS
        messages: Io.Queue(*Message),
        buffer: []*Message,
        last_message_ptr: @TypeOf(config.queue_messages_count),

        pub fn init(allocator: Allocator) !*Queue {
            const queue = try allocator.create(Queue);
            queue.buffer = try allocator.alloc(*Message, config.queue_messages_count);
            queue.last_message_ptr = 0;

            var message_idx: u8 = 0;
            while (message_idx < config.queue_messages_count) : (message_idx += 1) {
                const message = try allocator.create(Message);
                message.* = .{
                    .id = 0,
                    .command = .{ .flush_mem_tables = {} },
                    .base_ms = 0,
                    .pushed_ms = 0,
                    .planned_ms = 0,
                };
                queue.buffer[message_idx] = message;
            }
            queue.messages = .init(queue.buffer);

            return queue;
        }

        pub fn deinit(queue: *Queue, allocator: Allocator, io: Io) void {
            queue.messages.close(io);

            while (true) {
                const message = queue.messages.getOneUncancelable(io) catch |err| switch (err) {
                    error.Closed => break,
                };
                message.deinitContents(allocator, io);
            }

            // Free the whole message pool (some entries may never have been queued).
            for (queue.buffer) |message| {
                allocator.destroy(message);
            }

            allocator.free(queue.buffer);
            allocator.destroy(queue);
        }

        pub fn putOne(queue: *Queue, io: Io, command: Command) (Io.QueueClosedError || Io.Cancelable)!void {
            var message = queue.buffer[queue.last_message_ptr];
            message.command = command;
            queue.last_message_ptr += 1;
            return queue.messages.putOne(io, message);
        }

        pub fn getOne(queue: *Queue, io: Io) (Io.QueueClosedError || Io.Cancelable)!*Message {
            return queue.messages.getOne(io);
        }
    };
}
