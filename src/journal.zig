const std = @import("std");

const Allocator = std.mem.Allocator;
const Io = std.Io;

pub const Journal = struct {
    const Self = @This();

    io: Io,
    file: Io.File,
    buffer: []u8,
    file_writer: Io.File.Writer,

    pub fn createNew(
        allocator: Allocator,
        io: Io,
        base_dir: Io.Dir,
        sub_path: []const u8,
        buffer_size: usize,
    ) !*Self {
        var file = try base_dir.createFile(io, sub_path, .{
            .exclusive = true,
            .read = true,
            .truncate = true,
        });
        errdefer file.close(io);

        const buffer = try allocator.alloc(u8, buffer_size);
        errdefer allocator.free(buffer);

        const journal = try allocator.create(Self);
        errdefer allocator.destroy(journal);

        journal.* = .{
            .io = io,
            .file = file,
            .buffer = buffer,
            .file_writer = file.writer(io, buffer),
        };

        return journal;
    }

    /// Opens (or creates) a journal file and seeks to the end so that writes append.
    pub fn openOrCreateAppend(
        allocator: Allocator,
        io: Io,
        base_dir: Io.Dir,
        sub_path: []const u8,
        buffer_size: usize,
    ) !*Self {
        var file = base_dir.openFile(io, sub_path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => try base_dir.createFile(io, sub_path, .{
                .read = true,
                .truncate = false,
            }),
            else => |e| return e,
        };
        errdefer file.close(io);

        const buffer = try allocator.alloc(u8, buffer_size);
        errdefer allocator.free(buffer);

        const journal = try allocator.create(Self);
        errdefer allocator.destroy(journal);

        journal.* = .{
            .io = io,
            .file = file,
            .buffer = buffer,
            .file_writer = file.writer(io, buffer),
        };

        const size = (try journal.file.stat(io)).size;
        try journal.file_writer.seekTo(size);

        return journal;
    }

    pub fn deinit(journal: *Self, allocator: Allocator) void {
        journal.file_writer.interface.flush() catch {};
        journal.file.close(journal.io);
        allocator.free(journal.buffer);
        allocator.destroy(journal);
    }

    pub fn writer(journal: *Self) *Io.Writer {
        return &journal.file_writer.interface;
    }

    pub fn offset(journal: *const Self) u64 {
        return journal.file_writer.logicalPos();
    }

    /// Flushes any buffered data to the file.
    pub fn flush(journal: *Self) !void {
        try journal.file_writer.interface.flush();
    }

    /// Sets the next write offset. Note: moving backwards will overwrite existing bytes.
    pub fn seekTo(journal: *Self, new_offset: u64) !void {
        try journal.file_writer.seekTo(new_offset);
    }

    /// Writes `bytes` starting at the current offset, using the internal buffer (chunked),
    /// flushes, and advances the offset.
    pub fn writeAll(journal: *Self, bytes: []const u8) !u64 {
        const start = journal.offset();
        try journal.file_writer.interface.writeAll(bytes);
        try journal.file_writer.interface.flush();
        return journal.offset() - start;
    }

    /// Seeks to `at_offset`, writes `bytes` (chunked), flushes, and advances the offset.
    pub fn writeAllAt(journal: *Self, at_offset: u64, bytes: []const u8) !u64 {
        try journal.seekTo(at_offset);
        return journal.writeAll(bytes);
    }

    /// Streams data from `reader` into the journal writer, flushes, and returns bytes written.
    ///
    /// `reader` is expected to have `stream(writer: *std.Io.Writer) !usize`
    /// and to signal completion with `error.EndOfStream`.
    pub fn streamFrom(journal: *Self, reader: anytype) !u64 {
        const start = journal.offset();

        while (true) {
            _ = reader.stream(&journal.file_writer.interface) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
        }

        try journal.file_writer.interface.flush();
        return journal.offset() - start;
    }

    /// Seeks to `at_offset`, then `streamFrom`.
    pub fn streamFromAt(journal: *Self, at_offset: u64, reader: anytype) !u64 {
        try journal.seekTo(at_offset);
        return journal.streamFrom(reader);
    }
};

test "Journal: appends across writeAll calls" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var journal = try Journal.openOrCreateAppend(allocator, io, tmp.dir, "journal.bin", 8);
    defer journal.deinit(allocator);

    try testing.expectEqual(@as(u64, 0), journal.offset());

    const n1 = try journal.writeAll("hello");
    try testing.expectEqual(@as(u64, 5), n1);
    try testing.expectEqual(@as(u64, 5), journal.offset());

    const n2 = try journal.writeAll(" world");
    try testing.expectEqual(@as(u64, 6), n2);
    try testing.expectEqual(@as(u64, 11), journal.offset());

    const contents = try tmp.dir.readFileAlloc(io, "journal.bin", allocator, .unlimited);
    defer allocator.free(contents);
    try testing.expectEqualStrings("hello world", contents);
}

test "Journal: writeAllAt overwrites at offset and advances" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var journal = try Journal.createNew(allocator, io, tmp.dir, "journal.bin", 4);
    defer journal.deinit(allocator);

    _ = try journal.writeAll("abc");
    try testing.expectEqual(@as(u64, 3), journal.offset());

    const n = try journal.writeAllAt(1, "Z");
    try testing.expectEqual(@as(u64, 1), n);
    try testing.expectEqual(@as(u64, 2), journal.offset());

    // Append again at the new offset (2): "abc" with "b" overwritten => "aZc",
    // then write at offset 2 => overwrites "c" => "aZ!"
    _ = try journal.writeAll("!");
    try testing.expectEqual(@as(u64, 3), journal.offset());

    const contents = try tmp.dir.readFileAlloc(io, "journal.bin", allocator, .unlimited);
    defer allocator.free(contents);
    try testing.expectEqualStrings("aZ!", contents);
}

test "Journal: streamFrom writes chunks and advances offset" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var journal = try Journal.createNew(allocator, io, tmp.dir, "journal.bin", 3);
    defer journal.deinit(allocator);

    const ChunkReader = struct {
        const WriterError = Io.Writer.FileError;

        chunks: []const []const u8,
        idx: usize = 0,

        pub fn stream(self: *@This(), w: *Io.Writer) WriterError!usize {
            if (self.idx >= self.chunks.len) return error.EndOfStream;
            const chunk = self.chunks[self.idx];
            self.idx += 1;
            try w.writeAll(chunk);
            return chunk.len;
        }
    };

    var reader: ChunkReader = .{ .chunks = &.{ "ab", "c", "def", "g" } };
    const written = try journal.streamFrom(&reader);
    try testing.expectEqual(@as(u64, 7), written);
    try testing.expectEqual(@as(u64, 7), journal.offset());

    // Next stream appends.
    var reader2: ChunkReader = .{ .chunks = &.{ "!", "!" } };
    const written2 = try journal.streamFrom(&reader2);
    try testing.expectEqual(@as(u64, 2), written2);
    try testing.expectEqual(@as(u64, 9), journal.offset());

    const contents = try tmp.dir.readFileAlloc(io, "journal.bin", allocator, .unlimited);
    defer allocator.free(contents);
    try testing.expectEqualStrings("abcdefg!!", contents);
}
