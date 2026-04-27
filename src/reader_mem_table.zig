const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Writer = std.Io.Writer;
const Error = Writer.FileError;

const printObj = @import("utils/debug.zig").printObj;

const Entity = @import("entities.zig").OrderItem;
const Entities = @import("mem_table.zig").Entities;
const MemTablePtr = @import("mem_table.zig").MemTablePtr;
const MemEntryPtr = @import("mem_table.zig").MemEntryPtr;

pub fn ReaderMemTableType(comptime TableList: type) type {
    return struct {
        const Reader = @This();

        // FIELDS
        mem_tables: TableList,
        table_ptr: MemTablePtr,
        end_ptr: MemTablePtr,

        pub fn init(allocator: std.mem.Allocator, mem_tables: TableList) !*Reader {
            var reader = try allocator.create(Reader);
            reader.mem_tables = mem_tables;

            return reader;
        }

        pub fn start(reader: *Reader, start_ptr: MemTablePtr, limit: MemTablePtr) void {
            assert(limit > 0);

            reader.table_ptr = start_ptr;
            reader.end_ptr = start_ptr + limit - 1;

            assert(reader.end_ptr < reader.mem_tables.len);
            assert(reader.table_ptr <= reader.end_ptr);
        }

        pub fn deinit(reader: *Reader, allocator: std.mem.Allocator) void {
            allocator.destroy(reader);
        }

        pub fn stream(reader: *Reader, writer: *Writer) Error!usize {
            if (reader.table_ptr > reader.end_ptr) {
                reader.table_ptr = undefined;
                reader.end_ptr = undefined;

                return Error.EndOfStream;
            }

            var enitity_ptr: MemEntryPtr = 0;
            const slice = reader.mem_tables[reader.table_ptr].entities.slice();

            while (enitity_ptr < slice.len) : (enitity_ptr += 1) {
                const entity = slice.get(enitity_ptr);
                try writer.writeAll(std.mem.asBytes(&entity));
            }

            reader.table_ptr += 1;

            return slice.len;
        }
    };
}
