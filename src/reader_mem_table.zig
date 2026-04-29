const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const Writer = std.Io.Writer;
const Error = Writer.FileError;

const printObj = @import("utils/debug.zig").printObj;

const MemTablePtr = @import("mem_table.zig").MemTablePtr;
const MemEntryPtr = @import("mem_table.zig").MemEntryPtr;

pub fn MetaReaderMemTableType(comptime PoolMemTables: type) type {
    return struct {
        const Reader = @This();

        // FIELDS
        table_ptr: MemTablePtr,
        pool_mem_tables: *PoolMemTables,

        pub fn init(allocator: std.mem.Allocator, pool_mem_tables: *PoolMemTables) !*Reader {
            var reader = try allocator.create(Reader);
            reader.pool_mem_tables = pool_mem_tables;

            return reader;
        }

        pub fn deinit(reader: *Reader, allocator: std.mem.Allocator) void {
            allocator.destroy(reader);
        }

        pub fn start(reader: *Reader) void {
            reader.table_ptr = 0;
        }

        pub fn stream(reader: *Reader, writer: *Writer) Error!usize {
            if (reader.table_ptr == reader.pool_mem_tables.filled_table_ptrs.len) {
                reader.table_ptr = undefined;
                return Error.EndOfStream;
            }
            var total_streamed_bytes: usize = 0;
            while (reader.table_ptr < reader.pool_mem_tables.filled_table_ptrs.len) : (reader.table_ptr += 1) {
                if (reader.pool_mem_tables.filled_table_ptrs[reader.table_ptr]) {
                    const bytes = std.mem.asBytes(&reader.pool_mem_tables.tables[reader.table_ptr].meta);
                    try writer.writeAll(bytes);
                    total_streamed_bytes += bytes.len;
                }
            }

            return total_streamed_bytes;
        }
    };
}

pub fn DataReaderMemTableType(comptime PoolMemTables: type) type {
    return struct {
        const Reader = @This();

        // FIELDS
        table_ptr: MemTablePtr,
        pool_mem_tables: *PoolMemTables,

        pub fn init(allocator: std.mem.Allocator, pool_mem_tables: *PoolMemTables) !*Reader {
            var reader = try allocator.create(Reader);
            reader.pool_mem_tables = pool_mem_tables;

            return reader;
        }

        pub fn deinit(reader: *Reader, allocator: std.mem.Allocator) void {
            allocator.destroy(reader);
        }

        pub fn start(reader: *Reader) void {
            reader.table_ptr = 0;
        }

        pub fn stream(reader: *Reader, writer: *Writer) Error!usize {
            if (reader.table_ptr == reader.pool_mem_tables.filled_table_ptrs.len) {
                reader.table_ptr = undefined;

                return Error.EndOfStream;
            }

            if (!reader.pool_mem_tables.filled_table_ptrs[reader.table_ptr]) {
                reader.table_ptr += 1;
                return 0;
            }

            var enitity_ptr: MemEntryPtr = 0;
            const slice = reader.pool_mem_tables.tables[reader.table_ptr].entities.slice();

            var total_streamed_bytes: usize = 0;
            while (enitity_ptr < slice.len) : (enitity_ptr += 1) {
                const entity = slice.get(enitity_ptr);
                const bytes = std.mem.asBytes(&entity);
                try writer.writeAll(bytes);
                total_streamed_bytes += bytes.len;
            }

            reader.table_ptr += 1;

            return total_streamed_bytes;
        }
    };
}
