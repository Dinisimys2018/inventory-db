const std = @import("std");
const ArrayList = std.ArrayList;
const assert = std.debug.assert;
const testing = std.testing;

const printObj = @import("utils/debug.zig").printObj;

const IndexPoolType = @import("index_table.zig").IndexPoolType;

pub const MemTablePtr = u8;
pub const MemEntryPtr = u32;
// TODO: возможно MemTableType можно перенести внутри MemTablePoolType
pub fn MemTableType(comptime EntryType: type, comptime entries_max_count: MemEntryPtr) type {
    return struct {
        const MemTable = @This();

        entries: std.MultiArrayList(EntryType) = .empty,

        pub fn init(allocator: std.mem.Allocator) !*MemTable {
            var mem_table = try allocator.create(MemTable);
            mem_table.entries = try .initCapacity(allocator, entries_max_count);

            return mem_table;
        }

        pub fn deinit(mem_table: *MemTable, allocator: std.mem.Allocator) void {
            mem_table.entries.deinit(allocator);
            allocator.destroy(mem_table);
        }

        pub fn insert(mem_table: *MemTable, entries: []EntryType) void {
            for (entries) |entry| {
                mem_table.entries.appendAssumeCapacity(entry);
            }
        }

        pub fn find(mem_table: *MemTable, entry_ptr: MemEntryPtr) !EntryType {
            return mem_table.entries.get(entry_ptr);
        }
    };
}

pub fn MemTablePoolType(
    comptime EntryType: type,
    comptime indexes_meta: anytype,
    comptime mem_tables_max_count: MemTablePtr,
    comptime entries_max_count: u32,
) type {
    return struct {
        const FieldEntry = std.MultiArrayList(EntryType).Field;
        const entry_field_tags:[indexes_meta.len]FieldEntry = blk: {
            var tmp_entry_field_tags: [indexes_meta.len]FieldEntry = undefined;
            var i: usize = 0;
            while (i < indexes_meta.len) : (i += 1) {
                tmp_entry_field_tags[i] = std.meta.stringToEnum(FieldEntry, indexes_meta[i].field_name).?;
            }
            break :blk tmp_entry_field_tags;
        };

        const MemTablePool = @This();
        const MemTable = MemTableType(EntryType, entries_max_count);
        const TableList = []*MemTable;
        const MemIndexPool = IndexPoolType(
            MemTablePtr,
            MemEntryPtr,
            indexes_meta,
            mem_tables_max_count,
            entries_max_count,
        );

        // Struct Fields
        tables: TableList,
        free_table_ptrs: [mem_tables_max_count]bool,
        filled_table_ptrs: [mem_tables_max_count]bool,
        active_table_ptr: MemTablePtr = 0,
        index_pool: *MemIndexPool,

        pub fn init(allocator: std.mem.Allocator) !*MemTablePool {
            var mem_table_pool = try allocator.create(MemTablePool);
            mem_table_pool.tables = try allocator.alloc(*MemTable, mem_tables_max_count);
            mem_table_pool.free_table_ptrs = .{true} ** mem_tables_max_count;
            mem_table_pool.filled_table_ptrs = .{false} ** mem_tables_max_count;
            mem_table_pool.active_table_ptr = 0;
            mem_table_pool.index_pool = try .init(allocator);

            var mem_table_ptr: MemTablePtr = 0;

            while (mem_table_ptr < mem_tables_max_count) : (mem_table_ptr += 1) {
                mem_table_pool.tables[mem_table_ptr] = try .init(allocator);
            }

            return mem_table_pool;
        }

        pub fn deinit(mem_tables_pool: *MemTablePool, allocator: std.mem.Allocator) void {
            for (mem_tables_pool.tables) |table| {
                table.deinit(allocator);
            }
            allocator.free(mem_tables_pool.tables);
            mem_tables_pool.index_pool.deinit(allocator);
            allocator.destroy(mem_tables_pool);
        }

        pub fn insert(mem_tables_pool: *MemTablePool, entries: []EntryType) !void {
            var entries_start: usize = 0;
            var entries_end: usize = 0;

            while (entries_end < entries.len) {
                // Сразу убираем активную таблицу из свободных,
                // чтобы другие вызовы не получили доступ к ней
                mem_tables_pool.free_table_ptrs[mem_tables_pool.active_table_ptr] = false;

                var active_table = mem_tables_pool.tables[mem_tables_pool.active_table_ptr];

                // Получаем количество, которое мы можем вставить в активную таблицу
                const rest = active_table.entries.capacity - active_table.entries.len;
                entries_end += rest;

                // Контролируем границу
                if (entries_end >= entries.len) {
                    entries_end = entries.len;
                }
                const toInsert = entries[entries_start..entries_end];
                active_table.insert(toInsert);

                comptime var field_meta_index: u8 = 0;

                inline while (field_meta_index < indexes_meta.len) : (field_meta_index += 1) {
                    //TODO: Necessary to check the efficiency of this method .slice().items()
                    try mem_tables_pool.index_pool.insert(mem_tables_pool.active_table_ptr, indexes_meta[field_meta_index].field_name, active_table.entries.slice().items(entry_field_tags[field_meta_index]));
                }

                // Если мы заполнили все свободное место
                // значит перемещаем активную таблицу в filled_table_ptrs
                if (rest == entries_end - entries_start) {
                    mem_tables_pool.filled_table_ptrs[mem_tables_pool.active_table_ptr] = true;

                    // Если есть еще свободные таблицы,
                    // тогда смещаем индекс для работы с новой активной таблицой
                    if (mem_tables_pool.active_table_ptr < mem_tables_pool.filled_table_ptrs.len - 1) {
                        mem_tables_pool.active_table_ptr += 1;
                    } else {
                        //TODO full-filled_table_ptrs all mem_tables
                        // Надо придумать механизм работы с перезаполненным пулом
                        // Возможно реализовать ожидание через IO sleep
                        // или принудительно скидывать таблицы на диск и освобождать
                        unreachable;
                    }
                }

                entries_start = entries_end;
            }
        }

        pub fn find(mem_tables_pool: *MemTablePool, field_name: []const u8, field_value: anytype) !EntryType {
            const lookup_value = try mem_tables_pool.index_pool.find(field_name, field_value);

            return try mem_tables_pool.tables[lookup_value.table_ptr].find(lookup_value.value_ptr);
        }

        pub fn calculateFreeTables(mem_tables_pool: *MemTablePool) MemTablePtr {
            var count_tables: MemTablePtr = 0;
            for (mem_tables_pool.free_table_ptrs) |is_free| {
                if (is_free) {
                    count_tables += 1;
                }
            }

            return count_tables;
        }

        pub fn calculateFilledTables(mem_tables_pool: *MemTablePool) MemTablePtr {
            var count_tables: MemTablePtr = 0;
            for (mem_tables_pool.filled_table_ptrs) |is_filled| {
                if (is_filled) {
                    count_tables += 1;
                }
            }

            return count_tables;
        }
    };
}

// ==== Testing ====
const EntityFieldIndexListType = @import("index_table.zig").EntityFieldIndexListType;
const fields_count = 2;

const TestEntity = struct {
    pub const OrderId = u32;
    pub const ProductId = u32;

    order_id: OrderId,
    product_id: ProductId,

    pub const IndexesMeta = EntityFieldIndexListType(fields_count);

    pub const indexes_meta: IndexesMeta = .{
        .{
            .field_name = "order_id",
            .index_strategy = .indexes_u32,
        },
        .{
            .field_name = "product_id",
            .index_strategy = .indexes_u32,
        },
    };
};

test "MemTablePool: (max count entries for all tables in pool) - 1" {
    const allocator = std.testing.allocator;

    const entries_max_count = 5;
    const mem_tables_max_count = 5;
    const MemTablePool = MemTablePoolType(
        TestEntity,
        TestEntity.indexes_meta,
        mem_tables_max_count,
        entries_max_count,
    );
    var mem_table_pool: *MemTablePool = try .init(allocator);
    defer mem_table_pool.deinit(allocator);

    // Максимальное количество entries,
    // которое может вместить весь pool минус 1,
    // чтобы не заполнить все таблицы
    const entries_total = entries_max_count * mem_tables_max_count - 1;

    // Preparing input data
    var input_entries: std.ArrayList(TestEntity) = try .initCapacity(allocator, entries_total);
    defer input_entries.deinit(allocator);

    var index: u8 = 0;

    while (index < entries_total) : (index += 1) {
        const entity: TestEntity = .{
            .order_id = index * 2 + 1,
            .product_id = index * 2 + 2,
        };
        input_entries.appendAssumeCapacity(entity);
    }
    // -------------------

    //==== General test ====

    try mem_table_pool.insert(input_entries.items);

    const count_filled_tables = mem_table_pool.calculateFilledTables();
    const count_free_tables = mem_table_pool.calculateFreeTables();

    try testing.expectEqual(mem_tables_max_count - 1, count_filled_tables);
    try testing.expectEqual(mem_tables_max_count - 1 - count_filled_tables, count_free_tables);

    for (input_entries.items) |expected_entry| {
        const find_entry_by_order_id = try mem_table_pool.find("order_id", expected_entry.order_id);
        try testing.expectEqual(expected_entry.product_id, find_entry_by_order_id.product_id);

        const find_entry_by_product_id = try mem_table_pool.find("product_id", expected_entry.product_id);
        try testing.expectEqual(expected_entry.order_id, find_entry_by_product_id.order_id);
    }
}

test "benchmark MemPool insert" {
    const allocator = std.testing.allocator;

    const entries_max_count: u32 = 16_384;
    const mem_tables_max_count: MemTablePtr = 97;
    const MemTablePool = MemTablePoolType(
        TestEntity,
        TestEntity.indexes_meta,
        mem_tables_max_count,
        entries_max_count,
    );

    var mem_table_pool: *MemTablePool = try .init(allocator);
    defer mem_table_pool.deinit(allocator);

    const desired_bytes: usize = 12 * 1024 * 1024;
    const entries_total: usize = desired_bytes / @sizeOf(TestEntity);

    // Preparing input data
    var input_entries: std.ArrayList(TestEntity) = try .initCapacity(allocator, entries_total);
    defer input_entries.deinit(allocator);

    for (0..entries_total) |idx| {
        const i: u32 = @intCast(idx);
        const entity: TestEntity = .{
            .order_id = i * 2 + 1,
            .product_id = i * 2 + 2,
        };
        input_entries.appendAssumeCapacity(entity);
    }

    const Io = std.Io;
    const io = std.testing.io;
    const start_ns = Io.Clock.awake.now(io).nanoseconds;
    try mem_table_pool.insert(input_entries.items);
    const end_ns = Io.Clock.awake.now(io).nanoseconds;
    const elapsed_ns = end_ns - start_ns;
    const elapsed_ms: u64 = @intCast(@divTrunc(elapsed_ns, std.time.ns_per_ms));
    const elapsed_s: f64 =
        @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, std.time.ns_per_s);
    const bytes_total: usize = @sizeOf(TestEntity) * input_entries.items.len;
    const mb_total: u64 = @intCast(@divTrunc(bytes_total, 1024 * 1024));

    std.debug.print(
        \\benchmark: MemPool insert
        \\  entries: {d}
        \\  time:    {d} ms ({d:.2} s)
        \\  data:    {d} bytes (~{d} MiB)
        \\
    ,
        .{ input_entries.items.len, elapsed_ms, elapsed_s, bytes_total, mb_total },
    );

    const count_filled_tables = mem_table_pool.calculateFilledTables();
    const count_free_tables = mem_table_pool.calculateFreeTables();
    try testing.expectEqual(mem_tables_max_count - 1, count_filled_tables);
    try testing.expectEqual(1, count_free_tables);
}
