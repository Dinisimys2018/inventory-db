const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const stdx_sort = @import("sort.zig");

const m_module = @import("module.zig");

const EntitiesRange = struct { usize, usize };

pub const TableLookupResult = struct {
    table_ptr: usize,
    entities_range: EntitiesRange,
};

pub const LookupResult = std.ArrayList(TableLookupResult);

pub fn LookupWithTwoKeysType(comptime config: *const m_module.ConfigModule) type {
    const Components = config.Components();
    const first_key_meta = comptime Components.Entity.map_fields_meta.getAssertContains(.order_id);

    return struct {
        const Lookup = @This();

        // FIELDS
        module: *Components.Module,
        mem_lookup_result: *LookupResult,
        level_0_lookup_result: *LookupResult,
        limit: usize,
        buffer_entities: []Components.Entity,

        pub fn init(allocator: Allocator, module: *Components.Module, limit: usize) !*Lookup {
            assert(limit <= 200 and limit >= 1);

            const lookup = try allocator.create(Lookup);
            lookup.* = .{
                .module = module,
                .mem_lookup_result = try allocator.create(LookupResult),
                .level_0_lookup_result = try allocator.create(LookupResult),
                .limit = limit,
                .buffer_entities = try allocator.alloc(Components.Entity, limit),
            };
            lookup.mem_lookup_result.* = try .initCapacity(allocator, limit);
            lookup.level_0_lookup_result.* = try .initCapacity(allocator, limit);

            return lookup;
        }

        pub fn deinit(lookup: *Lookup, allocator: Allocator) void {
            allocator.free(lookup.buffer_entities);
            lookup.mem_lookup_result.deinit(allocator);
            lookup.level_0_lookup_result.deinit(allocator);

            allocator.destroy(lookup.level_0_lookup_result);
            allocator.destroy(lookup.mem_lookup_result);
            allocator.destroy(lookup);
        }

        pub fn lookupByFirstKey(
            lookup: *Lookup,
            key_value: Components.IndexTable.FirstKey,
        ) []const Components.Entity {
            var limit = lookup.limit;
            var count: usize = 0;
            // count = lookup.lookupByFirstKeyInMemory(key_value, limit);

            if (count < limit) {
                limit -= count;
                count += lookup.lookupByFirstKeyInLevel0(key_value, limit);
            }

            return lookup.readResults();
        }

        pub fn lookupByFirstKeyInMemory(
            lookup: *Lookup,
            key_value: Components.IndexTable.FirstKey,
            limit: usize,
        ) usize {
            assert(key_value != 0);

            //TODO: P3 need to check how we can clear result not before each lookup, but after this
            lookup.mem_lookup_result.clearRetainingCapacity();

            var table_ptr = lookup.module.pool_mem_tables.active_table_ptr;
            var mem_table: *Components.MemTable = undefined;
            var index: *Components.IndexTable = undefined;
            var count: usize = 0;

            while (table_ptr < lookup.module.pool_mem_tables.tables.len) : (table_ptr += 1) {
                mem_table = lookup.module.pool_mem_tables.tables[table_ptr];
                index = lookup.module.pool_mem_tables.indexes[table_ptr];

                if (index.inFirstKeyInterval(key_value)) {
                    var entities_range = stdx_sort.equalRangeDesc(
                        Components.Entity.OrderId,
                        mem_table.entities.slice().items(first_key_meta.tag),
                        key_value,
                        stdx_sort.compareNumberKeys(Components.IndexTable.FirstKey),
                    );

                    if (entities_range[1] == 0) return count;

                    count += entities_range[1];
                    if (count > limit) {
                        entities_range[1] = entities_range[1] - limit - count;
                    }

                    lookup.mem_lookup_result.appendAssumeCapacity(.{
                        .table_ptr = table_ptr,
                        .entities_range = entities_range,
                    });
                }
            }

            return lookup.mem_lookup_result.items.len;
        }

        pub fn lookupByFirstKeyInLevel0(
            lookup: *Lookup,
            key_value: Components.IndexTable.FirstKey,
            limit: usize,
        ) usize {
            assert(key_value != 0);

            //TODO: P3 need to check how we can clear result not before each lookup, but after this
            lookup.level_0_lookup_result.clearRetainingCapacity();

            var table_ptr: usize = 0;
            var table: *Components.StorageTable = undefined;
            var index: *Components.IndexTable = undefined;
            // var count: usize = 0;
            _ = limit;

            while (table_ptr < lookup.module.level_0_pool_storage_tables.actual_count_tables) : (table_ptr += 1) {
                table = lookup.module.level_0_pool_storage_tables.tables[table_ptr];
                index = lookup.module.level_0_pool_storage_tables.indexes[table_ptr];

                if (index.inFirstKeyInterval(key_value)) {
                    lookup.level_0_lookup_result.appendAssumeCapacity(.{
                        .table_ptr = table_ptr,
                        .entities_range = .{1, 1},
                    });
                }
            }

            return lookup.level_0_lookup_result.items.len;
        }

        pub fn readResults(
            lookup: *Lookup,
        ) []const Components.Entity {
            const count = lookup.module.pool_mem_tables.getActualEntities(
                lookup.mem_lookup_result,
                lookup.buffer_entities,
            );

            return lookup.buffer_entities[0..count];
        }
    };
}
