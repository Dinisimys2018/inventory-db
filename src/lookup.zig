const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const printObj = @import("utils/debug.zig").printObj;
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
    
    return struct {

        const Lookup = @This();

        // FIELDS
        module: *Components.Module,
        mem_lookup_result: *LookupResult,

        pub fn init(allocator: Allocator, module: *Components.Module) !*Lookup {
            const lookup = try allocator.create(Lookup);
            lookup.* = .{
                .module = module,
                .mem_lookup_result = try allocator.create(LookupResult),
            };
            lookup.mem_lookup_result.* = try .initCapacity(allocator, config.mem_tables_max_count);

            return lookup;
        }

        pub fn deinit(lookup: *Lookup, allocator: Allocator) void {
            lookup.mem_lookup_result.deinit(allocator);
            allocator.destroy(lookup.mem_lookup_result);
            allocator.destroy(lookup);
        }

        pub fn lookupByFirstKeyInMemory(lookup: *Lookup, key_value: Components.IndexTable.FirstKey,) void {
            assert(key_value != 0);

            //TODO: P3 need to check how we can clear result not before each lookup, but after this
            lookup.mem_lookup_result.clearRetainingCapacity();

            var table_ptr = lookup.module.pool_mem_tables.active_table_ptr;
            var mem_table: *Components.MemTable = undefined;
            var index: *Components.IndexTable = undefined;

            while (table_ptr < lookup.module.pool_mem_tables.tables.len): (table_ptr += 1) {
                mem_table = lookup.module.pool_mem_tables.tables[table_ptr];
                index = lookup.module.pool_mem_tables.indexes[table_ptr];

                if (index.inFirstKeyInterval(key_value)) {
        
            const entities_range = stdx_sort.equalRangeDesc(
                Components.Entity.OrderId,
                mem_table.entities.slice().items(Components.Entity.map_field_tags.getAssertContains(.order_id)),
                key_value,
                stdx_sort.compareNumberKeys(Components.IndexTable.FirstKey),
            );

            if (entities_range[1] == 0) return;
                    lookup.mem_lookup_result.appendAssumeCapacity(.{
                        .table_ptr = table_ptr,
                        .entities_range = entities_range,
                    });
                }
            }
        }

        // pub fn lookupByFirstKeyInLevel0(lookup: *Lookup, key_value: Components.IndexTable.FirstKey,) !*const LookupResult  {
        //     assert(key_value != 0); 
        // }
    };
}
