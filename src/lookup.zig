const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const printObj = @import("utils/debug.zig").printObj;
const module = @import("module.zig");

const EntitiesRange = struct { usize, usize };

pub const TableLookupResult = struct {
    table_ptr: usize,
    entities_range: EntitiesRange,
};

pub const LookupResult = std.ArrayList(TableLookupResult);


pub fn OrderItemLookupType(comptime config: module.ConfigModule) type {
    const Components = config.Components();
    
    return struct {

        const Lookup = @This();

        // FIELDS
        module: *Components.Module,
        mem_lookup_result: *LookupResult,

        pub fn init(allocator: Allocator, module_component: *Components.Module) !*Lookup {
            const lookup = try allocator.create(Lookup);
            lookup.* = .{
                .module = module_component,
                .mem_lookup_result = try allocator.create(LookupResult),
            };
            
            lookup.mem_lookup_result.* = try .initCapacity(allocator, config.mem_tables_max_count);

            return lookup;
        }

        pub fn deinit(lookup: *Lookup, allocator: Allocator) void {
            allocator.destroy(lookup);
        }

        pub fn lookupByOrderIdInMemory(lookup: *Lookup, key_value: Components.Entity.OrderId,) !void {
            assert(key_value != 0);

            //TODO: P3 need to check how we can clear result not before each lookup, but after this
            lookup.mem_lookup_result.clearRetainingCapacity();

            var table_ptr = lookup.module.pool_mem_tables.active_table_ptr;
            var mem_table: *Components.MemTable = undefined;

            while (table_ptr > 0): (table_ptr -= 1) {
                mem_table = lookup.module.pool_mem_tables.tables[last_indx];

                if (mem_table.index.inFirstInterval(key_value)) {
                      mem_table.primarySort();
                }

            const entities_range = stdx_sort.equalRangeDesc(
                Components.Entity.OrderId,
                mem_table.entities.slice().items(Components.Entity.map_field_tags.get(.order_id)),
                key_value,
                stdx_sort.compareNumberKeys(Components.Entity.OrderId),
            );

            if (entities_range[1] == 0) return error.NotFound;

                    table_pool.lookup_result.appendAssumeCapacity(.{
                        .table_ptr = last_indx,
                        .entities_range = entities_range,
                    });
                }
            }

            if (table_pool.lookup_result.items.len > 0) return table_pool.lookup_result;

            return error.NotFound;        }
    };
}
