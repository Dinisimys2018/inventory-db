const std = @import("std");
const ArrayList = std.array_list;
const assert = std.debug.assert;
const testing = std.testing;

const printObj = @import("utils/debug.zig").printObj;

const OrderIdIndexPool = @import("order_id_index.zig").IndexPoolType;
const OrderIdKey = @import("order_id_index.zig").Key;
const OrderIdLookupValue = @import("order_id_index.zig").LookupValue;


pub fn LookupType(comptime keys_per_block: u32) type {
return struct {
    const Lookup = @This();

    //Fields
    order_id_index_pool_ptr: *OrderIdIndexPool(keys_per_block),

    pub fn lookupByOrderId(lookup: *Lookup, key: OrderIdKey) !OrderIdLookupValue {
        return lookup.order_id_index_pool_ptr.pool.lookup(key);
    }
};
}



