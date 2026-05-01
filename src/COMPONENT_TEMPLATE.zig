// const std = @import("std");
// const Allocator = std.mem.Allocator;
// const assert = std.debug.assert;

// const printObj = @import("utils/debug.zig").printObj;
// const module = @import("module.zig");

// pub fn MODULE_TYPEType(comptime config: module.ConfigModule) type {
//     const Components = config.Components();
//     _ = Components;
//
//     return struct {
//         const MODULE_TYPE = @This();

//     pub fn init(allocator: Allocator) !*MODULE_TYPE{
//         const MODULE_INSTANCE = try allocator.create(MODULE_TYPE);

//         return MODULE_INSTANCE;
//     }

//     pub fn deinit(MODULE_INSTANCE: *MODULE_TYPE, allocator: Allocator) void {
//         allocator.destroy(MODULE_INSTANCE);
//     }

//     };
// }