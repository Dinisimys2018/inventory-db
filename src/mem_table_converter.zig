const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const printObj = @import("utils/debug.zig").printObj;

const Entity = @import("entities.zig").OrderItem;

pub fn ConverterType() type {
    return struct {
        const Converter = @This();

        pub fn convertToWrite(
            entities: std.MultiArrayList(Entity),
        ) void {
            
        }
    };
}
