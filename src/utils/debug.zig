const std = @import("std");

const DebugConfig = struct {
    modules: std.EnumMap(Module, bool),
};

pub const Module = enum {
    storage,
    lookup,
};

// TODO: P3 UTILS_DEBUG
// Move to build.zig
const debug_config: DebugConfig = .{
    .modules = .init(.{
        .storage = false,
        .lookup = true,
    }),
};

pub fn printObj(title: []const u8, obj: anytype) void {
    std.debug.print("\n==={s}===\n{any}\n=======\n", .{ title, obj });
}

pub fn ModulePrinterType(comptime module: Module) type {
    const module_config = debug_config.modules.getAssertContains(module);

    return struct {
        pub fn obj(title: []const u8, value: anytype) void {
            //TODO: P5 UTILS_DEBUG
            // Research comptime conditional
            if (module_config) {
                std.debug.print("\n=== MODULE{any} ===\n... {s} ...\n {any} \n=====================\n", .{module, title, value });
            }
        }
    };
}
