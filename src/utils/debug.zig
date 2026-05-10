const std = @import("std");

const DebugConfig = struct {
    modules: std.EnumMap(Module, bool),
};

pub const Module = enum {
    module,
    storage,
    lookup,
    mem_tables,
    scheduler,
};

// TODO: P3 UTILS_DEBUG
// Move to build.zig
const debug_config: DebugConfig = .{
    .modules = .init(.{
        .module = true,
        .storage = false,
        .lookup = false,
        .mem_tables = true,
        .scheduler = true,
    }),
};

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

        pub fn err(value: anyerror) void {
            obj("ERROR", value);
        }
    };
}
