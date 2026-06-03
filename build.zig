const std = @import("std");
const protobuf = @import("protobuf");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const protobuf_dep = b.dependency("protobuf", .{
        .target = target,
        .optimize = optimize,
    });

    const imports = [_]std.Build.Module.Import{
        .{
            .name = "protobuf",
            .module = protobuf_dep.module("protobuf"),
        },
    };

    const root_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &imports,
    });

    const exe = b.addExecutable(.{
        .name = "inventory_db",
        .root_module = root_module,
    });


    const gen_proto = b.step(
        "gen-proto",
        "generates zig files from protocol buffer definitions",
    );

    const protoc_step = protobuf.RunProtocStep.create(b, target, .{
        // out directory for the generated zig files
        .destination_directory = b.path("src/proto_services"),
        // Optional custom generator, otherwise it will use the built-in generator + google's protoc
        .generator = protobuf_dep.artifact("protoc-gen-zig"),
        .source_files = &.{
            b.path("protocol/inventory/order_item.proto"),
        },
        .include_directories = &.{
            b.path("protocol/inventory"),
        },
        // Preserve unknown fields during binary decode/encode round trips.
        // Defaults to false.
        .preserve_unknown_fields = false,
    });

    gen_proto.dependOn(&protoc_step.step);
    // This declares intent for the executable to be installed into the
    // install prefix when running `zig build` (i.e. when executing the default
    // step). By default the install prefix is `zig-out/` but can be overridden
    // by passing `--prefix` or `-p`.
    b.installArtifact(exe);

    // This creates a top level step. Top level steps have a name and can be
    // invoked by name when running `zig build` (e.g. `zig build run`).
    // This will evaluate the `run` step rather than the default step.
    // For a top level step to actually do something, it must depend on other
    // steps (e.g. a Run step, as we will see in a moment).
    const run_step = b.step("run", "Run the app");

    // This creates a RunArtifact step in the build graph. A RunArtifact step
    // invokes an executable compiled by Zig. Steps will only be executed by the
    // runner if invoked directly by the user (in the case of top level steps)
    // or if another step depends on it, so it's up to you to define when and
    // how this Run step will be executed. In our case we want to run it when
    // the user runs `zig build run`, so we create a dependency link.
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);

    // By making the run step depend on the default step, it will be run from the
    // installation directory rather than directly from within the cache directory.
    run_cmd.step.dependOn(b.getInstallStep());

    // This allows the user to pass arguments to the application in the build
    // command itself, like this: `zig build run -- arg1 arg2 etc`
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // Creates an executable that will run `test` blocks from the provided module.
    // Here `mod` needs to define a target, which is why earlier we made sure to
    // set the releative field.

    const test_filter = b.option([]const []const u8, "test-filter", "Test filter");

const mod_tests = b.addTest(.{
    .root_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &imports,
    }),
.filters = if (test_filter) |filter| filter else &[_][]const u8{},
});

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });

    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);

    const exe_check = b.addExecutable(.{
    .name = "root_check",
    .root_module = root_module,
});
// There is no `b.installArtifact(exe_check);` here.

// Finally we add the "check" step which will be detected
// by ZLS and automatically enable Build-On-Save.
// If you copy this into your `build.zig`, make sure to rename 'foo'
const check = b.step("check", "Check if foo compiles");
check.dependOn(&exe_check.step);
}
