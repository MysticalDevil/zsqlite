const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const slt_optimize: std.builtin.OptimizeMode = if (optimize == .Debug) .ReleaseFast else optimize;

    const lib_mod = b.addModule("zsqlite", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = false,
    });

    const tests = b.addTest(.{
        .root_module = lib_mod,
    });
    const run_tests = b.addRunArtifact(tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);

    const slt_exe = b.addExecutable(.{
        .name = "zsqlite-slt",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/slt_runner.zig"),
            .target = target,
            .optimize = slt_optimize,
            .link_libc = false,
            .imports = &.{
                .{
                    .name = "zsqlite",
                    .module = b.addModule("zsqlite_slt", .{
                        .root_source_file = b.path("src/lib.zig"),
                        .target = target,
                        .optimize = slt_optimize,
                        .link_libc = false,
                    }),
                },
            },
        }),
    });
    b.installArtifact(slt_exe);

    const run_slt = b.addRunArtifact(slt_exe);
    if (b.args) |args| {
        run_slt.addArgs(args);
    }
    const slt_step = b.step("slt", "Run sqllogictest files");
    slt_step.dependOn(&run_slt.step);
}
