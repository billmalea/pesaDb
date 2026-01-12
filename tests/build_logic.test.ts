import { describe, expect, test } from "bun:test";
import { getBuildCommand } from "../build_native";
import { join } from "path";

describe("Build Script Logic", () => {
    test("Should generate correct g++ command for Linux", () => {
        const cwd = "/usr/src/app";
        const config = getBuildCommand("linux", cwd);

        expect(config.platform).toBe("linux");
        expect(config.cmd).toBe("g++");

        // args: -shared -fPIC -O3 -o <out> <src>
        expect(config.args[0]).toBe("-shared");
        expect(config.args[1]).toBe("-fPIC");
        expect(config.args[3]).toBe("-o");
        expect(config.args[4]).toBe(join(cwd, "src/native/native_wal.so"));
        expect(config.args[5]).toBe(join(cwd, "src/native/wal.cpp"));
    });

    test("Should return Win32 instructions for Windows", () => {
        const config = getBuildCommand("win32", "C:\\app");
        expect(config.platform).toBe("win32");
        expect(config.cmd).toBe("cl");
        expect(config.outMsg).toContain("VS Developer Command Prompt");
    });
});
