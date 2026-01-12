import { spawn } from "child_process";
import { join } from "path";
import * as fs from "fs";

export function getBuildCommand(platform: string, cwd: string) {
    const nativeDir = join(cwd, "src/native");
    if (platform === "win32") {
        return {
            platform: "win32",
            cmd: "cl",
            args: ["/LD", join(nativeDir, "wal.cpp")],
            outMsg: "On Windows, please run 'cl /LD src/native/wal.cpp' in a VS Developer Command Prompt."
        };
    } else {
        const outPath = join(nativeDir, "native_wal.so");
        const args = [
            "-shared",
            "-fPIC",
            "-O3",
            "-o", outPath,
            join(nativeDir, "wal.cpp")
        ];
        return {
            platform: "linux",
            cmd: "g++",
            args: args,
            outPath: outPath
        };
    }
}

async function run() {
    const config = getBuildCommand(process.platform, process.cwd());

    console.log(`[Build] Detected platform: ${config.platform}`);
    console.log(`[Build] Compiling C++ Native Engine...`);

    if (config.platform === "win32") {
        console.log(config.outMsg);
    } else {
        console.log(`[Build] Running: ${config.cmd} ${config.args.join(" ")}`);

        try {
            const child = spawn(config.cmd, config.args, { stdio: "inherit" });

            child.on("close", (code) => {
                if (code === 0) {
                    console.log(`[Build] Success! Binary created at: ${config.outPath}`);
                    if (fs.existsSync(config.outPath!)) {
                        console.log(`[Build] Verified file exists: ${fs.statSync(config.outPath!).size} bytes`);
                    }
                } else {
                    console.error(`[Build] Compilation failed with code ${code}. Ensure 'g++' is installed.`);
                    process.exit(1);
                }
            });

            child.on("error", (err) => {
                console.error(`[Build] Failed to start subprocess: ${err.message}`);
                process.exit(1);
            });

        } catch (e) {
            console.error(e);
            process.exit(1);
        }
    }
}

// Check if running directly (bun run build_native.ts)
if (import.meta.main) {
    run();
}
