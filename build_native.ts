import { spawn } from "child_process";
import { join } from "path";
import * as fs from "fs";

const isWindows = process.platform === "win32";
const nativeDir = join(process.cwd(), "src/native");
const outFile = isWindows ? "wal_debug.dll" : "native_wal.so";
const outPath = join(nativeDir, outFile);

console.log(`[Build] Detected platform: ${process.platform}`);
console.log(`[Build] Compiling C++ Native Engine...`);

if (isWindows) {
    // We assume the user has VS Build Tools environment set up manually or via specific shell
    // Just printing instructions for Windows dev as automatic compilation from pure JS without vsdevcmd is hard
    console.log("On Windows, please run 'cl /LD src/native/wal.cpp' in a VS Developer Command Prompt.");
} else {
    // Linux/macOS - use G++ or Clang
    const args = [
        "-shared",
        "-fPIC",
        "-O3", // Optimize for speed
        "-o", outPath,
        join(nativeDir, "wal.cpp")
    ];

    console.log(`[Build] Running: g++ ${args.join(" ")}`);

    const child = spawn("g++", args, { stdio: "inherit" });

    child.on("close", (code) => {
        if (code === 0) {
            console.log(`[Build] Success! Binary created at: ${outPath}`);
            // List the file to confirm
            if (fs.existsSync(outPath)) {
                console.log(`[Build] Verified file exists: ${fs.statSync(outPath).size} bytes`);
            }
        } else {
            console.error(`[Build] Compilation failed with code ${code}. Ensure 'g++' is installed.`);
            process.exit(1);
        }
    });
}
