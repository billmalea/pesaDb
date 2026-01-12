import { spawn } from "child_process";
import { join } from "path";
import { existsSync, unlinkSync } from "fs";

// Path to vcvars64.bat (Adjust if needed based on previous find result)
const VCVARS_PATH = String.raw`C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat`;

async function build() {
    console.log("🚀 Building Native C++ Engine...");

    const source = join(process.cwd(), "src/native/wal.cpp");
    const outputDir = join(process.cwd(), "src/native");
    const outputDll = "wal_debug.dll";

    // Clean previous build
    if (existsSync(join(outputDir, outputDll))) {
        unlinkSync(join(outputDir, outputDll));
    }

    // Command to run inside the Visual Studio environment
    // /LD = Create DLL
    // /MD = Multithreaded DLL Runtime
    // /O2 = Maximize Speed
    // Quote the entire command string for cmd /c:  cmd /c " "path" && cl "
    const cmd = `"${VCVARS_PATH}" && cl /LD /MD /O2 "${source}" /Fe:"${join(outputDir, outputDll)}"`;

    console.log(`Executing: ${cmd}`);

    const child = spawn("cmd.exe", ["/s", "/c", `"${cmd}"`], {
        stdio: "inherit",
        cwd: process.cwd(),
        windowsVerbatimArguments: true
    });

    child.on("close", (code) => {
        if (code === 0) {
            console.log("✅ Build Successful: src/native/wal.dll");
        } else {
            console.error(`❌ Build Failed with code ${code}`);
            process.exit(1);
        }
    });
}

build();
