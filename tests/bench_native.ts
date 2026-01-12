import { WalManager, WalOpType } from "../src/engine/WAL";
import { NativeWalManager } from "../src/engine/NativeWAL";
import { unlink } from "node:fs/promises";

const COUNT = 20000;
const DATA = { id: 1, name: "bench", val: 123.45 };

async function benchmark() {
    console.log(`\n🚀 Native vs Managed WAL Benchmark (${COUNT} writes)...\n`);

    // 1. Managed (TypeScript) WAL BOOSTER
    const walTS = new WalManager("bench_ts");
    try { await unlink("data/bench_ts.wal"); } catch { }
    await walTS.init();

    console.log("👉 TypeScript WAL (Buffered Mode)...");
    const startTS = performance.now();
    for (let i = 0; i < COUNT; i++) {
        await walTS.append(i, WalOpType.INSERT, "tbl", DATA, false);
    }
    await walTS.flush();
    const endTS = performance.now();
    console.log(`   TS Speed: ${(COUNT / ((endTS - startTS) / 1000)).toFixed(2)} ops/sec`);
    await walTS.close();

    // 2. Native (C++) WAL
    const walNative = new NativeWalManager("bench_native");
    try { await unlink("data/bench_native.wal"); } catch { }
    await walNative.init();

    console.log("👉 C++ Native WAL (Buffered Mode)...");
    const startNative = performance.now();
    for (let i = 0; i < COUNT; i++) {
        await walNative.append(i, i, WalOpType.INSERT, "tbl", JSON.stringify(DATA), false);
    }
    await walNative.flush();
    const endNative = performance.now();
    console.log(`   C++ Speed: ${(COUNT / ((endNative - startNative) / 1000)).toFixed(2)} ops/sec`);
    await walNative.close();

    // Cleanup
    try { await unlink("data/bench_ts.wal"); } catch { }
    try { await unlink("data/bench_native.wal"); } catch { }
}

benchmark();
