import { NativeWalManager, WalOpType } from "../src/engine/NativeWAL";
import { unlink } from "node:fs/promises";

const COUNT = 1_000_000;
const DATA = { id: 1, name: "million_row_test", val: 999.99 };

async function runOneMillion() {
    console.log(`\n🚀 1 Million Row Challenge (C++ Native WAL)...\n`);

    const wal = new NativeWalManager("bench_1m");
    try { await unlink("data/bench_1m.wal"); } catch { }
    await wal.init();

    const start = performance.now();
    for (let i = 0; i < COUNT; i++) {
        // Buffered append (sync=false)
        await wal.append(i, i, WalOpType.INSERT, "tbl", JSON.stringify(DATA), false);

        if (i % 100_000 === 0) process.stdout.write(".");
    }
    // Final Flush guarantees durability of the whole batch
    await wal.flush();

    const end = performance.now();
    const duration = (end - start) / 1000;

    console.log("\n");
    console.log(`✅ Compressed 1,000,000 writes in ${duration.toFixed(2)} seconds`);
    console.log(`⚡ Speed: ${(COUNT / duration).toFixed(2)} ops/sec`);

    await wal.close();
    try { await unlink("data/bench_1m.wal"); } catch { }
}

runOneMillion();
