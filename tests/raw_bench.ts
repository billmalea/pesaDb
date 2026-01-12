
import { open } from "node:fs/promises";
import { unlink } from "node:fs/promises";

const COUNT = 20000;
const DATA = new Uint8Array(new TextEncoder().encode('{"id": 1, "name": "benchmark_row", "active": true, "val": 123.45}\n'));

async function benchRawFS() {
    console.log(`\n🚀 Starting Raw FS Benchmark (${COUNT} writes)...\n`);
    const path = "raw_bench.log";
    try { await unlink(path); } catch { }

    // 1. Sync Writes (Like Auto-Commit PesaDB)
    console.log("👉 Mode 1: Open, Write, Sync (Per Row)");
    const fh = await open(path, 'a+');
    const start1 = performance.now();
    for (let i = 0; i < COUNT; i++) {
        await fh.appendFile(DATA);
        await fh.sync();
    }
    const end1 = performance.now();
    console.log(`   Speed: ${(COUNT / ((end1 - start1) / 1000)).toFixed(2)} ops/sec`);
    await fh.close();

    // 2. Buffered Writes (Like Transaction PesaDB)
    console.log("👉 Mode 2: Buffered Write (One Sync at end)");
    try { await unlink(path); } catch { }
    const fh2 = await open(path, 'a+');
    const start2 = performance.now();
    for (let i = 0; i < COUNT; i++) {
        await fh2.appendFile(DATA);
    }
    await fh2.sync();
    const end2 = performance.now();
    console.log(`   Speed: ${(COUNT / ((end2 - start2) / 1000)).toFixed(2)} ops/sec`);
    await fh2.close();

    try { await unlink(path); } catch { }
}

benchRawFS();
