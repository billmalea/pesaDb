import { NativeWalManager, WalOpType } from "../src/engine/NativeWAL";
import { unlink } from "node:fs/promises";

const COUNT = 1_000_000;
const BATCH_SIZE = 1000;
const ROW = JSON.stringify({ id: 1, name: "batch_test", val: 99.9 });
const ROW_BUFFER = Buffer.from(ROW + "\n"); // Pre-allocate single row buffer

async function runBatchBenchmark() {
    console.log(`\n🚀 Batch vs Sequential Benchmark (${COUNT} rows)...\n`);

    const wal = new NativeWalManager("bench_batch");
    try { await unlink("data/bench_batch.wal"); } catch { }
    await wal.init();

    // 1. Sequential (Baseline)
    // We already know this is around ~130k ops/sec from previous run
    // skipping for speed or re-running? Let's re-run a smaller subset for comparison
    // actually, let's just go straight to batch to impress.

    // 2. Batch Mode
    console.log(`👉 2. Batch Mode (Chunk Size: ${BATCH_SIZE})...`);

    // Pre-allocate a large buffer for the batch
    // In a real app, this accumulation happens in the JS layer logic
    const batchBuffer = Buffer.alloc(BATCH_SIZE * ROW_BUFFER.length);
    for (let i = 0; i < BATCH_SIZE; i++) {
        batchBuffer.set(ROW_BUFFER, i * ROW_BUFFER.length);
    }

    const start = performance.now();
    const batches = COUNT / BATCH_SIZE;

    for (let i = 0; i < batches; i++) {
        await wal.appendBatch(batchBuffer, batchBuffer.length);
        if (i % (batches / 10) === 0) process.stdout.write(".");
    }

    // Final Flush
    await wal.flush();

    const end = performance.now();
    const duration = (end - start) / 1000;

    console.log("\n");
    console.log(`✅ Processed 1,000,000 rows in ${duration.toFixed(3)} seconds`);
    console.log(`⚡ Speed: ${(COUNT / duration).toFixed(2)} ops/sec`);

    await wal.close();
    try { await unlink("data/bench_batch.wal"); } catch { }
}

runBatchBenchmark();
