import { Database } from "../src/engine/Database";
import { unlink, stat } from "node:fs/promises";
import { DATA_DIR } from "../src/engine/Constants";
import { join } from "path";

const DB_NAME = "transactions_bench";
const CATALOG_PATH = "bench_catalog.json";
const ROW_COUNT = 20000; // Increased for stress testing

async function benchmark() {
    console.log(`\n🚀 Starting PesaDB Benchmark (${ROW_COUNT} transactions)....\n`);

    // Cleanup
    const dbPath = join(DATA_DIR, `${DB_NAME}.db`);
    const idxPath = join(DATA_DIR, `${DB_NAME}.idx`);
    const walPath = join(DATA_DIR, `global.wal`); // Global WAL
    const catPath = join(DATA_DIR, CATALOG_PATH);

    try { await unlink(dbPath); } catch { }
    try { await unlink(idxPath); } catch { }
    try { await unlink(walPath); } catch { }
    try { await unlink(catPath); } catch { }

    const db = new Database(CATALOG_PATH, "bench_integrated_" + Date.now());
    await db.init();

    // 1. Create Table
    await db.execute(`CREATE TABLE ${DB_NAME} (id INT PRIMARY KEY, reference STRING, is_processed BOOLEAN, amount FLOAT)`);

    // 2. Insert Performance
    console.log("👉 Measurement: Write Performance (Insert)");
    const startInsert = performance.now();

    // START TRANSACTION (Bulk Insert Optimization)
    await db.execute("BEGIN TRANSACTION");

    for (let i = 0; i < ROW_COUNT; i++) {
        await db.execute(`INSERT INTO ${DB_NAME} VALUES (${i}, 'TXN_${i}_REF', ${i % 2 === 0}, ${Math.random() * 10000})`);
        if (i % 1000 === 0) process.stdout.write(`\rInserted ${i}/${ROW_COUNT}`);
    }
    console.log(`\rInserted ${ROW_COUNT}/${ROW_COUNT}`);

    // COMMIT TRANSACTION (Single Flush)
    await db.execute("COMMIT");

    const endInsert = performance.now();
    const durationInsert = endInsert - startInsert;
    console.log(`   ✅ Inserted ${ROW_COUNT} transactions in ${durationInsert.toFixed(2)}ms`);
    console.log(`   ⚡ Speed: ${(ROW_COUNT / (durationInsert / 1000)).toFixed(2)} txns/sec`);

    // 3. Storage Efficiency
    console.log("\n👉 Measurement: Storage Efficiency");
    let dbSize = 0;
    try {
        const stats = await stat(dbPath);
        dbSize = stats.size;
    } catch { }

    // If DB file missing (In-Memory Mode), check WAL size as primary storage
    if (dbSize === 0) {
        try {
            const stats = await stat(walPath);
            dbSize = stats.size; // Treat WAL as the "DB Size" for metric comparison
            console.log("   ℹ️  (Using WAL size as DB file is skipped in In-Memory Mode)");
        } catch { }
    }

    const jsonSize = ROW_COUNT * 75; // Approx bytes per row JSON
    console.log(`   📦 PesaDB Size: ${(dbSize / 1024).toFixed(2)} KB`);
    console.log(`   📄 JSON Size (Approx): ${(jsonSize / 1024).toFixed(2)} KB`);
    console.log(`   📉 Efficiency: ${((1 - (dbSize / jsonSize)) * 100).toFixed(2)}% smaller than JSON`);

    // 4. Read Performance (Indexed)
    console.log("\n👉 Measurement: Read Performance (Indexed Lookups)");
    const startReadIdx = performance.now();
    for (let i = 0; i < 1000; i++) {
        const target = Math.floor(Math.random() * ROW_COUNT);
        await db.execute(`SELECT * FROM ${DB_NAME} WHERE id = ${target}`);
    }
    const endReadIdx = performance.now();
    console.log(`   ✅ 1000 Random Transaction Lookups in ${(endReadIdx - startReadIdx).toFixed(2)}ms`);
    console.log(`   ⚡ Speed: ${(1000 / ((endReadIdx - startReadIdx) / 1000)).toFixed(2)} txns/sec`);

    // 5. Read Performance (Full Scan)
    console.log("\n👉 Measurement: Read Performance (High Value Transaction Scan)");
    const startScan = performance.now();
    const res = await db.execute(`SELECT * FROM ${DB_NAME} WHERE amount > 5000`);
    const endScan = performance.now();
    console.log(`   ✅ Scanned ${ROW_COUNT} rows, found ${res.length} matches in ${(endScan - startScan).toFixed(2)}ms`);

    // Cleanup
    try { await unlink(dbPath); await unlink(idxPath); await unlink(walPath); await unlink(catPath); } catch { }
}

benchmark();
