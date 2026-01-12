import { Database } from "../src/engine/Database";
import { unlink, writeFile } from "node:fs/promises";
import { DATA_DIR } from "../src/engine/Constants";
import { join } from "path";
import { describe, test, expect } from "bun:test";

const DB_NAME = "recovery_test";
const CATALOG_PATH = "recovery_catalog.json";

describe("Crash Recovery", () => {
    test("Full Recovery Flow", async () => {
        console.log(`\n🧪 Starting Recovery Test...\n`);

        const dbPath = join(DATA_DIR, `${DB_NAME}.db`);
        const idxPath = join(DATA_DIR, `${DB_NAME}.idx`);
        const walPath = join(DATA_DIR, `global.wal`);
        const catPath = join(DATA_DIR, CATALOG_PATH);

        // 1. Clean Slate
        try { await unlink(dbPath); } catch { }
        try { await unlink(idxPath); } catch { }
        try { await unlink(walPath); } catch { }
        try { await unlink(catPath); } catch { }

        // 2. Init and Insert Data
        console.log("👉 Step 1: Inserting Data...");
        const db1 = new Database(CATALOG_PATH);
        await db1.init();
        await db1.execute(`CREATE TABLE ${DB_NAME} (id INT PRIMARY KEY, val STRING)`);
        await db1.execute(`INSERT INTO ${DB_NAME} VALUES (1, 'Data 1')`);
        await db1.execute(`INSERT INTO ${DB_NAME} VALUES (2, 'Data 2')`);
        console.log("   ✅ Inserted 2 rows.");

        // 3. Simulate Crash: Corrupt/Delete the Data File but keep WAL
        console.log("👉 Step 2: Simulating Data File Corruption (Crash)...");
        // We overwrite the DB file with just the header (empty table)
        // Or just delete it? If we delete it, `Table.init` creates a new one.
        // Let's delete it.
        await unlink(dbPath);
        // Also delete index to force full recovery reliance
        await unlink(idxPath);
        console.log("   ✅ Deleted .db and .idx files (WAL remains).");

        // 4. Restart DB (Recovery)
        console.log("👉 Step 3: Restarting DB...");
        const db2 = new Database(CATALOG_PATH);
        await db2.init(); // This should trigger WAL recovery

        // 5. Verify Data
        console.log("👉 Step 4: Verifying Data...");
        const rows = await db2.execute(`SELECT * FROM ${DB_NAME}`);
        console.log(`   found ${rows.length} rows.`);

        expect(rows).toHaveLength(2);
        expect(rows[0].val).toBe('Data 1');
        expect(rows[1].val).toBe('Data 2');
        console.log("   ✅ SUCCESS: Data recovered from WAL!");

        // Cleanup
        try { await unlink(dbPath); await unlink(idxPath); await unlink(walPath); await unlink(catPath); } catch { }
    });
});
