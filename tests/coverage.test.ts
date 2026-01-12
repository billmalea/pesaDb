import { describe, test, expect } from "bun:test";
import { Catalog } from "../src/engine/Catalog";
import { Database } from "../src/engine/Database";
import { ColumnType } from "../src/engine/Constants";
import { unlink } from "node:fs/promises";

// Cleanup helpers
describe("Coverage Boosters", () => {

    test("Catalog.getTable returns correct data", async () => {
        const c = new Catalog();
        try { await unlink("data/catalog.json"); } catch { }
        await c.init();
        c.addTable("test_ct", [{ name: "id", type: ColumnType.INT, isPrimary: true }]);
        expect(c.getTable("test_ct")).toHaveLength(1);
        expect(c.getTable("non_existent")).toBeUndefined();
    });

    test("Database evaluate operators", async () => {
        // We can test evaluate indirectly via execute
        // Use a unique catalog path to avoid race conditions
        const dbName = "test_ops_cov_" + Date.now();
        const catalogPath = `catalog_cov_${Date.now()}.json`;

        const db = new Database(catalogPath, dbName);
        // Clean existing (though random should handle it)
        try { await unlink("data/" + catalogPath); await unlink(`data/${dbName}.db`); await unlink(`data/${dbName}.idx`); await unlink(`data/${dbName}.wal`); } catch { }

        await db.init();
        await db.execute(`CREATE TABLE ${dbName} (id INT PRIMARY KEY, val INT)`);
        await db.execute(`INSERT INTO ${dbName} VALUES (1, 10)`);
        await db.execute(`INSERT INTO ${dbName} VALUES (2, 20)`);

        const res1 = await db.execute(`SELECT * FROM ${dbName} WHERE val < 15`);
        expect(res1).toHaveLength(1);
        expect(res1[0].id).toBe(1);

        const res2 = await db.execute(`SELECT * FROM ${dbName} WHERE val > 15 AND val < 25`);
        expect(res2).toHaveLength(1);
        expect(res2[0].id).toBe(2);

        const res3 = await db.execute(`SELECT * FROM ${dbName} WHERE val = 10 OR val = 20`);
        expect(res3).toHaveLength(2);

        // Cleanup
        await db.close();
        try { await unlink("data/" + catalogPath); await unlink(`data/${dbName}.db`); await unlink(`data/${dbName}.idx`); await unlink(`data/${dbName}.wal`); } catch { }
    });

    test("Database DROP TABLE full flow", async () => {
        const dbName = "test_drop_cov_" + Date.now();
        const catalogPath = `catalog_drop_cov_${Date.now()}.json`;
        const db = new Database(catalogPath, dbName);

        // Cleanup
        try { await unlink("data/" + catalogPath); await unlink(`data/${dbName}.db`); await unlink(`data/${dbName}.idx`); await unlink(`data/${dbName}.wal`); } catch { }

        await db.init();
        await db.execute(`CREATE TABLE ${dbName} (id INT PRIMARY KEY)`);
        await db.execute(`INSERT INTO ${dbName} VALUES (1)`);

        // Verify Files Exist
        const fs = await import('node:fs/promises');
        await fs.access(`data/${dbName}.wal`); // Check WAL as DB file might not exist yet if no checkpoint

        // DROP
        await db.execute(`DROP TABLE ${dbName}`);

        // Verify Files Gone
        try {
            await fs.access(`data/${dbName}.db`);
            throw new Error("File should verify deleted"); // Should not reach here
        } catch (e: any) {
            expect(e.code).toBe("ENOENT");
        }

        // Verify Catalog Update
        expect(db["catalog"].getTable(dbName)).toBeUndefined();
        await db.close();
    });

    test("Database Transactions BEGIN/COMMIT", async () => {
        const dbName = "test_txn_cov_" + Date.now();
        const catalogPath = `catalog_txn_cov_${Date.now()}.json`;
        const db = new Database(catalogPath, dbName);
        try { await unlink("data/" + catalogPath); await unlink(`data/${dbName}.db`); await unlink(`data/${dbName}.idx`); await unlink(`data/${dbName}.wal`); } catch { }

        await db.init();
        await db.execute(`CREATE TABLE ${dbName} (id INT PRIMARY KEY)`);

        const resBegin = await db.execute("BEGIN");
        expect(resBegin.message).toBe("Transaction Started");
        expect(db["inTransaction"]).toBe(true);

        await db.execute(`INSERT INTO ${dbName} VALUES (1)`);

        const resCommit = await db.execute("COMMIT");
        expect(resCommit.message).toBe("Transaction Committed");
        expect(db["inTransaction"]).toBe(false);
        await db.close();
        try { await unlink("data/" + catalogPath); await unlink(`data/${dbName}.db`); await unlink(`data/${dbName}.idx`); await unlink(`data/${dbName}.wal`); } catch { }
    });

    test("WAL Manual Coverage (Clear, Close)", async () => {
        const { WalManager, WalOpType } = await import("../src/engine/WAL");
        const walName = "test_wal_manual_" + Date.now();
        const walInit = new WalManager(walName);
        try { await unlink(`data/${walName}.wal`); } catch { }

        await walInit.init();
        await walInit.append(1, WalOpType.INSERT, "tbl", { a: 1 }, false); // Buffered
        await walInit.flush(); // Manual flush

        await walInit.clear();
        await walInit.close();

        // Test append throws if not init?
        // Actually WalManager.append checks if (!this.fileHandle) throw
        // Since we closed it, we need a new instance to fail properly on NativeWAL check?
        // NativeWAL checks handle.

        const walBad = new WalManager("test_wal_bad_" + Date.now());
        try {
            await walBad.append(1, WalOpType.INSERT, "t", {}, true);
        } catch (e: any) {
            expect(e.message).toBe("WAL not initialized");
        }
    });

});
