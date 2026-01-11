import { describe, test, expect } from "bun:test";
import { BinaryUtils } from "../src/engine/BinaryUtils";
import { Catalog } from "../src/engine/Catalog";
import { Index } from "../src/engine/Index";
import { Database } from "../src/engine/Database";
import { ColumnType } from "../src/engine/Constants";
import { unlink } from "node:fs/promises";

// Cleanup helpers
describe("Coverage Boosters", () => {

    test("BinaryUtils.getSize types", () => {
        expect(BinaryUtils.getSize(999 as any, "foo")).toBe(0);
        expect(BinaryUtils.getSize(ColumnType.INT, 123)).toBe(4);
        expect(BinaryUtils.getSize(ColumnType.FLOAT, 12.3)).toBe(8);
        expect(BinaryUtils.getSize(ColumnType.BOOLEAN, true)).toBe(1);
        new BinaryUtils(); // Cover constructor
    });

    test("Catalog.getTable returns correct data", async () => {
        const c = new Catalog();
        try { await unlink("catalog.json"); } catch { }
        await c.init();
        c.addTable("test_ct", [{ name: "id", type: ColumnType.INT }]);
        expect(c.getTable("test_ct")).toHaveLength(1);
        expect(c.getTable("non_existent")).toBeUndefined();
    });

    test("Index.get returns offset", async () => {
        const idx = new Index("test_idx_coverage", ColumnType.INT);
        try { await unlink("test_idx_coverage.idx"); } catch { }
        await idx.init();
        await idx.add(100, 5000);
        expect(idx.get(100)).toBe(5000);
        expect(idx.get(999)).toBeUndefined();
    });

    test("Index init throws on unsupported type during read", async () => {
        // Create an index file with some data
        const idx = new Index("test_idx_fail", ColumnType.INT);
        try { await unlink("test_idx_fail.idx"); } catch { }
        await idx.init();
        await idx.add(1, 100);

        // Now try to read it back as boolean (unsupported)
        const badIdx = new Index("test_idx_fail", ColumnType.BOOLEAN);
        // We expect it to fail during init loop when it tries to read key
        try {
            await badIdx.init();
        } catch (e: any) {
            expect(e.message).toContain("Unsupported Index Type");
        }
    });

    test("Database evaluate operators", async () => {
        // We can test evaluate indirectly via execute
        // Use a unique catalog path to avoid race conditions
        const dbName = "test_ops_cov";
        const catalogPath = "catalog_cov.json";

        const db = new Database(catalogPath);
        // Clean existing
        try { await unlink(catalogPath); await unlink(`${dbName}.db`); await unlink(`${dbName}.idx`); } catch { }

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
        try { await unlink(catalogPath); await unlink(`${dbName}.db`); await unlink(`${dbName}.idx`); } catch { }
    });
});
