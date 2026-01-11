import type { Column } from "../src/engine/Table";
import { Table } from "../src/engine/Table";
import { ColumnType } from "../src/engine/Constants";
import { unlink } from "node:fs/promises";

// Cleanup old files
try { await unlink("data/test_users.db"); } catch { }
try { await unlink("data/test_users.idx"); } catch { }

const columns: Column[] = [
    { name: "id", type: ColumnType.INT, isPrimary: true },
    { name: "name", type: ColumnType.STRING },
    { name: "is_active", type: ColumnType.BOOLEAN },
    { name: "score", type: ColumnType.FLOAT }
];

const table = new Table("test_users", columns);

console.log("Initializing Table...");
await table.init();

console.log("Inserting Rows...");
await table.insert({ id: 1, name: "Alice", is_active: true, score: 95.5 });
await table.insert({ id: 2, name: "Bob", is_active: false, score: 88.0 });
await table.insert({ id: 3, name: "Charlie", is_active: true, score: 72.3 });

console.log("Reading Rows...");
const rows = await table.selectAll();
console.log(rows);

if (rows.length !== 3) throw new Error("Row count mismatch!");

// Test Duplicate Key
console.log("Testing Duplicate Key...");
try {
    await table.insert({ id: 1, name: "Alice Clone", is_active: true, score: 0 });
    throw new Error("Failed to catch Duplicate Key!");
} catch (e: any) {
    if (!e.message.includes("Duplicate Entry")) throw e;
    console.log("Caught Duplicate Key correctly.");
}

console.log("Test Passed!");
