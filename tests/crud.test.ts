import { Database } from "../src/engine/Database";
import { unlink } from "node:fs/promises";

// Cleanup
const catalogPath = "catalog_crud.json";
// Cleanup
try { await unlink("data/" + catalogPath); } catch { }
try { await unlink("data/test_crud_db.db"); } catch { }
try { await unlink("data/test_crud_db.wal"); } catch { }

const dbName = "test_crud_db";
const db = new Database(catalogPath, dbName);
await db.init();

console.log("Create...");
await db.execute(`CREATE TABLE ${dbName} (id INT PRIMARY KEY, val STRING)`);
await db.execute(`INSERT INTO ${dbName} VALUES (1, 'A')`);
await db.execute(`INSERT INTO ${dbName} VALUES (2, 'B')`);
await db.execute(`INSERT INTO ${dbName} VALUES (3, 'C')`);

console.log("Update...");
await db.execute(`UPDATE ${dbName} SET val = 'Z' WHERE id = 2`);

console.log("Verify Update...");
const res1 = await db.execute(`SELECT * FROM ${dbName} WHERE id = 2`);
if (res1[0].val !== 'Z') throw new Error("Update failed");

console.log("Delete...");
await db.execute(`DELETE FROM ${dbName} WHERE id = 1`);

console.log("Verify Delete...");
const res2 = await db.execute(`SELECT * FROM ${dbName}`);
if (res2.length !== 2) throw new Error("Delete failed count");
if (res2.find((r: any) => r.id === 1)) throw new Error("Delete failed (found id 1)");

console.log("CRUD Passed!");
await db.close();
