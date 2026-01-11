import { Database } from "../src/engine/Database";
import { unlink } from "node:fs/promises";

// Cleanup
const catalogPath = "catalog_crud.json";
// Cleanup
try { await unlink(catalogPath); } catch { }
try { await unlink("test_crud.db"); } catch { }
try { await unlink("test_crud.idx"); } catch { }

const db = new Database(catalogPath);
await db.init();

console.log("Create...");
await db.execute("CREATE TABLE test_crud (id INT PRIMARY KEY, val STRING)");
await db.execute("INSERT INTO test_crud VALUES (1, 'A')");
await db.execute("INSERT INTO test_crud VALUES (2, 'B')");
await db.execute("INSERT INTO test_crud VALUES (3, 'C')");

console.log("Update...");
await db.execute("UPDATE test_crud SET val = 'Z' WHERE id = 2");

console.log("Verify Update...");
const res1 = await db.execute("SELECT * FROM test_crud WHERE id = 2");
if (res1[0].val !== 'Z') throw new Error("Update failed");

console.log("Delete...");
await db.execute("DELETE FROM test_crud WHERE id = 1");

console.log("Verify Delete...");
const res2 = await db.execute("SELECT * FROM test_crud");
if (res2.length !== 2) throw new Error("Delete failed count");
if (res2.find((r: any) => r.id === 1)) throw new Error("Delete failed (found id 1)");

console.log("CRUD Passed!");
