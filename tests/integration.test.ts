import { Database } from "../src/engine/Database";
import { unlink } from "node:fs/promises";

const catalogPath = "catalog_int.json";
// Cleanup
try { await unlink(catalogPath); } catch { }
try { await unlink("data/transactions.db"); } catch { }
try { await unlink("data/transactions.idx"); } catch { }

const db = new Database(catalogPath);
console.log("Initializing DB...");
await db.init();

console.log("Creating Table...");
await db.execute("CREATE TABLE transactions (id INT PRIMARY KEY, reference STRING, amount FLOAT)");

console.log("Inserting Data...");
await db.execute("INSERT INTO transactions VALUES (1, 'REF_001', 150.50)");
await db.execute("INSERT INTO transactions VALUES (2, 'REF_002', 2000.00)");
await db.execute("INSERT INTO transactions VALUES (3, 'REF_003', 50.25)");

console.log("Selecting All...");
const all = await db.execute("SELECT * FROM transactions");
console.log(all);

console.log("Selecting WHERE amount > 1000...");
const highValue = await db.execute("SELECT reference FROM transactions WHERE amount > 1000");
console.log(highValue);

if (all.length !== 3) throw new Error("Count mismatch");
if (highValue.length !== 1) throw new Error("Where clause failed");
if (highValue[0].reference !== 'REF_002') throw new Error("Projection failed");

console.log("Integration Test Passed!");
