import { Database } from "../src/engine/Database";
import { unlink } from "node:fs/promises";

const catalogPath = "catalog_int.json";
// Cleanup
try { await unlink("data/" + catalogPath); } catch { }
try { await unlink("data/test_int_db.db"); } catch { }
try { await unlink("data/test_int_db.wal"); } catch { }

const dbName = "test_int_db";
const db = new Database(catalogPath, dbName);
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

// --- JOIN TEST ---
console.log("TEST: Joining Users and Orders...");
await db.execute("CREATE TABLE users (id INT PRIMARY KEY, name STRING)");
await db.execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total FLOAT)");

await db.execute("INSERT INTO users VALUES (1, 'Alice')");
await db.execute("INSERT INTO users VALUES (2, 'Bob')");

await db.execute("INSERT INTO orders VALUES (101, 1, 50.00)");
await db.execute("INSERT INTO orders VALUES (102, 1, 75.00)");
await db.execute("INSERT INTO orders VALUES (103, 2, 20.00)");

const joinRes = await db.execute("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
console.log("Join Result:", JSON.stringify(joinRes, null, 2));

if (joinRes.length !== 3) throw new Error(`Join failed. Expected 3 rows, got ${joinRes.length}`);
const aliceOrders = joinRes.filter((r: any) => r.name === 'Alice');
if (aliceOrders.length !== 2) throw new Error("Join logic error: Alice should have 2 orders");

console.log("JOIN Logic Verified! ✅");

await db.close();
