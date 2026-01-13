import { Database } from "../engine/Database";

const db = new Database();
await db.init();

console.log("PesaDB REPL v1.0");
console.log("Type 'exit' to quit.");

const prompt = "pesadb> ";
process.stdout.write(prompt);

import { createInterface } from "readline";
const rl = createInterface({ input: process.stdin, output: process.stdout, terminal: false });

for await (const line of rl) {
    const input = line.trim();
    if (input === 'exit') process.exit(0);
    if (input === '') {
        process.stdout.write(prompt);
        continue;
    }

    try {
        const start = performance.now();
        const result = await db.execute(input);
        const end = performance.now();

        if (Array.isArray(result)) {
            console.table(result);
            console.log(`(${result.length} rows) [${(end - start).toFixed(2)}ms]`);
        } else {
            console.log(result);
            console.log(`[${(end - start).toFixed(2)}ms]`);
        }
    } catch (e: any) {
        console.error("Error:", e.message);
    }
    process.stdout.write(prompt);
}
