import { Parser } from "../src/engine/Parser";

const queries = [
    "CREATE TABLE transactions (id INT PRIMARY KEY, reference STRING, amount FLOAT)",
    "INSERT INTO transactions VALUES (1, 'REF_123', 500.50)",
    "SELECT * FROM transactions WHERE id = 1",
    "SELECT reference, amount FROM transactions WHERE amount > 100 AND reference = 'REF_123'"
];

for (const sql of queries) {
    console.log(`Parsing: ${sql}`);
    const parser = new Parser(sql);
    const ast = parser.parse();
    console.log(JSON.stringify(ast, null, 2));
}

console.log("Parser Test Passed!");
