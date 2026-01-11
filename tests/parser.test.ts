import { Parser } from "../src/engine/Parser";

const queries = [
    "CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT)",
    "INSERT INTO users VALUES (1, 'Bill', 30)",
    "SELECT * FROM users WHERE id = 1",
    "SELECT name, age FROM users WHERE age > 20 AND name = 'Bill'"
];

for (const sql of queries) {
    console.log(`Parsing: ${sql}`);
    const parser = new Parser(sql);
    const ast = parser.parse();
    console.log(JSON.stringify(ast, null, 2));
}

console.log("Parser Test Passed!");
