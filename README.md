# PesaDB

**PesaDB** is a lightweight, relational database management system implemented from scratch in **TypeScript** (running on **Bun and C++**). 

## 🚀 Key Features

-   **Custom Binary Storage Engine**: Instead of text-based formats (like JSON/CSV), PesaDB uses a custom binary packing protocol (`DataView`/`ArrayBuffer`) for efficiency and to demonstrate understanding of memory layout.
-   **Persistent Hash Indexing**: Enforces Primary Key uniqueness and allows O(1) lookups for `SELECT ... WHERE id = X`.
-   **Recursive Descent SQL Parser**:  parser (no Regex hacks/libraries) that converts SQL into an AST.
-   **Full CRUD Support**: Supports `CREATE`, `INSERT`, `SELECT` (with `WHERE`), `UPDATE`, and `DELETE`.
-   **Interactive REPL**: A command-line interface to interact with the DB.
-   **Web Demo**: A Transaction Manager UI with **Live Performance Benchmarking** to verify engine speed in the browser.

## 🛠️ Architecture

### 1. The Storage Engine (`src/engine/Table.ts`)
**In-Memory Architecture with WAL Persistence.**
-   **Primary Storage**: Data lives in RAM (`Row[]` + `Map<PK, Row>`) for O(1) access.
-   **Durability**: A **Write-Ahead Log (WAL)** persists all changes to disk *before* they are applied to memory.
-   **Recovery**: On startup, the engine replays the WAL to restore the in-memory state.
-   **Benefits**: 
    -   **Speed**: Reads are instant (Memory lookup). Writes are appended to log (Sequential I/O).
    -   **Safety**: Synchronous WAL ensures ACID compliance.

### 2. The Indexer
Maintains a `Map<PrimaryKey, Row>` in memory.
-   Allows the engine to check for `isPrimary` violations instantly (O(1)).
-   Provides specialized fast-path for `SELECT ... WHERE pk = val`.

### 3. The Query Engine (`src/engine/Parser.ts`, `src/engine/Database.ts`)
-   **Parser**: Tokenizes SQL string and recursively builds nodes (`SelectStmt`, `Expr`, etc).
-   **Executor**: Traverses the AST and calls `Table` methods.

## 📦 Tech Stack

-   **Runtime**: [Bun](https://bun.sh) (Chosen for speed and native TypeScript support).
-   **Language**: TypeScript.
-   **Dependencies**: None! (Standard Library only).

## 🏃 Usage

### 1. Installation
Ensure you have [Bun](https://bun.sh) installed.
```bash
bun install
```

### 2. Run the Interactive REPL
```bash
bun src/repl/index.ts
```
*Example:*
```sql
CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT)
INSERT INTO users VALUES (1, 'Alice', 25)
SELECT * FROM users
```

### 3. Run the Web Demo
```bash
bun run src/server/app.ts
```
Open [http://localhost:3000](http://localhost:3000) in your browser.

#### Demo Screenshots

1.  **Initial Dashboard**:
    ![Start](docs/images/bench-result.jpg)

2.  **Live Benchmark Progress**:
    ![Progress](docs/images/bench-progress.jpg)

3.  **Benchmark Results & Data Inspection**:
    ![Results](docs/images/bench-start.jpg)



## 📊 Performance & Benchmarks

## 📊 Performance & Benchmarks

Benchmarks were run on a dataset of **20,000 Financial Transactions** (simulating Pesapal data) using **Native WAL (C++)** on Windows.

| Metric | Result | Description |
| :--- | :--- | :--- |
| **Write Speed** | **~28,322 txns/sec** | Memory Insert + Native WAL Append. |
| **Read Speed (Indexed)** | **~20,865 txns/sec** | O(1) Memory Lookup. |
| **Efficiency** | **100% JSON Reduction** | Data is stored primarily in WAL; main DB file is a snapshot. |

### 🚀 Real Benchmark Output (`tests/benchmark.ts`)
```text
🚀 Starting PesaDB Benchmark (20000 transactions)...

👉 Measurement: Write Performance (Insert)
   ✅ Inserted 20000 transactions in 706.15ms
   ⚡ Speed: 28322.76 txns/sec

👉 Measurement: Read Performance (Indexed Lookups)
   ✅ 1000 Random Transaction Lookups in 47.93ms
   ⚡ Speed: 20865.11 txns/sec
```
> **Note**: The **Indexed Read** is blazing fast because it hits pure memory. Persistence is guaranteed by the WAL.


## 🧪 Verification

Run the test suite to verify the Core Engine and SQL Parser:
```bash
bun test                    # Run all tests
bun test tests/parser.test.ts # Run specific test
```

## ⚖️ License
MIT.
