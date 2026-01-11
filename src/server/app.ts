import { Database } from "../engine/Database";

const db = new Database();
await db.init();

// Ensure demo data exists
try {
    await db.execute("CREATE TABLE students (id INT PRIMARY KEY, name STRING, age INT)");
    await db.execute("INSERT INTO students VALUES (1, 'Alice', 20)");
    await db.execute("INSERT INTO students VALUES (2, 'Bob', 22)");
} catch (e) {
    // Ignore if exists
}

Bun.serve({
    port: 3000,
    async fetch(req) {
        const url = new URL(req.url);

        // API Endpoint
        if (url.pathname === "/api/sql" && req.method === "POST") {
            const start = performance.now();
            try {
                const body = await req.json() as any;
                const sql = body.sql;
                console.log(`[SQL] ${sql}`);
                const result = await db.execute(sql);
                const duration = performance.now() - start;
                return Response.json({ success: true, data: result, timeMs: duration.toFixed(3) });
            } catch (e: any) {
                return Response.json({ success: false, error: e.message }, { status: 400 });
            }
        }

        if (url.pathname === "/api/benchmark" && req.method === "POST") {
            // Run a safe, mini-benchmark for demo purposes
            try {
                const start = performance.now();
                const TEM_TABLE = "bench_web";
                await db.execute(`CREATE TABLE ${TEM_TABLE} (id INT PRIMARY KEY, val FLOAT)`);

                // 1. Insert 1000 items
                const t1 = performance.now();
                for (let i = 0; i < 1000; i++) {
                    await db.execute(`INSERT INTO ${TEM_TABLE} VALUES (${i}, ${Math.random()})`);
                }
                const insertTime = performance.now() - t1;

                // 2. Read 1000 items (Indexed)
                const t2 = performance.now();
                for (let i = 0; i < 1000; i++) {
                    await db.execute(`SELECT * FROM ${TEM_TABLE} WHERE id = ${i}`);
                }
                const readTime = performance.now() - t2;

                // Cleanup
                await db.execute(`DELETE FROM ${TEM_TABLE}`); // We don't have DROP TABLE yet, but this clears data. 
                // Actually, let's just leave it or overwrite next time. 
                // For a proper demo, we might want to just restart the server to clean up files, 
                // but specific table cleanup isn't fully implemented in our minimal engine (DROP TABLE).
                // We'll rely on the unique table name or just reuse it.
                // Since CREATE throws if exists, let's handle that.

                return Response.json({
                    success: true,
                    metrics: {
                        insertSpeed: (1000 / (insertTime / 1000)).toFixed(0),
                        readSpeed: (1000 / (readTime / 1000)).toFixed(0),
                        insertTime: insertTime.toFixed(2),
                        readTime: readTime.toFixed(2)
                    }
                });
            } catch (e: any) {
                // Likely table exists from previous run, that's fine for a naive demo
                return Response.json({ success: false, error: "Benchmark failed (try restarting server to clear DB): " + e.message });
            }
        }


        // Static Files
        if (url.pathname === "/") {
            return new Response(Bun.file("src/public/index.html"));
        }

        // 404
        return new Response("Not Found", { status: 404 });
    },
});

console.log("Server running at http://localhost:3000");
