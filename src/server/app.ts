import { Database } from "../engine/Database";

const db = new Database();
await db.init();

// Ensure demo data exists
try {
    await db.execute("CREATE TABLE transactions (id INT PRIMARY KEY, reference STRING, amount FLOAT)");
    await db.execute("INSERT INTO transactions VALUES (1, 'REF_START_1', 150.00)");
    await db.execute("INSERT INTO transactions VALUES (2, 'REF_START_2', 2500.50)");
} catch (e) {
    // Ignore if exists
}

let isBenchmarking = false;

Bun.serve({
    port: 3000,
    idleTimeout: 60, // Increase to 60s for heavy benchmarks
    async fetch(req) {
        const url = new URL(req.url);

        // API Endpoint
        if (url.pathname === "/api/sql" && req.method === "POST") {
            if (isBenchmarking) {
                return Response.json({ success: false, error: "Database is busy (Benchmark in progress)" }, { status: 503 });
            }
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
            if (isBenchmarking) {
                return Response.json({ success: false, error: "Benchmark already in progress" }, { status: 429 });
            }
            isBenchmarking = true;

            // Return a ReadableStream
            return new Response(new ReadableStream({
                async start(controller) {
                    const sessionID = Math.floor(Math.random() * 100000);
                    const TEM_TABLE = `bench_txn_${sessionID}`;

                    const log = (msg: string) => {
                        console.log(msg);
                        controller.enqueue(new TextEncoder().encode("LOG:" + msg + "\n"));
                    };

                    try {
                        const tTotalStart = performance.now();
                        // 0. Clean Setup
                        await db.execute(`CREATE TABLE ${TEM_TABLE} (id INT PRIMARY KEY, ref STRING, amount FLOAT, currency STRING, status STRING)`);

                        // 1. Insert 5000
                        log(`[BENCH] Starting 5,000 Inserts into ${TEM_TABLE}...`);
                        const t1 = performance.now();
                        const currencies = ["KES", "USD", "EUR", "GBP"];
                        const COUNT = 5000;
                        for (let i = 0; i < COUNT; i++) {
                            const ref = `TXN-${20260000 + i}`;
                            const amt = parseFloat((Math.random() * 10000).toFixed(2));
                            const curr = currencies[i % 4];
                            await db.execute(`INSERT INTO ${TEM_TABLE} VALUES (${i}, '${ref}', ${amt}, '${curr}', 'PENDING')`);
                            if (i % 1000 === 0 && i > 0) {
                                log(`[BENCH] Inserted ${i} records...`);
                                // Yield control slightly to ensure flush
                                await new Promise(r => setTimeout(r, 0));
                            }
                        }
                        const insertTime = performance.now() - t1;
                        log(`[BENCH] Inserts completed in ${insertTime.toFixed(2)}ms`);

                        // 2. Read 5000
                        log(`[BENCH] Starting 5,000 Indexed Reads...`);
                        const t2 = performance.now();
                        for (let i = 0; i < COUNT; i++) {
                            await db.execute(`SELECT * FROM ${TEM_TABLE} WHERE id = ${i}`);
                            if (i % 1000 === 0 && i > 0) {
                                log(`[BENCH] Read ${i} records...`);
                                await new Promise(r => setTimeout(r, 0));
                            }
                        }
                        const readTime = performance.now() - t2;
                        log(`[BENCH] Reads completed in ${readTime.toFixed(2)}ms`);

                        // 3. Bulk Update
                        log(`[BENCH] Starting Bulk Update of 5,000 records...`);
                        const t3 = performance.now();
                        await db.execute(`UPDATE ${TEM_TABLE} SET status = 'COMPLETED' WHERE id >= 0`);
                        const updateTime = performance.now() - t3;
                        log(`[BENCH] Bulk Update completed in ${updateTime.toFixed(2)}ms`);

                        const metrics = {
                            insertSpeed: (COUNT / (insertTime / 1000)).toFixed(0),
                            readSpeed: (COUNT / (readTime / 1000)).toFixed(0),
                            updateSpeed: (COUNT / (updateTime / 1000)).toFixed(0),
                            insertTime: insertTime.toFixed(2),
                            readTime: readTime.toFixed(2),
                            updateTime: updateTime.toFixed(2)
                        };

                        // Send Final JSON
                        const finalData = JSON.stringify({
                            success: true,
                            tableName: TEM_TABLE,
                            metrics
                        });
                        controller.enqueue(new TextEncoder().encode("RESULT:" + finalData + "\n"));
                        controller.close();
                    } catch (e: any) {
                        const errData = JSON.stringify({ success: false, error: "Benchmark failed: " + e.message });
                        controller.enqueue(new TextEncoder().encode("RESULT:" + errData));
                        controller.close();
                    } finally {
                        isBenchmarking = false;
                    }
                }
            }), {
                headers: { "Content-Type": "text/plain" }
            });
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
