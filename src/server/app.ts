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
            try {
                const body = await req.json() as any;
                const sql = body.sql;
                console.log(`[SQL] ${sql}`);
                const result = await db.execute(sql);
                return Response.json({ success: true, data: result });
            } catch (e: any) {
                console.error(e);
                return Response.json({ success: false, error: e.message }, { status: 400 });
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
