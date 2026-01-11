import { Catalog } from "./Catalog";
import { Table, type Column, type Row } from "./Table";
import { Parser } from "./Parser";
import type { InsertStmt, SelectStmt, CreateStmt, DeleteStmt, UpdateStmt, Expr } from "./AST";
import { mkdir } from "node:fs/promises";
import { DATA_DIR } from "./Constants";

export class Database {
    private catalog: Catalog;
    private tables: Map<string, Table> = new Map();

    constructor(customCatalogPath?: string) {
        this.catalog = new Catalog(customCatalogPath);
    }

    async init() {
        try { await mkdir(DATA_DIR, { recursive: true }); } catch { }
        await this.catalog.init();
        // Load existing tables
        for (const [name, columns] of Object.entries(this.catalog.data.tables)) {
            const table = new Table(name, columns);
            await table.init();
            this.tables.set(name, table);
        }
    }

    async execute(sql: string): Promise<any> {
        const parser = new Parser(sql);
        const ast = parser.parse();

        if (ast.type === 'CREATE') return this.execCreate(ast);
        if (ast.type === 'INSERT') return this.execInsert(ast);
        if (ast.type === 'SELECT') return this.execSelect(ast);
        if (ast.type === 'DELETE') return this.execDelete(ast);
        if (ast.type === 'UPDATE') return this.execUpdate(ast);
    }

    private async execCreate(stmt: CreateStmt) {
        if (this.tables.has(stmt.table)) throw new Error(`Table ${stmt.table} already exists`);

        await this.catalog.addTable(stmt.table, stmt.columns);
        const table = new Table(stmt.table, stmt.columns);
        await table.init();
        this.tables.set(stmt.table, table);
        return { message: "Table created" };
    }

    private async execInsert(stmt: InsertStmt) {
        const table = this.tables.get(stmt.table);
        if (!table) throw new Error(`Table ${stmt.table} not found`);

        if (stmt.values.length !== table.columns.length) {
            throw new Error(`Column count mismatch`);
        }

        const row: Row = {};
        for (let i = 0; i < table.columns.length; i++) {
            const col = table.columns[i];
            if (col) row[col.name] = stmt.values[i];
        }

        await table.insert(row);
        return { message: "Inserted 1 row" };
    }

    private async execSelect(stmt: SelectStmt) {
        const table = this.tables.get(stmt.table);
        if (!table) throw new Error(`Table ${stmt.table} not found`);

        let rows: Row[] = [];

        // OPTIMIZATION: Check for Primary Key Lookup
        // Criteria: WHERE clause is "pk = value"
        let isPkLookup = false;
        if (stmt.where && stmt.where.type === 'BINARY' && stmt.where.op === '=' && table.pkColumn) {
            const left = stmt.where.left;
            const right = stmt.where.right;
            let key: any;

            if (left.type === 'IDENTIFIER' && left.name === table.pkColumn?.name && right.type === 'LITERAL') {
                key = right.value;
            } else if (right.type === 'IDENTIFIER' && right.name === table.pkColumn?.name && left.type === 'LITERAL') {
                key = left.value;
            }

            if (key !== undefined) {
                const row = await table.getRowByPrimaryKey(key);
                if (row) rows = [row];
                isPkLookup = true;
            }
        }

        if (!isPkLookup) {
            rows = await table.selectAll();
            // Apply WHERE (if not already handled by lookup)
            if (stmt.where) {
                rows = rows.filter(row => this.evaluate(stmt.where!, row));
            }
        }

        // Apply Projection
        if (stmt.columns[0] !== '*') {
            rows = rows.map(r => {
                const newRow: Row = {};
                for (const col of stmt.columns) {
                    if (r[col] !== undefined) newRow[col] = r[col];
                }
                return newRow;
            });
        }

        return rows;
    }

    private async execDelete(stmt: DeleteStmt) {
        const table = this.tables.get(stmt.table);
        if (!table) throw new Error(`Table ${stmt.table} not found`);

        const allRows = await table.selectAll();
        let keepRows = allRows;

        if (stmt.where) {
            keepRows = allRows.filter(row => !this.evaluate(stmt.where!, row));
        } else {
            keepRows = []; // Delete All
        }

        const deletedCount = allRows.length - keepRows.length;
        if (deletedCount > 0) {
            await table.overwrite(keepRows);
        }
        return { message: `Deleted ${deletedCount} rows` };
    }

    private async execUpdate(stmt: UpdateStmt) {
        const table = this.tables.get(stmt.table);
        if (!table) throw new Error(`Table ${stmt.table} not found`);

        const rows = await table.selectAll();
        let updatedCount = 0;

        for (const row of rows) {
            if (!stmt.where || this.evaluate(stmt.where, row)) {
                // Update fields
                for (const assign of stmt.assignments) {
                    row[assign.column] = assign.value;
                }
                updatedCount++;
            }
        }

        if (updatedCount > 0) {
            await table.overwrite(rows);
        }
        return { message: `Updated ${updatedCount} rows` };
    }

    private evaluate(expr: Expr, row: Row): any {
        if (expr.type === 'LITERAL') return expr.value;
        if (expr.type === 'IDENTIFIER') return row[expr.name];
        if (expr.type === 'BINARY') {
            const left = this.evaluate(expr.left, row);
            const right = this.evaluate(expr.right, row);
            switch (expr.op) {
                case '=': return left == right;
                case '>': return left > right;
                case '<': return left < right;
                case 'AND': return left && right;
                case 'OR': return left || right;
            }
        }
        return false;
    }
}
