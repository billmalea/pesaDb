import { ColumnType, DATA_DIR } from "./Constants";
import { type WalManager, WalOpType } from "./WAL";
import { join } from "path";

export interface Column {
    name: string;
    type: ColumnType;
    isPrimary?: boolean;
}

export type Row = Record<string, any>;

export class Table {
    private rows: Row[] = [];
    private pkMap: Map<string | number, Row> = new Map();
    public pkColumn?: Column;
    private path: string;

    constructor(public name: string, public columns: Column[], private wal?: WalManager) {
        this.path = join(DATA_DIR, `${name}.db`);
        this.pkColumn = columns.find(c => c.isPrimary);
    }

    // Initialize: Load data from disk (snapshot) + Replay WAL
    async init() {
        this.rows = [];
        this.pkMap.clear();

        // RECOVERY: Replay WAL
        if (this.wal) {
            const entries = await this.wal.readAll();
            for (const entry of entries) {
                if (entry.tableName !== this.name) continue;

                if (entry.opType === WalOpType.INSERT) {
                    const row = entry.data;
                    this.rows.push(row);
                    if (this.pkColumn) {
                        this.pkMap.set(row[this.pkColumn.name], row);
                    }
                }
                // TODO: Handle UPDATE / DELETE for full recovery
            }
        }
    }

    async insert(row: Row, sync: boolean = true): Promise<void> {
        // 1. Check Primary Key Constraint (In-Memory)
        if (this.pkColumn) {
            const pkVal = row[this.pkColumn.name];
            if (pkVal === undefined || pkVal === null) throw new Error("Primary Key cannot be null");
            if (this.pkMap.has(pkVal)) {
                throw new Error(`Duplicate Entry for Primary Key: ${pkVal}`);
            }
        }

        // 2. WAL Write (ACID: Durability) - The ONLY disk IO
        if (this.wal) {
            await this.wal.append(0, WalOpType.INSERT, this.name, row, sync);
        }

        // 3. Update Memory
        this.rows.push(row);
        if (this.pkColumn) {
            this.pkMap.set(row[this.pkColumn.name], row);
        }
    }

    // No recoverInsert needed anymore (it was writing to disk)
    async recoverInsert(row: Row): Promise<void> {
        // No-op or we could use this if we were truly recovering from WAL replay
        this.rows.push(row);
        if (this.pkColumn) {
            this.pkMap.set(row[this.pkColumn.name], row);
        }
    }

    async selectAll(): Promise<Row[]> {
        return this.rows;
    }

    async getRowByPrimaryKey(key: string | number): Promise<Row | undefined> {
        return this.pkMap.get(key);
    }

    async readRowAt(startOffset: number): Promise<Row> {
        throw new Error("Offset reading deprecated in In-Memory Mode");
    }

    async overwrite(rows: Row[]) {
        // WAL LOG? "Delete All"? 
        // For now, simple update memory
        this.rows = [...rows];
        this.pkMap.clear();
        if (this.pkColumn) {
            for (const r of this.rows) this.pkMap.set(r[this.pkColumn.name], r);
        }
        // In real DB we would WAL log a "TRUNCATE" or "REPLACE_ALL"
    }

    async deleteFiles() {
        const fs = await import('node:fs/promises');
        try { await fs.unlink(this.path); } catch { }
        this.rows = [];
        this.pkMap.clear();
    }
}
