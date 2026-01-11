import type { BunFile } from "bun";
import { BinaryUtils } from "./BinaryUtils";
import { ColumnType, HEADER_SIZE, MAGIC_BYTES, VERSION, DATA_DIR } from "./Constants";
import { Index } from "./Index";
import { join } from "path";

export interface Column {
    name: string;
    type: ColumnType;
    isPrimary?: boolean;
}

export type Row = Record<string, any>;

export class Table {
    private file: BunFile;
    private path: string;
    private index?: Index;
    public pkColumn?: Column;
    private currentOffset: number = 0;

    constructor(public name: string, public columns: Column[]) {
        this.path = join(DATA_DIR, `${name}.db`);
        this.file = Bun.file(this.path);
        this.pkColumn = columns.find(c => c.isPrimary);
        if (this.pkColumn) {
            this.index = new Index(name, this.pkColumn.type);
        }
    }

    // Initialize file with Header if it doesn't exist
    async init() {
        const fs = await import('node:fs/promises');
        try {
            await fs.access(this.path);
            const buffer = await fs.readFile(this.path);
            this.currentOffset = buffer.byteLength;
        } catch {
            const buffer = new ArrayBuffer(HEADER_SIZE);
            const view = new DataView(buffer);
            // Write Magic "PESA"
            new Uint8Array(buffer).set(new TextEncoder().encode(MAGIC_BYTES), 0);
            // Write Version
            view.setUint8(4, VERSION);
            await fs.writeFile(this.path, new Uint8Array(buffer));
            this.currentOffset = HEADER_SIZE;
        }

        if (this.index) {
            await this.index.init();
        }
    }

    async insert(row: Row): Promise<void> {
        // 1. Check Primary Key Constraint
        if (this.index && this.pkColumn) {
            const pkVal = row[this.pkColumn.name];
            if (pkVal === undefined || pkVal === null) throw new Error("Primary Key cannot be null");
            if (this.index.has(pkVal)) {
                throw new Error(`Duplicate Entry for Primary Key: ${pkVal}`);
            }
        }

        // Calculate needed size
        let size = 0;
        for (const col of this.columns) {
            size += BinaryUtils.getSize(col.type, row[col.name]);
        }

        const buffer = new ArrayBuffer(size);
        const view = new DataView(buffer);
        let offset = 0;

        for (const col of this.columns) {
            const val = row[col.name];
            switch (col.type) {
                case ColumnType.INT:
                    view.setInt32(offset, val as number);
                    offset += 4;
                    break;
                case ColumnType.FLOAT:
                    view.setFloat64(offset, val as number);
                    offset += 8;
                    break;
                case ColumnType.BOOLEAN:
                    view.setUint8(offset, val ? 1 : 0);
                    offset += 1;
                    break;
                case ColumnType.STRING:
                    offset = BinaryUtils.writeString(view, offset, val as string);
                    break;
            }
        }

        const fs = await import('node:fs/promises');
        await fs.appendFile(this.path, new Uint8Array(buffer));

        // 2. Update Index with the offset where we started writing
        if (this.index && this.pkColumn) {
            await this.index.add(row[this.pkColumn.name], this.currentOffset);
        }

        this.currentOffset += size;
    }

    async selectAll(): Promise<Row[]> {
        const fs = await import('node:fs/promises');
        // Use readFile which returns Buffer (Uint8Array in Node)
        const nodeBuffer = await fs.readFile(this.path);
        // Ensure we work with the underlying ArrayBuffer
        const buffer = nodeBuffer.buffer.slice(nodeBuffer.byteOffset, nodeBuffer.byteOffset + nodeBuffer.byteLength);

        const view = new DataView(buffer);
        const rows: Row[] = [];
        let offset = HEADER_SIZE; // Skip Header

        while (offset < buffer.byteLength) {
            const row: Row = {};
            for (const col of this.columns) {
                switch (col.type) {
                    case ColumnType.INT:
                        row[col.name] = view.getInt32(offset);
                        offset += 4;
                        break;
                    case ColumnType.FLOAT:
                        row[col.name] = view.getFloat64(offset);
                        offset += 8;
                        break;
                    case ColumnType.BOOLEAN:
                        row[col.name] = view.getUint8(offset) === 1;
                        offset += 1;
                        break;
                    case ColumnType.STRING:
                        const res = BinaryUtils.readString(view, offset);
                        row[col.name] = res.value;
                        offset = res.nextOffset;
                        break;
                }
            }
            rows.push(row);
        }
        return rows;
    }

    async getRowByPrimaryKey(key: string | number): Promise<Row | undefined> {
        if (!this.index) return undefined;
        const offset = this.index.get(key);
        if (offset === undefined) return undefined;
        return this.readRowAt(offset);
    }

    async readRowAt(startOffset: number): Promise<Row> {
        const fs = await import('node:fs/promises');
        // We need to read enough bytes to cover the row. 
        // Since we don't know the exact size upfront (variable strings), we can either:
        // 1. Read a chunk and grow if needed (complex)
        // 2. Read field by field (lots of syscalls)
        // 3. For this PoC, we can read a "safe" large chunk or just use a file handle to read exactly what we need.

        // Let's use a file handle to read sequentially from the offset
        const handle = await fs.open(this.path, 'r');
        try {
            const row: Row = {};
            let currentFilePos = startOffset;

            for (const col of this.columns) {
                // Buffer for fixed size reads
                const buffer = new Uint8Array(8);

                switch (col.type) {
                    case ColumnType.INT: {
                        await handle.read(buffer, 0, 4, currentFilePos);
                        const view = new DataView(buffer.buffer);
                        row[col.name] = view.getInt32(0);
                        currentFilePos += 4;
                        break;
                    }
                    case ColumnType.FLOAT: {
                        await handle.read(buffer, 0, 8, currentFilePos);
                        const view = new DataView(buffer.buffer);
                        row[col.name] = view.getFloat64(0);
                        currentFilePos += 8;
                        break;
                    }
                    case ColumnType.BOOLEAN: {
                        await handle.read(buffer, 0, 1, currentFilePos);
                        const view = new DataView(buffer.buffer);
                        row[col.name] = view.getUint8(0) === 1;
                        currentFilePos += 1;
                        break;
                    }
                    case ColumnType.STRING: {
                        // Read length (2 bytes)
                        await handle.read(buffer, 0, 2, currentFilePos);
                        const viewLength = new DataView(buffer.buffer);
                        const length = viewLength.getUint16(0);
                        currentFilePos += 2;

                        // Read String Content
                        const strBuf = new Uint8Array(length);
                        await handle.read(strBuf, 0, length, currentFilePos);
                        row[col.name] = new TextDecoder().decode(strBuf);
                        currentFilePos += length;
                        break;
                    }
                }
            }
            return row;
        } finally {
            await handle.close();
        }
    }

    // Heavy operation: Rewrite entire file!
    async overwrite(rows: Row[]) {
        const fs = await import('node:fs/promises');

        // Truncate DB
        const buffer = new ArrayBuffer(HEADER_SIZE);
        const view = new DataView(buffer);
        new Uint8Array(buffer).set(new TextEncoder().encode(MAGIC_BYTES), 0);
        view.setUint8(4, VERSION);
        await fs.writeFile(this.path, new Uint8Array(buffer));
        this.currentOffset = HEADER_SIZE;

        // Clear Index
        if (this.index) {
            await this.index.clear();
        }

        // Re-insert all rows (Index will be rebuilt)
        for (const row of rows) {
            await this.insert(row);
        }
    }
}
