import { open, type FileHandle } from "node:fs/promises";
import { join } from "path";
import { DATA_DIR, HEADER_SIZE, MAGIC_BYTES, VERSION } from "./Constants";
import { BinaryUtils } from "./BinaryUtils";

export enum WalOpType {
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3,
    CHECKPOINT = 99
}

export interface WalEntry {
    lsn: number; // Log Sequence Number
    txnId: number;
    opType: WalOpType;
    tableName: string;
    data: any; // The row data or change payload
}

export class WalManager {
    private path: string;
    private fileHandle?: FileHandle;
    private currentLsn: number = 0;

    constructor(public dbName: string) {
        this.path = join(DATA_DIR, `${dbName}.wal`);
    }

    async init() {
        const fs = await import('node:fs/promises');
        try {
            this.fileHandle = await open(this.path, 'a+');
            const stats = await this.fileHandle.stat();
            if (stats.size === 0) {
                // Initialize if needed, or just start writing
            }
            // In a real DB, we'd read the last LSN here
            this.currentLsn = Date.now(); // Simple monotonic LSN for this PoC
        } catch (e) {
            console.error("Failed to init WAL", e);
            throw e;
        }
    }

    async append(txnId: number, opType: WalOpType, tableName: string, data: any, sync: boolean = true) {
        if (!this.fileHandle) throw new Error("WAL not initialized");

        // Format:
        // [LSN: 8 bytes] [TxnId: 4 bytes] [OpType: 1 byte] [TableLen: 2 bytes] [TableName: N bytes] [DataLen: 4 bytes] [Data: JSON string for PoC]
        // Note: For true binary efficiency we'd pack 'data' as binary too, but JSON is safer for the complex Row object structure in this stage.

        const tableBytes = new TextEncoder().encode(tableName);
        const dataStr = JSON.stringify(data);
        const dataBytes = new TextEncoder().encode(dataStr);

        const totalSize = 8 + 4 + 1 + 2 + tableBytes.length + 4 + dataBytes.length;
        const buffer = new ArrayBuffer(totalSize);
        const view = new DataView(buffer);
        let offset = 0;

        this.currentLsn++;

        // LSN (using Float64 for 53-bit integer safety in JS)
        view.setFloat64(offset, this.currentLsn); offset += 8;

        // TxnID
        view.setInt32(offset, txnId); offset += 4;

        // OpType
        view.setUint8(offset, opType); offset += 1;

        // Table Name
        view.setUint16(offset, tableBytes.length); offset += 2;
        new Uint8Array(buffer).set(tableBytes, offset); offset += tableBytes.length;

        // Data
        view.setUint32(offset, dataBytes.length); offset += 4;
        new Uint8Array(buffer).set(dataBytes, offset); offset += dataBytes.length;

        // WRITE
        await this.fileHandle.appendFile(new Uint8Array(buffer));

        // SYNC ONLY IF REQUESTED
        if (sync) {
            await this.fileHandle.sync();
        }
    }

    async flush() {
        if (this.fileHandle) {
            await this.fileHandle.sync();
        }
    }

    async readAll(): Promise<WalEntry[]> {
        if (!this.fileHandle) return [];
        const fs = await import('node:fs/promises');
        // Close handle to read safely or read from another handle
        await this.fileHandle.close();

        const buffer = await fs.readFile(this.path);
        const view = new DataView(buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength));
        let offset = 0;
        const entries: WalEntry[] = [];

        while (offset < view.byteLength) {
            if (offset + 19 > view.byteLength) break; // Header check

            const lsn = view.getFloat64(offset); offset += 8;
            const txnId = view.getInt32(offset); offset += 4;
            const opType = view.getUint8(offset); offset += 1;

            const tableLen = view.getUint16(offset); offset += 2;
            const tableName = new TextDecoder().decode(buffer.subarray(offset, offset + tableLen));
            offset += tableLen;

            const dataLen = view.getUint32(offset); offset += 4;
            const dataStr = new TextDecoder().decode(buffer.subarray(offset, offset + dataLen));
            offset += dataLen;

            entries.push({
                lsn,
                txnId,
                opType,
                tableName,
                data: JSON.parse(dataStr)
            });
        }

        // Re-open for writing
        this.fileHandle = await open(this.path, 'a+');
        return entries;
    }

    async clear() {
        if (this.fileHandle) {
            await this.fileHandle.close();
        }
        const fs = await import('node:fs/promises');
        await fs.writeFile(this.path, new Uint8Array(0));
        this.fileHandle = await open(this.path, 'a+');
        this.currentLsn = 0;
    }

    async close() {
        if (this.fileHandle) await this.fileHandle.close();
    }
}
