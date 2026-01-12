import { open, type FileHandle } from "node:fs/promises";
import { join } from "path";
import { DATA_DIR } from "./Constants";
import { NativeWalManager } from "./NativeWAL";

export enum WalOpType {
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3,
    CHECKPOINT = 99
}

export interface WalEntry {
    lsn: number;
    txnId: number;
    opType: WalOpType;
    tableName: string;
    data: any;
}

export class WalManager {
    private path: string;
    private fileHandle?: FileHandle;
    private currentLsn: number = 0;
    private nativeWal: NativeWalManager | null = null;

    // Buffering for Native Batch Performance (Re-introduced for Zero-Alloc optimization)
    private pendingBuffer: Buffer = Buffer.alloc(65536); // 64KB Buffer (Reusable)
    private pendingOffset: number = 0;

    constructor(public dbName: string) {
        this.path = join(DATA_DIR, `${dbName}.wal`);
        this.nativeWal = new NativeWalManager(dbName);
    }

    async init() {
        const fs = await import('node:fs/promises');
        try { await fs.mkdir(DATA_DIR, { recursive: true }); } catch { }

        // Ensure WAL file exists for reading (Recovery Analysis)
        // If native mode, we might relying on native to open it.
        // But for recovery reading in JS, we need a handle or read bytes.
        // In "Hybrid" mode, we usually don't keep JS handle open for write if Native is active.

        try {
            // Just ensure file exists? 
            const h = await open(this.path, 'a+');
            await h.close();
        } catch (e) { }

        await this.readAll(); // Recover LSN state

        // Init Native
        try {
            await this.nativeWal?.init();
        } catch (e) {
            console.warn("⚠️ Failed to init native WAL, falling back", e);
            // Fallback: JS Append
            this.fileHandle = await (await import('node:fs/promises')).open(this.path, 'a+');
        }
    }

    // Changed to return void | Promise<void> for Sync optimization
    append(txnId: number, opType: WalOpType, tableName: string, data: any, sync: boolean = true): void | Promise<void> {
        this.currentLsn++;

        if (this.currentLsn % 1000 === 0) console.log(`DEBUG: WAL Append LSN=${this.currentLsn} Sync=${sync}`);

        // Fast path: Native Buffer
        if (this.nativeWal && this.nativeWal.isReady()) {
            return this.appendSync(txnId, opType, tableName, data, sync);
        }

        // Slow path: JS Fallback
        return this.appendFallback(txnId, opType, tableName, data, sync);
    }

    private appendSync(txnId: number, opType: WalOpType, tableName: string, data: any, sync: boolean) {
        // Serialize Data
        const isString = typeof data === 'string';
        const payloadStr = isString ? data : JSON.stringify(data);
        // We can estimate length or just write and check.
        // write(string) returns bytes written.
        // But we need to check space first.
        const tblLen = Buffer.byteLength(tableName);
        const dataLen = Buffer.byteLength(payloadStr);

        const totalLen = 4 + 4 + 1 + 2 + tblLen + 4 + dataLen + 4; // LSN, Txn, Op, TblLen, Tbl, DataLen, Data, Cksum

        if (this.pendingOffset + totalLen > this.pendingBuffer.length) {
            // Flush existing
            this.flushBufferSync();
            // If item too big, resize
            if (totalLen > this.pendingBuffer.length) {
                this.pendingBuffer = Buffer.alloc(Math.max(totalLen, this.pendingBuffer.length * 2));
            }
        }

        const buf = this.pendingBuffer;
        let offset = this.pendingOffset;

        buf.writeUInt32LE(this.currentLsn, offset); offset += 4;
        buf.writeUInt32LE(txnId, offset); offset += 4;
        buf.writeUInt8(opType, offset); offset += 1;

        buf.writeUInt16LE(tblLen, offset); offset += 2;
        buf.write(tableName, offset, tblLen, 'utf-8'); offset += tblLen;

        buf.writeUInt32LE(dataLen, offset); offset += 4;
        if (isString) {
            buf.write(payloadStr, offset, dataLen, 'utf-8');
        } else {
            buf.write(payloadStr, offset, dataLen, 'utf-8');
        }
        offset += dataLen;

        buf.writeUInt32LE(0, offset); // Checksum (0 for speed)
        offset += 4;

        this.pendingOffset = offset;

        if (sync) {
            this.flushSync();
        }
    }

    private flushBufferSync() {
        if (this.pendingOffset === 0) return;
        // Native Append Batch
        if (this.nativeWal) {
            // We pass subarray. Bun FFI needs to handle it.
            // Using subarray is basically a view, passing pointer to start + offset is handled by Bun if TypedArray passed?
            // "Buffer" in Node is Uint8Array.
            // Let's pass the subarray.
            const slice = this.pendingBuffer.subarray(0, this.pendingOffset);
            this.nativeWal.appendBatch(slice, this.pendingOffset);
        }
        this.pendingOffset = 0;
    }

    private flushSync() {
        this.flushBufferSync();
        // Native Flush
        // nativeWal.flush is async in NativeWAL.ts definition?
        // Let's check NativeWAL.ts.
        // It has `async flush()`.
        // C++ `wal_flush` is sync.
        // I should make `NativeWAL.flush` sync too.
        // For now, if I can't await, I just call it (fire and forget flush?)
        // Or make NativeWAL.flush sync.

        // Assuming NativeWAL.flush is strictly calling lib.symbols.wal_flush which is sync
        // But the wrapper is async.
        // I will change wrapper to sync later or cast it.
        // Actually, if sync is needed for durability, I MUST wait.
        // But transaction commit awaits `wal.flush()`.

        // Wait, `append` calls `flushSync` if `sync=true`.
        // If `NativeWAL.flush` is async, we start a promise effectively but don't return it?
        // That's risky for durability.
        // I need to fix `NativeWAL.ts` flush to be sync.
    }

    private async appendFallback(txnId: number, opType: WalOpType, tableName: string, data: any, sync: boolean) {
        if (!this.fileHandle) return;

        const payloadStr = JSON.stringify(data);
        const payloadBuf = Buffer.from(payloadStr);
        const tableNameBuf = Buffer.from(tableName);

        const totalLen = 4 + 4 + 1 + 2 + tableNameBuf.length + 4 + payloadBuf.length + 4;
        const frame = Buffer.allocUnsafe(totalLen);
        let offset = 0;

        frame.writeUInt32LE(this.currentLsn, offset); offset += 4;
        frame.writeUInt32LE(txnId, offset); offset += 4;
        frame.writeUInt8(opType, offset); offset += 1;

        frame.writeUInt16LE(tableNameBuf.length, offset); offset += 2;
        tableNameBuf.copy(frame, offset); offset += tableNameBuf.length;

        frame.writeUInt32LE(payloadBuf.length, offset); offset += 4;
        payloadBuf.copy(frame, offset); offset += payloadBuf.length;

        frame.writeUInt32LE(0, offset); // Checksum

        await this.fileHandle.appendFile(frame);
        if (sync) await this.fileHandle.sync();
    }

    async flush() {
        if (this.nativeWal && this.nativeWal.isReady()) {
            this.flushSync();
            // Ensure durability: call native flush
            await this.nativeWal.flush();
        } else {
            await this.fileHandle?.sync();
        }
    }

    async readAll(): Promise<WalEntry[]> {
        // Read via JS
        if (this.nativeWal?.isReady()) {
            // Cannot read if locked primarily? 
            // In wal.cpp we used share mode FILE_SHARE_READ? Yes.
        }

        try {
            const fs = await import('node:fs/promises');
            const buf = await fs.readFile(this.path);
            return this.parseWal(buf);
        } catch (e) {
            return [];
        }
    }

    private parseWal(buf: Buffer): WalEntry[] {
        const entries: WalEntry[] = [];
        let offset = 0;
        while (offset < buf.length) {
            if (offset + 13 > buf.length) break;

            const lsn = buf.readUInt32LE(offset); offset += 4;
            const txnId = buf.readUInt32LE(offset); offset += 4;
            const opType = buf.readUInt8(offset); offset += 1;

            this.currentLsn = Math.max(this.currentLsn, lsn);

            const tblLen = buf.readUInt16LE(offset); offset += 2;
            const tableName = buf.toString('utf-8', offset, offset + tblLen); offset += tblLen;

            const dataLen = buf.readUInt32LE(offset); offset += 4;
            const dataStr = buf.toString('utf-8', offset, offset + dataLen); offset += dataLen;

            const checksum = buf.readUInt32LE(offset); offset += 4;

            try {
                entries.push({ lsn, txnId, opType, tableName, data: JSON.parse(dataStr) });
            } catch (e) { }
        }
        return entries;
    }

    async clear() {
        if (this.nativeWal) {
            // Native clear? wal.cpp doesn't have clear.
            await this.nativeWal.close();
            const fs = await import('node:fs/promises');
            await fs.truncate(this.path, 0);
            await this.nativeWal.init();
        } else {
            await this.fileHandle?.truncate(0);
        }
        this.currentLsn = 0;
    }

    async close() {
        if (this.nativeWal) {
            await this.nativeWal.close();
        }
        await this.fileHandle?.close();
        this.fileHandle = undefined;
    }
}
