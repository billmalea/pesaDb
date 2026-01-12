import { dlopen, FFIType, CString } from "bun:ffi";
import { join } from "path";
import { DATA_DIR } from "./Constants";

const path = join(process.cwd(), "src/native/wal_debug.dll");

// Load DLL
const lib = dlopen(path, {
    wal_open: {
        args: [FFIType.cstring],
        returns: FFIType.ptr
    },
    wal_append: {
        // handle, lsn(double), txnId(i32), op(i32), tbl(cstring), data(cstring), sync(bool)
        args: [FFIType.ptr, FFIType.f64, FFIType.i32, FFIType.i32, FFIType.cstring, FFIType.cstring, FFIType.bool],
        returns: FFIType.i32
    },
    wal_flush: {
        args: [FFIType.ptr],
        returns: FFIType.i32
    },
    wal_append_batch: {
        args: [FFIType.ptr, FFIType.ptr, FFIType.i32],
        returns: FFIType.i32
    },
    wal_close: {
        args: [FFIType.ptr],
        returns: FFIType.void
    }
});

export enum WalOpType {
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3,
    CHECKPOINT = 99
}

export class NativeWalManager {
    private handle: any = null;
    private dbPath: string;

    // Mimic the JS WAL LSN for now, though ideally C++ should track it
    private currentLsn = 0;

    constructor(dbName: string) {
        // Ensure absolute path
        this.dbPath = join(process.cwd(), DATA_DIR, `${dbName}.wal`);
    }

    private static handles: Map<string, any> = new Map();

    async init() {
        // Reuse handle if already open (Singleton per path)
        if (NativeWalManager.handles.has(this.dbPath)) {
            this.handle = NativeWalManager.handles.get(this.dbPath);
            return;
        }

        const fs = await import('node:fs/promises');
        try { await fs.mkdir(DATA_DIR, { recursive: true }); } catch { }

        const cPath = Buffer.from(this.dbPath + "\0");
        this.handle = lib.symbols.wal_open(cPath);
        if (!this.handle) {
            // Try to provide more context safely
            throw new Error(`Failed to open WAL at ${this.dbPath}`);
        }

        NativeWalManager.handles.set(this.dbPath, this.handle);
    }

    isReady(): boolean {
        return this.handle !== null;
    }


    // Direct binary append without JSON overhead if we want max speed
    // But for compatibility with the rest of the engine, we still serialize here for now.
    // The WIN here is the I/O, not necessarily the serialization yet.
    append(lsn: number, txnId: number, opType: WalOpType, tableName: string, data: string, sync: boolean) {
        if (!this.handle) throw new Error("WAL not initialized");

        return lib.symbols.wal_append(
            this.handle,
            lsn,
            txnId,
            opType,
            Buffer.from(tableName + "\0"),
            Buffer.from(data + "\0"),
            sync
        );
    }

    appendBatch(data: Buffer, length: number) {
        if (!this.handle) return -1;
        // Sync call
        return lib.symbols.wal_append_batch(this.handle, data, length);
    }


    async flush() {
        if (this.handle) lib.symbols.wal_flush(this.handle);
    }

    async close() {
        if (this.handle) {
            lib.symbols.wal_close(this.handle);
            NativeWalManager.handles.delete(this.dbPath);
            this.handle = null;
        }
    }
}
