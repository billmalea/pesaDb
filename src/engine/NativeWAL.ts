import { dlopen, FFIType, CString } from "bun:ffi";
import { join } from "path";
import { DATA_DIR } from "./Constants";

const isWindows = process.platform === "win32";
const nativeDir = join(process.cwd(), "src/native");
// On Linux, we expect the .so to be built by our build script
const dllPath = join(nativeDir, isWindows ? "wal_debug.dll" : "native_wal.so");

// Conditional DLL Load
let lib: any = null;

try {
    lib = dlopen(dllPath, {
        wal_open: { args: [FFIType.cstring], returns: FFIType.ptr },
        wal_append: { args: [FFIType.ptr, FFIType.f64, FFIType.i32, FFIType.i32, FFIType.cstring, FFIType.cstring, FFIType.bool], returns: FFIType.i32 },
        wal_flush: { args: [FFIType.ptr], returns: FFIType.i32 },
        wal_append_batch: { args: [FFIType.ptr, FFIType.ptr, FFIType.i32], returns: FFIType.i32 },
        wal_close: { args: [FFIType.ptr], returns: FFIType.void }
    });
} catch (e) {
    console.error(`[NativeWAL] Failed to load Native Library at ${dllPath}`);
    console.error(`[NativeWAL] Error:`, e);

    // Hard crash if user wants NO fallback.
    // Or we throw to ensure they see it.
    throw new Error("Critical: Native Engine failed to load. Ensure build_native.ts ran successfully.");
}

export enum WalOpType {
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3,
    CHECKPOINT = 99
}

export class NativeWalManager {
    private handle: any = null;
    private dbPath: string;
    private currentLsn = 0;
    private static handles: Map<string, any> = new Map();

    constructor(dbName: string) {
        this.dbPath = join(process.cwd(), DATA_DIR, `${dbName}.wal`);
    }

    async init() {
        // Fallback or Singleton Check
        if (!lib) return;

        if (NativeWalManager.handles.has(this.dbPath)) {
            this.handle = NativeWalManager.handles.get(this.dbPath);
            return;
        }

        const fs = await import('node:fs/promises');
        try { await fs.mkdir(DATA_DIR, { recursive: true }); } catch { }

        const cPath = Buffer.from(this.dbPath + "\0");
        this.handle = lib.symbols.wal_open(cPath);

        if (!this.handle) {
            console.error(`[NativeWAL] Failed to open WAL at ${this.dbPath}`);
            return;
        }

        NativeWalManager.handles.set(this.dbPath, this.handle);
    }

    isReady(): boolean {
        return this.handle !== null;
    }

    append(lsn: number, txnId: number, opType: WalOpType, tableName: string, data: string, sync: boolean) {
        if (!this.handle) throw new Error("WAL not initialized");
        // Direct Native Call Only
        return lib.symbols.wal_append(
            this.handle, lsn, txnId, opType,
            Buffer.from(tableName + "\0"),
            Buffer.from(data + "\0"),
            sync
        );
    }

    appendBatch(data: Buffer, length: number) {
        if (!this.handle) return -1;
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
