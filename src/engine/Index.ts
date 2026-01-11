import { BinaryUtils } from "./BinaryUtils";
import { ColumnType, DATA_DIR } from "./Constants";
import { join } from "path";

export class Index {
    private path: string;
    private map: Map<string | number, number> = new Map();

    constructor(public name: string, public keyType: ColumnType) {
        this.path = join(DATA_DIR, `${name}.idx`);
    }

    async init() {
        const fs = await import('node:fs/promises');
        try {
            await fs.access(this.path);
            const nodeBuffer = await fs.readFile(this.path);
            const buffer = nodeBuffer.buffer.slice(nodeBuffer.byteOffset, nodeBuffer.byteOffset + nodeBuffer.byteLength);
            const view = new DataView(buffer);
            let offset = 0;
            while (offset < buffer.byteLength) {
                let key: string | number;
                // Read Key
                if (this.keyType === ColumnType.INT) {
                    key = view.getInt32(offset);
                    offset += 4;
                } else if (this.keyType === ColumnType.STRING) {
                    const res = BinaryUtils.readString(view, offset);
                    key = res.value;
                    offset = res.nextOffset;
                } else {
                    throw new Error("Unsupported Index Type");
                }
                // Read File Offset
                const fileOffset = view.getUint32(offset);
                offset += 4;

                this.map.set(key, fileOffset);
            }
        } catch {
            // Create empty file
            await fs.writeFile(this.path, new Uint8Array(0));
        }
    }

    has(key: string | number): boolean {
        return this.map.has(key);
    }

    get(key: string | number): number | undefined {
        return this.map.get(key);
    }

    async add(key: string | number, fileOffset: number) {
        if (this.map.has(key)) {
            throw new Error(`Unique Constraint Violation: Key ${key} already exists.`);
        }
        this.map.set(key, fileOffset);

        // Append to file
        let keySize = 0;
        if (this.keyType === ColumnType.INT) keySize = 4;
        else if (this.keyType === ColumnType.STRING) keySize = BinaryUtils.getSize(ColumnType.STRING, key);

        const buffer = new ArrayBuffer(keySize + 4);
        const view = new DataView(buffer);
        let offset = 0;

        if (this.keyType === ColumnType.INT) {
            view.setInt32(0, key as number);
            offset += 4;
        } else {
            offset = BinaryUtils.writeString(view, 0, key as string);
        }

        view.setUint32(offset, fileOffset);

        const fs = await import('node:fs/promises');
        await fs.appendFile(this.path, new Uint8Array(buffer));
    }

    async clear() {
        this.map.clear();
        const fs = await import('node:fs/promises');
        await fs.writeFile(this.path, new Uint8Array(0));
    }
}
