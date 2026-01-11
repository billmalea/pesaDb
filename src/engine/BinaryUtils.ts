import { ColumnType } from "./Constants";

export class BinaryUtils {
    constructor() { }

    // Write string with 2-byte length prefix
    static writeString(view: DataView, offset: number, str: string): number {
        const encoder = new TextEncoder();
        const encoded = encoder.encode(str);
        view.setUint16(offset, encoded.length); // 2 bytes for length
        // We need to copy byte by byte or use a TypedArray set if we had access to the buffer directly
        // DataView doesn't have a 'setBytes' method easily, so we use the buffer.
        const buffer = new Uint8Array(view.buffer);
        buffer.set(encoded, offset + 2);
        return offset + 2 + encoded.length;
    }

    static readString(view: DataView, offset: number): { value: string, nextOffset: number } {
        const length = view.getUint16(offset);
        const buffer = new Uint8Array(view.buffer, offset + 2, length);
        const decoder = new TextDecoder();
        const value = decoder.decode(buffer);
        return { value, nextOffset: offset + 2 + length };
    }

    static getSize(type: ColumnType, value: any): number {
        switch (type) {
            case ColumnType.INT: return 4; // Int32
            case ColumnType.FLOAT: return 8; // Float64
            case ColumnType.BOOLEAN: return 1; // 1 byte
            case ColumnType.STRING: return 2 + new TextEncoder().encode(value as string).length;
            default: return 0;
        }
    }
}
