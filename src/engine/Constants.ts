export const MAGIC_BYTES = "PESA"; // 4 bytes
export const VERSION = 1;      // 1 byte
export const PAGE_SIZE = 4096; // Standard Page Size

export enum ColumnType {
    INT = 1,
    STRING = 2,
    BOOLEAN = 3,
    FLOAT = 4
}

export const HEADER_SIZE = 5; // MAGIC(4) + VERSION(1)
export const DATA_DIR = "data";
