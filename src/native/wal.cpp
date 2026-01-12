#include <windows.h>
#include <vector>
#include <string>
#include <cstring>
#include <stdio.h>
#include <stdint.h>

// Internal C++ Class to manage WAL State and Buffering
class WalEngine {
public:
    HANDLE hFile;
    std::vector<char> buffer;
    size_t pending_offset;
    const size_t BUFFER_SIZE = 65536; // 64KB

    WalEngine(const char* path) : hFile(INVALID_HANDLE_VALUE), pending_offset(0) {
        hFile = CreateFileA(
            path,
            GENERIC_WRITE,
            FILE_SHARE_READ,
            NULL,
            OPEN_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            NULL
        );
        buffer.resize(BUFFER_SIZE);
    }

    ~WalEngine() {
        close();
    }

    bool is_valid() {
        return hFile != INVALID_HANDLE_VALUE;
    }

    void close() {
        if (hFile != INVALID_HANDLE_VALUE) {
            flush(); // Ensure data is saved
            CloseHandle(hFile);
            hFile = INVALID_HANDLE_VALUE;
        }
    }

    // Flush internal buffer to disk
    bool flush() {
        if (pending_offset == 0) return true;
        
        DWORD bytesWritten = 0;
        BOOL success = WriteFile(hFile, buffer.data(), pending_offset, &bytesWritten, NULL);
        if (success) {
            // Hard Sync to Disk
            FlushFileBuffers(hFile);
            pending_offset = 0;
        }
        return success ? true : false;
    }

    // Append a WAL Frame
    // Frame Format: [LSN:4][Txn:4][Op:1][TblLen:2][Tbl][DataLen:4][Data][Cksum:4]
    int32_t append(double lsn, int32_t txnId, int32_t opType, const char* tableName, const char* data, bool sync) {
        if (hFile == INVALID_HANDLE_VALUE) return -1;

        size_t id_size = sizeof(uint32_t) * 2 + sizeof(uint8_t); // LSN, Txn, Op
        size_t tbl_len = strlen(tableName);
        size_t data_len = strlen(data);
        
        size_t total_size = id_size + 
                            sizeof(uint16_t) + tbl_len + 
                            sizeof(uint32_t) + data_len + 
                            sizeof(uint32_t); // Checksum

        // If too big for remaining buffer, flush first
        if (pending_offset + total_size > buffer.size()) {
            if (!flush()) return -2;
            
            // If single item is huge, expand buffer or direct write?
            // For now, let's just resize if needed
            if (total_size > buffer.size()) {
                buffer.resize(total_size * 2);
            }
        }

        char* ptr = buffer.data() + pending_offset;
        
        // Write LSN (4 bytes for now to match benchmark expectations, or cast double?)
        // Benchmark JS was writing DoubleLE (8 bytes) initially but switched to 4 byte int.
        // Let's stick to 32-bit int for LSN in this C++ implementation for speed/simplicity as per previous simple format
        uint32_t lsn32 = (uint32_t)lsn;
        
        memcpy(ptr, &lsn32, 4); ptr += 4;
        memcpy(ptr, &txnId, 4); ptr += 4;
        uint8_t op = (uint8_t)opType;
        memcpy(ptr, &op, 1); ptr += 1;

        uint16_t t_len = (uint16_t)tbl_len;
        memcpy(ptr, &t_len, 2); ptr += 2;
        memcpy(ptr, tableName, t_len); ptr += t_len;

        uint32_t d_len = (uint32_t)data_len;
        memcpy(ptr, &d_len, 4); ptr += 4;
        memcpy(ptr, data, d_len); ptr += d_len;

        // Checksum (Dummy 0)
        uint32_t cksum = 0;
        memcpy(ptr, &cksum, 4); ptr += 4;

        pending_offset += total_size;

        if (sync) {
            if (!flush()) return -3;
        }

        return 1;
    }

    int32_t append_batch(const char* data, int32_t length) {
        if (hFile == INVALID_HANDLE_VALUE) return -1;
        
        // If we have pending data in buffer, flush it first to maintain order
        if (pending_offset > 0) {
            if (!flush()) return -2;
        }

        // Direct write of the batch
        DWORD bytesWritten = 0;
        BOOL success = WriteFile(hFile, data, length, &bytesWritten, NULL);
        if (success) {
            // We assume batch is synced at end of batch in JS logic if needed, 
            // but usually batch append doesn't imply sync unless asked. 
            // But here we are just writing bytes. JS manages sync via wal_flush call.
        }
        return success ? (int32_t)bytesWritten : -3;
    }
};

// --- C Exports ---

extern "C" __declspec(dllexport) void* wal_open(const char* path) {
    WalEngine* engine = new WalEngine(path);
    if (!engine->is_valid()) {
        delete engine;
        return NULL;
    }
    return (void*)engine;
}

extern "C" __declspec(dllexport) void wal_close(void* handle) {
    WalEngine* engine = (WalEngine*)handle;
    if (engine) {
        delete engine; // Destructor calls close()
    }
}

extern "C" __declspec(dllexport) int32_t wal_append(void* handle, double lsn, int32_t txnId, int32_t opType, const char* tableName, const char* data, bool sync) {
    if (!handle) return -1;
    WalEngine* engine = (WalEngine*)handle;
    return engine->append(lsn, txnId, opType, tableName, data, sync);
}

extern "C" __declspec(dllexport) int32_t wal_append_batch(void* handle, const char* data, int32_t length) {
    if (!handle) return -1;
    WalEngine* engine = (WalEngine*)handle;
    return engine->append_batch(data, length);
}

extern "C" __declspec(dllexport) int32_t wal_flush(void* handle) {
    WalEngine* engine = (WalEngine*)handle;
    if (!engine) return -1;
    return engine->flush() ? 1 : 0;
}
