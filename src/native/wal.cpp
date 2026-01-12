#ifdef _WIN32
    #include <windows.h>
    #define WAL_EXPORT __declspec(dllexport)
    typedef HANDLE FileHandle;
    #define INVALID_FILE_HANDLE INVALID_HANDLE_VALUE
#else
    #include <fcntl.h>
    #include <unistd.h>
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <errno.h>
    #define WAL_EXPORT __attribute__((visibility("default")))
    typedef int FileHandle;
    #define INVALID_FILE_HANDLE -1
#endif

#include <vector>
#include <string>
#include <cstring>
#include <stdio.h>
#include <stdint.h>

// Internal C++ Class to manage WAL State and Buffering
class WalEngine {
public:
    FileHandle hFile;
    std::vector<char> buffer;
    size_t pending_offset;
    const size_t BUFFER_SIZE = 65536; // 64KB

    WalEngine(const char* path) : hFile(INVALID_FILE_HANDLE), pending_offset(0) {
#ifdef _WIN32
        hFile = CreateFileA(
            path,
            GENERIC_WRITE,
            FILE_SHARE_READ,
            NULL,
            OPEN_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            NULL
        );
#else
        // POSIX Open: Read/Write, Create if missing, Append mode
        // 0644 = rw-r--r--
        hFile = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
#endif
        buffer.resize(BUFFER_SIZE);
    }

    ~WalEngine() {
        close_file();
    }

    bool is_valid() {
        return hFile != INVALID_FILE_HANDLE;
    }

    void close_file() {
        if (hFile != INVALID_FILE_HANDLE) {
            flush(); // Ensure data is saved
#ifdef _WIN32
            CloseHandle(hFile);
#else
            close(hFile);
#endif
            hFile = INVALID_FILE_HANDLE;
        }
    }

    // Flush internal buffer to disk
    bool flush() {
        if (pending_offset == 0) return true;
        
        bool success = false;
#ifdef _WIN32
        DWORD bytesWritten = 0;
        success = WriteFile(hFile, buffer.data(), (DWORD)pending_offset, &bytesWritten, NULL) != 0;
        if (success) {
            FlushFileBuffers(hFile);
        }
#else
        ssize_t written = write(hFile, buffer.data(), pending_offset);
        success = (written == (ssize_t)pending_offset);
        if (success) {
            fsync(hFile);
        }
#endif
        if (success) {
            pending_offset = 0;
        }
        return success;
    }

    // Append a WAL Frame
    // Frame Format: [LSN:4][Txn:4][Op:1][TblLen:2][Tbl][DataLen:4][Data][Cksum:4]
    int32_t append(double lsn, int32_t txnId, int32_t opType, const char* tableName, const char* data, bool sync) {
        if (hFile == INVALID_FILE_HANDLE) return -1;

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
            
            // If single item is huge, resize
            if (total_size > buffer.size()) {
                buffer.resize(total_size * 2);
            }
        }

        char* ptr = buffer.data() + pending_offset;
        
        // Write LSN (4 bytes)
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
        if (hFile == INVALID_FILE_HANDLE) return -1;
        
        // If we have pending data in buffer, flush it first
        if (pending_offset > 0) {
            if (!flush()) return -2;
        }

        // Direct write of the batch
#ifdef _WIN32
        DWORD bytesWritten = 0;
        BOOL success = WriteFile(hFile, data, length, &bytesWritten, NULL);
        return success ? (int32_t)bytesWritten : -3;
#else
        ssize_t written = write(hFile, data, length);
        return (int32_t)written;
#endif
    }
};

// --- C Exports ---

extern "C" WAL_EXPORT void* wal_open(const char* path) {
    WalEngine* engine = new WalEngine(path);
    if (!engine->is_valid()) {
        delete engine;
        return NULL;
    }
    return (void*)engine;
}

extern "C" WAL_EXPORT void wal_close(void* handle) {
    WalEngine* engine = (WalEngine*)handle;
    if (engine) {
        delete engine; // Destructor calls close()
    }
}

extern "C" WAL_EXPORT int32_t wal_append(void* handle, double lsn, int32_t txnId, int32_t opType, const char* tableName, const char* data, bool sync) {
    if (!handle) return -1;
    WalEngine* engine = (WalEngine*)handle;
    return engine->append(lsn, txnId, opType, tableName, data, sync);
}

extern "C" WAL_EXPORT int32_t wal_append_batch(void* handle, const char* data, int32_t length) {
    if (!handle) return -1;
    WalEngine* engine = (WalEngine*)handle;
    return engine->append_batch(data, length);
}

extern "C" WAL_EXPORT int32_t wal_flush(void* handle) {
    WalEngine* engine = (WalEngine*)handle;
    if (!engine) return -1;
    return engine->flush() ? 1 : 0;
}
