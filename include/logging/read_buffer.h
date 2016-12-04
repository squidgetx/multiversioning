/**
 * read_buffer.h: Buffer abstractions for reading.
 *
 * Author: David Hatch
 */

#ifndef _LOGGING_READ_BUFFER_H
#define _LOGGING_READ_BUFFER_H

#include <cstddef>
#include <cstring>
#include <type_traits>

#include "no_copy.h"

class IReadBuffer {
    DISALLOW_COPY(IReadBuffer);
public:
    IReadBuffer() = default;
    virtual ~IReadBuffer() = default;

    /**
     * Read out a primitive type.
     *
     * Return true on success, false otherwise.
     */
    template <typename T,
              typename = std::enable_if<std::is_arithmetic<T>::value>>
    bool read(T *out) {
        unsigned char outArr[sizeof(T)];
        auto nRead = readBytes(outArr, sizeof(T));
        if (nRead < sizeof(T))
            return false;

        memcpy(out, outArr, sizeof(T));
        return true;
    }

    /**
     * Read 'nBytes' from the buffer.
     *
     * Returns: the number of bytes actually read, or 0 if none remain.
     */
    std::size_t read(unsigned char *out, std::size_t nBytes) {
        return readBytes(out, nBytes);
    }
protected:
    /**
     * Implementors should override this to implement their reading
     * logic.
     *
     * Returns: The number of bytes read. 0 if the buffer is exhausted.
     */
    virtual std::size_t readBytes(unsigned char *out, std::size_t nBytes) = 0;
};

/**
 * A buffer backed by a file.
 */
class FileBuffer : public IReadBuffer {
public:
    /**
     * Initialize the file buffer with the file descriptor 'fd'.
     */
    FileBuffer(int fd);
    virtual ~FileBuffer();

protected:
    virtual std::size_t readBytes(unsigned char *out, std::size_t nBytes) override;

private:
    /**
     * Read more of the file into '_buff'.
     */
    void readFile();

    int _fd;
    const std::size_t PAGE_SIZE;

    unsigned char *_readPtr = nullptr;
    unsigned char *_buff = nullptr;
    std::size_t _buffLen = 0;
};

/**
 * A 'ReadViewBuffer' wraps an underlying buffer, and will read
 * 'len' bytes from that buffer.
 *
 * It can be used to handout portions of a buffer to other functions, while
 * preventing them from reading excess data.
 */
class ReadViewBuffer : public IReadBuffer {
public:
    ReadViewBuffer(IReadBuffer *buffer, std::size_t len);
    virtual ~ReadViewBuffer();

    bool exhausted() {
        return _nRead == _len;
    }
protected:
    virtual std::size_t readBytes(unsigned char *out, std::size_t nBytes) override;
private:
    std::size_t _len;
    std::size_t _nRead = 0;
    IReadBuffer *_underlyingBuffer;
};

#endif /* _LOGGING_READ_BUFFER_H */
