/**
 * read_buffer.h: Buffer abstractions for reading.
 *
 * Author: David Hatch
 */

#include <cstddef>
#include <type_traits>

#include "no_copy.h"

#ifndef _LOGGING_READ_BUFFER_H
#define _LOGGING_READ_BUFFER_H

class IReadBuffer {
    DISALLOW_COPY(IReadBuffer);
public:
    IReadBuffer() = default;
    virtual ~IReadBuffer() = default;

    /**
     * Read out a primitive type.
     */
    template <typename T,
              typename = std::enable_if<std::is_arithmetic<T>::value>>
    T read() {
        unsigned char out[sizeof(T)];
        readBytes(&out, sizeof(T));

        return reinterpret_cast<T>(out);
    }

    /**
     * Read 'nBytes' from the buffer.
     */
    void read(unsigned char *out, std::size_t nBytes) {
        readBytes(out, nBytes);
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

#endif /* _LOGGING_READ_BUFFER_H */
