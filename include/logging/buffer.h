/*
 * buffer.h: A memory buffer.
 */

#ifndef _LOGGING_BUFFER_H
#define _LOGGING_BUFFER_H

#include <cassert>
#include <cstddef>
#include <exception>
#include <limits>
#include <vector>
#include <type_traits>

#include "no_copy.h"

// TODO handle endianness on write to file?

/**
 * An abstract class representing an append-only buffer.
 */
class IBuffer {
    DISALLOW_COPY(IBuffer);
    friend class BufferProxy;
public:
    IBuffer() = default;
    // Allow implmentations to be movable.
    IBuffer(IBuffer&& other) = default;

    /**
     * Write 'data' to the buffer.'
     *
     * Returns 'true' on success, 'false' if the data could not be written.
     */
    template <typename T,
              typename = std::enable_if<std::is_arithmetic<T>::value>>
    bool write(const T& data) {
        const unsigned char* bytes =
            reinterpret_cast<const unsigned char*>(&data);
        std::size_t nBytes = sizeof(T);

        if (remaining() < nBytes)
            return false;

        assert(writeBytes(bytes, nBytes) == nBytes);
        return true;
    }

    /**
     * Write 'len' bytes from 'data' into the buffer.
     *
     * Returns the number of bytes written.  May be less that requested
     * if the reserved space is exceeded.
     */
    std::size_t write(const unsigned char *data, std::size_t len) {
        return writeBytes(data, len);
    }


    /**
     * The number of bytes remaining in this buffer to write into.
     *
     * If the buffer is unbounded, returns std::numeric_limits<size_t>::max.
     */
    virtual std::size_t remaining();
protected:
    /**
     * Write bytes to the buffer.
     *
     * Implementations must override this.
     */
    virtual std::size_t writeBytes(const unsigned char *data,
                                   std::size_t nBytes) = 0;
};

struct Region {
    Region(unsigned char *data, std::size_t size)
        : data(data), size(size) {};

    unsigned char *data;
    std::size_t size;

    unsigned char* end() const {
        return data + size;
    }

    std::size_t remaining(unsigned char *ptr) const {
        return end() - ptr;
    }
};


/**
 * Maintains a reserved region of a buffer, allowing for writing at a later
 * time.
 */
class BufferReservation : public IBuffer {
    DISALLOW_COPY(BufferReservation);
public:
    BufferReservation(std::vector<Region>&& regions);
    BufferReservation(BufferReservation&& other);

    /**
     * The number of bytes remaining to write.
     */
    virtual std::size_t remaining() override { return reservationRemaining; }
protected:
    virtual std::size_t writeBytes(const unsigned char *data,
                                   std::size_t nBytes) override;
private:
    std::vector<Region> regions;
    std::size_t reservationRemaining = 0;
    unsigned char *writePtr = nullptr;
    std::size_t currentRegion = 0;
};

/**
 * Buffer: An append only buffer abstracts efficient writing.
 *
 * It is designed to allocate in a manner that allows for efficient writing
 * to append-only file.
 */
class Buffer : public IBuffer {
    DISALLOW_COPY(Buffer);
public:
    Buffer();
    ~Buffer();

    /**
     * Write the entire buffer to 'fd'.
     */
    void writeToFile(int fd);

    /**
     * Returns a BufferReservation, which maintains a reference to a
     * 'reserved' bytes of this Buffer for later writing.
     *
     * The write pointer of this buffer is advanced by 'reserved'.
     */
    BufferReservation reserve(std::size_t reserved);

    virtual std::size_t remaining() override {
        return std::numeric_limits<std::size_t>::max();
    }
protected:
    virtual std::size_t writeBytes(const unsigned char* data,
                                   std::size_t nBytes) override;
private:
    /**
     * Allocates a new region and sets up the buffer to write there next.
     */
    void newRegion();

    // The underlying memory regions allocated.
    std::vector<Region> regions;

    unsigned char *writePtr = nullptr;
    std::size_t currentRegion = 0;
    const std::size_t PAGE_SIZE;
};

/**
 * BufferProxy: Wraps a buffer and simply forwards calls to the underlying
 * buffer.
 *
 * Meant as a base class for any proxies that perform more useful functions.
 */
class BufferProxy : public IBuffer {
public:
    /**
     * Construct a BufferProxy.  The lifetime of 'buffer' must exceed that of
     * the BufferProxy.
     */
    BufferProxy(IBuffer* buffer) : buffer(buffer) {};
    virtual std::size_t remaining() override { return buffer->remaining(); }
protected:
    virtual std::size_t writeBytes(const unsigned char* data,
                                   std::size_t nBytes) override {
        return buffer->writeBytes(data, nBytes);
    }
private:
    IBuffer *buffer;
};

/**
 * CountedBuffer: A buffer proxy that maintains a count of bytes written.
 */
class CountedBuffer : public BufferProxy {
public:
    CountedBuffer(IBuffer* buffer) : BufferProxy(buffer) {};

    /**
     * Return the number of bytes written to the buffer.
     */
    std::size_t getCount() const {
        return count;
    }

protected:
    virtual std::size_t writeBytes(const unsigned char* data,
                                   std::size_t nBytes) override {
        std::size_t written = BufferProxy::writeBytes(data, nBytes);
        count += written;
        return written;
    }

private:
    std::size_t count = 0;
};

#endif /* _LOGGING_BUFFER_H */
