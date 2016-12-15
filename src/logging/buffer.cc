#include "logging/buffer.h"

#include <cstddef>
#include <cstring>
#include <sys/mman.h>
#include <sys/uio.h>
#include <unistd.h>
#include <iostream>
#include <vector>

Buffer::Buffer() : page_size(getpagesize()) {
}

Buffer::~Buffer() {
    for (auto &&region : regions) {
        munmap(region.data(), region.size());
    }
}

std::size_t Buffer::writeBytes(const unsigned char* bytes, std::size_t len) {
    if (currentRegion >= regions.size()) {
        newRegion();
    }

    const Region& region = regions[currentRegion];
    std::size_t remaining = len;
    while (remaining > 0) {
        std::size_t regionRemaining = region.remaining(writePtr);
        std::size_t toCopy = regionRemaining < remaining ? regionRemaining :
                                                           remaining;
        std::memcpy(writePtr, bytes + (len - remaining),
                    toCopy);

        writePtr += toCopy;
        remaining -= toCopy;
        totalBytes += toCopy;
        if (writePtr == region.end()) {
            newRegion();
        }
    }

    return len;
}


void Buffer::writeToFile(int fd) {
    // Cap the last region.
    if (regions.size()) {
        regions[regions.size() - 1].iov_len = writePtr - regions[regions.size() - 1].data();
    }

    std::size_t written = 0;
    while (written < totalBytes) {
        std::size_t write_count = writev(fd, regions.data(), regions.size());
        if (static_cast<int>(write_count) == -1) {
            std::cerr << "Fatal error: Buffer::writeToFile cannot write. "
                      << strerror(errno)
                      << std::endl;
            std::exit(EXIT_FAILURE);
        }
        written += write_count;
    }
}

void Buffer::newRegion() {
    // TODO faster, core local memory allocation.
    void* regionData = mmap(nullptr, page_size*256, PROT_READ | PROT_WRITE,
                            MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (regionData == MAP_FAILED) {
        std::cerr << "Fatal error: newRegion cannot allocate. Reason: "
                  << strerror(errno)
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }

    regions.emplace_back(reinterpret_cast<unsigned char*>(regionData),
                         page_size*256);
    currentRegion = regions.size() - 1;
    writePtr = regions[currentRegion].data();
}

BufferReservation Buffer::reserve(std::size_t reserved) {
    if (currentRegion >= regions.size()) {
        newRegion();
    }

    const Region& region = regions[currentRegion];
    std::vector<Region> reservedRegions;
    while (reserved > 0) {
        std::size_t regionRemaining = region.remaining(writePtr);
        std::size_t toReserve = regionRemaining < reserved ? regionRemaining :
                                                             reserved;

        reservedRegions.emplace_back(writePtr, toReserve);

        writePtr += toReserve;
        reserved -= toReserve;
        totalBytes += toReserve;
        if (writePtr == region.end()) {
            newRegion();
        }
    }

    return {std::move(reservedRegions)};
}

BufferReservation::BufferReservation(std::vector<Region>&& regions)
    : regions(std::move(regions)) {

    // Calculate remaining
    if (!this->regions.size()) {
        reservationRemaining = 0;
        return;
    }

    for (const auto& region : this->regions) {
        reservationRemaining += region.size();
    }

    writePtr = this->regions[currentRegion].data();
}

BufferReservation::BufferReservation(BufferReservation &&other)
    : regions(std::move(other.regions)),
      reservationRemaining(other.reservationRemaining),
      writePtr(other.writePtr), currentRegion(other.currentRegion) {}

std::size_t BufferReservation::writeBytes(const unsigned char* data,
                                          std::size_t nBytes) {
    if (currentRegion >= regions.size()) {
        return 0;
    }

    std::size_t remaining = nBytes;
    while (remaining > 0) {
        const Region& region = regions[currentRegion];
        std::size_t regionRemaining = region.remaining(writePtr);
        std::size_t toCopy = regionRemaining < remaining ? regionRemaining :
                                                           remaining;
        std::memcpy(writePtr, data + (nBytes - remaining),
                    toCopy);

        writePtr += toCopy;
        remaining -= toCopy;
        reservationRemaining -= toCopy;
        if (writePtr == region.end()) {
            currentRegion++;
            if (currentRegion >= regions.size()) {
                return nBytes - remaining;
            }

            writePtr = regions[currentRegion].data();
        }
    }

    return nBytes;
}
