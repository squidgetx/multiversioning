#include "logging/buffer.h"

#include <cstddef>
#include <cstring>
#include <sys/mman.h>
#include <unistd.h>
#include <iostream>
#include <vector>

Buffer::Buffer() : PAGE_SIZE(getpagesize()) {
}

Buffer::~Buffer() {
    for (auto &&region : regions) {
        munmap(region.data, region.size);
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
        if (writePtr == region.end()) {
            newRegion();
        }
    }

    return len;
}

void Buffer::newRegion() {
    // TODO faster, core local memory allocation.
    void* regionData = mmap(nullptr, PAGE_SIZE, PROT_READ | PROT_WRITE,
                            MAP_ANONYMOUS, 0, 0);
    if (regionData == MAP_FAILED) {
        std::cerr << "Fatal error: newRegion cannot allocate. "
                  << strerror(errno)
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }

    regions.emplace_back(reinterpret_cast<unsigned char*>(regionData),

                         PAGE_SIZE);
    currentRegion = regions.size() - 1;
    writePtr = regions[currentRegion].data;
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
        if (writePtr == region.end()) {
            newRegion();
        }
    }

    return {std::move(reservedRegions)};
}

BufferReservation::BufferReservation(std::vector<Region>&& regions)
    : regions(std::move(regions)) {

    // Calculate remaining
    if (!regions.size()) {
        reservationRemaining = 0;
        return;
    }

    for (const auto& region : regions) {
        reservationRemaining += region.size;
    }
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

            writePtr = regions[currentRegion].data;
        }
    }

    return nBytes;
}
