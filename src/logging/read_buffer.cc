#include "logging/read_buffer.h"

#include <cstddef>
#include <cstring>
#include <errno.h>
#include <iostream>
#include <sys/mman.h>
#include <unistd.h>

FileBuffer::FileBuffer(int fd) : _fd(fd), PAGE_SIZE(getpagesize()) {
    void *buff = mmap(nullptr, PAGE_SIZE, PROT_READ | PROT_WRITE,
                      MAP_ANONYMOUS | MAP_PRIVATE, 0, 0);
    if (buff == MAP_FAILED) {
        std::cerr << "FileBuffer unable to map memory." << strerror(errno) <<
                     std::endl;
        exit(EXIT_FAILURE);
    }
    _readPtr = reinterpret_cast<unsigned char*>(buff);
}

FileBuffer::~FileBuffer() {
    if (_buff) {
        munmap(_buff, PAGE_SIZE);
    }
}

std::size_t FileBuffer::readBytes(unsigned char *out, std::size_t nBytes) {
    std::size_t read = 0;

    while (nBytes) {
        std::size_t remaining = (_buff + _buffLen) - _readPtr;
        std::size_t toRead = remaining < nBytes ? remaining : nBytes;

        if (toRead == 0 && nBytes) {
            readFile();
            if (_buffLen == 0)
                return read;
        }

        memcpy(out, _readPtr, toRead);
        out += toRead;
        nBytes -= toRead;
        read += toRead;
    }

    return read;
}

void FileBuffer::readFile() {
    std::size_t bytesRead = 0;
    while (bytesRead < PAGE_SIZE) {
        std::size_t amtRead = ::read(_fd, _buff, PAGE_SIZE - bytesRead);
        if ((int)amtRead == -1) {
            std::cerr << "FileBuffer::readFile unable to read. "
                      << strerror(errno)
                      << std::endl;
            exit(EXIT_FAILURE);
        }

        if (amtRead == 0) {
            break;
        }

        bytesRead += amtRead;
    }

    _buffLen = bytesRead;
    _readPtr = _buff;
}

ReadViewBuffer::ReadViewBuffer(IReadBuffer *buffer, std::size_t len)
    : _len(len), _underlyingBuffer(buffer) {
}

ReadViewBuffer::~ReadViewBuffer() {
}

std::size_t ReadViewBuffer::readBytes(unsigned char* out, std::size_t nBytes) {
    std::size_t nToRead = _len - _nRead < nBytes ? _len - _nRead : nBytes;
    std::size_t nRead = _underlyingBuffer->read(out, nToRead);

    _nRead += nRead;
    return nRead;
}
