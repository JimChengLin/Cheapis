#pragma once
#ifndef CHEAPIS_ENV_H
#define CHEAPIS_ENV_H

#include <memory>
#include <string>
#include <sys/time.h>

namespace cheapis {
    inline time_t GetCurrentTimeInSeconds() {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return tv.tv_sec;
    }

    inline time_t GetCurrentTimeInMilliseconds() {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return tv.tv_sec * 1000 + tv.tv_usec / 1000;
    }

    enum AccessPattern {
        kNormal,
        kSequential,
        kRandom,
    };

    int OpenFile(const std::string & name, int flags);

    int FileAllocate(int fd, uint64_t n);

    int FilePrefetch(int fd, uint64_t offset, uint64_t n);

    int FileHint(int fd, AccessPattern pattern);

    int FileRangeSync(int fd, uint64_t offset, uint64_t n);

    class MmapRWFile {
    public:
        MmapRWFile(void * base, uint64_t len, int fd)
                : base_(base), len_(len), fd_(fd) {}

        ~MmapRWFile();

    public:
        int Resize(uint64_t n);

        int Hint(AccessPattern pattern);

        void * Base() { return base_; }

        uint64_t GetFileSize() const { return len_; }

    private:
        void * base_;
        uint64_t len_;
        int fd_;
    };

    std::unique_ptr<MmapRWFile>
    OpenMmapRWFile(const std::string & name, uint64_t n);
}

#endif //CHEAPIS_ENV_H