#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include "env.h"
#include "log.h"

#if !defined(__linux__)
#define POSIX_FADV_NORMAL
#define POSIX_FADV_SEQUENTIAL
#define POSIX_FADV_RANDOM

#define posix_fadvise(fd, offset, len, advice) 0
#endif

#define PERM_rw_r__r__ 0644

namespace cheapis {
    int OpenFile(const std::string & name, int flags) {
        return open(name.c_str(), flags, PERM_rw_r__r__);
    }

    int FileAllocate(int fd, uint64_t n) {
        int r;
#if !defined(__linux__)
        r = ftruncate(fd, static_cast<off_t>(n));
#else
        r = posix_fallocate(fd, 0, static_cast<off_t>(n));
#endif
        return r;
    }

    int FilePrefetch(int fd, uint64_t offset, uint64_t n) {
        int r;
#if defined(__linux__)
        r = static_cast<int>(readahead(fd, offset, n));
#else
        radvisory advice = {static_cast<off_t>(offset),
                            static_cast<int>(n)};
        r = fcntl(fd, F_RDADVISE, &advice);
#endif
        return r;
    }

    int FileHint(int fd, AccessPattern pattern) {
        int r = 0;
        switch (pattern) {
            case kNormal:
                r = posix_fadvise(fd, 0, 0, POSIX_FADV_NORMAL);
                break;
            case kSequential:
                r = posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
                break;
            case kRandom:
#if defined(__linux__)
                r = posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);
#else
                r = fcntl(fd, F_RDAHEAD, 0);
#endif
                break;
        }
        return r;
    }

    int FileRangeSync(int fd, uint64_t offset, uint64_t n) {
        int r = 0;
#if defined(__linux__)
        r = sync_file_range(fd, static_cast<off_t>(offset), static_cast<off_t>(n),
                            SYNC_FILE_RANGE_WRITE);
#endif
        return r;
    }

    MmapRWFile::~MmapRWFile() {
        munmap(base_, len_);
        close(fd_);
    }

    int MmapRWFile::Resize(uint64_t n) {
        int r = FileAllocate(fd_, n);
        if (r != 0) {
            LIN_LOG_ERROR("Failed resizing the MmapRWFile. Error message: '%s'",
                          strerror(errno));
            return -1;
        }
#if !defined(__linux__)
        if (munmap(base_, len_) != 0) {
            LIN_LOG_ERROR("Failed resizing the MmapRWFile. Error message: '%s'",
                          strerror(errno));
            return -1;
        }
        base_ = mmap(nullptr, n, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
#else
        base_ = mremap(base_, len_, n, MREMAP_MAYMOVE);
#endif
        if (base_ == MAP_FAILED) {
            LIN_LOG_ERROR("Failed resizing the MmapRWFile. Error message: '%s'",
                          strerror(errno));
            return -1;
        }
        len_ = n;
        return 0;
    }

    int MmapRWFile::Hint(AccessPattern pattern) {
        int r = 0;
        switch (pattern) {
            case kNormal:
                r = posix_madvise(base_, len_, POSIX_MADV_NORMAL);
                break;
            case kSequential:
                r = posix_madvise(base_, len_, POSIX_MADV_SEQUENTIAL);
                break;
            case kRandom:
                r = posix_madvise(base_, len_, POSIX_MADV_RANDOM);
                break;
        }
        return r;
    }

    std::unique_ptr<MmapRWFile>
    OpenMmapRWFile(const std::string & name, uint64_t n) {
        int fd = OpenFile(name, O_CREAT | O_RDWR);
        if (fd < 0) {
            LIN_LOG_ERROR("Failed opening the MmapRWFile. Error message: '%s'",
                          strerror(errno));
            return nullptr;
        }
        int r = FileAllocate(fd, n);
        if (r != 0) {
            LIN_LOG_ERROR("Failed opening the MmapRWFile. Error message: '%s'",
                          strerror(errno));
            return nullptr;
        }
        void * base = mmap(nullptr, n, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (base == MAP_FAILED) {
            LIN_LOG_ERROR("Failed opening the MmapRWFile. Error message: '%s'",
                          strerror(errno));
            return nullptr;
        }
        return std::make_unique<MmapRWFile>(base, n, fd);
    }
}