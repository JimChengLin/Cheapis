#include <cerrno>
#include <deque>
#include <fcntl.h>
#include <unordered_map>

#include "../env.h"
#include "../executor.h"
#include "../log.h"
#include "filename.h"

#include "likely.h"
#include "sig_tree.h"
#include "sig_tree_impl.h"
#include "sig_tree_node_impl.h"
#include "sig_tree_rebuild_impl.h"
#include "sig_tree_visit_impl.h"

#define UINT5_MAX  ((1 << 5) - 1)
#define UINT11_MAX ((1 << 11) - 1)

namespace cheapis {
    using namespace sgt;

    constexpr unsigned int kMaxDataFileSize = 2147483648;

    class ExecutorDiskImpl;

    static inline uint16_t
    PackKVLength(size_t k_len, size_t v_len) {
        return static_cast<uint16_t>((std::min<size_t>(k_len, UINT5_MAX) << 11) |
                                     (std::min<size_t>(v_len, UINT11_MAX)));
    }

    static inline std::pair<uint16_t, uint16_t>
    UnpackLength(uint16_t len) {
        return {len >> 11, len & UINT11_MAX};
    }

    static inline uint64_t
    PackIDLengthAndOffset(uint16_t id, uint16_t len, uint32_t off) {
        return (static_cast<uint64_t>(id) << (16 + 32)) |
               (static_cast<uint64_t>(len) << 32) |
               (static_cast<uint64_t>(off));
    }

    static inline std::tuple<uint16_t, uint16_t, uint32_t>
    UnpackKVRep(uint64_t rep) {
        return {(rep >> (16 + 32)),
                (rep >> 32) & UINT16_MAX,
                (rep & UINT32_MAX)};
    }

    class KVTrans {
    private:
        ExecutorDiskImpl * executor_;
        uint64_t rep_;
        Slice k_;

    public:
        KVTrans(ExecutorDiskImpl * executor, uint64_t rep)
                : executor_(executor), rep_(rep) {}

        bool operator==(const Slice & k) const;

        Slice Key() const;

        bool Get(const Slice & k, std::string * v) const;

    private:
        void LoadKey(uint16_t id, uint16_t k_len, uint32_t offset);
    };

    class Helper final : public SignatureTreeTpl<KVTrans>::Helper {
    public:
        explicit Helper(ExecutorDiskImpl * executor) : executor_(executor) {}

        ~Helper() override = default;

        uint64_t Add(const Slice & k, const Slice & v) override { return {}; }

        void Del(KVTrans & trans) override {}

        uint64_t Pack(size_t offset) const override {
            return offset | (1ULL << 63);
        }

        size_t Unpack(const uint64_t & rep) const override {
            return rep & ((1ULL << 63) - 1);
        }

        bool IsPacked(const uint64_t & rep) const override {
            return static_cast<bool>(rep >> 63);
        }

        KVTrans Trans(const uint64_t & rep) const override {
            return KVTrans(executor_, rep);
        }

    private:
        ExecutorDiskImpl * executor_;
    };

    class AllocatorImpl final : public Allocator {
    public:
        explicit AllocatorImpl(std::unique_ptr<MmapRWFile> && file)
                : file_(std::move(file)),
                  allocate_(0),
                  recycle_(-1) {}

        ~AllocatorImpl() override = default;

        void * Base() override {
            return file_->Base();
        }

        size_t AllocatePage() override {
            size_t offset;
            if (recycle_ >= 0) {
                offset = static_cast<size_t>(recycle_);
                recycle_ = *reinterpret_cast<int64_t *>(reinterpret_cast<uintptr_t>(Base()) + offset);
            } else {
                offset = allocate_;
                size_t occupy = offset + kPageSize;
                if (occupy > file_->GetFileSize()) {
                    throw AllocatorFullException();
                } else {
                    allocate_ = occupy;
                }
            }
            return offset;
        }

        void FreePage(size_t offset) override {
            *reinterpret_cast<int64_t *>(reinterpret_cast<uintptr_t>(Base()) + offset) = recycle_;
            recycle_ = static_cast<int64_t>(offset);
        }

        void Grow() override {
            int r = file_->Resize(file_->GetFileSize() * 2);
            if (r != 0) {
                LIN_LOG_ERROR("Failed growing");
                exit(1);
            }
        }

    private:
        std::unique_ptr<MmapRWFile> file_;
        size_t allocate_;
        int64_t recycle_;
    };

    struct Header {
        uint16_t k_len;
        uint16_t v_len;
    };

    class ExecutorDiskImpl final : public Executor {
    private:
        enum Command {
            kGet,
            kSet,
            kDel,
            kUnsupported,
        };

        struct Task {
            rocksdb::autovector<std::string> argv;
            Client * c;
            int fd;
            Command cmd;
        };

    public:
        ExecutorDiskImpl(std::string dir,
                         std::unique_ptr<MmapRWFile> && file)
                : dir_(std::move(dir)),
                  helper_(this),
                  allocator_(std::move(file)),
                  tree_(&helper_, &allocator_) {}

        ~ExecutorDiskImpl() override {
            for (auto & p:fd_map_) {
                close(p.second);
            }
        }

        void Submit(const rocksdb::autovector<std::string_view> & argv,
                    Client * c, int fd) override {
            Task & task = tasks_.emplace_back();
            task.c = c;
            task.fd = fd;

            if (argv[0] == "GET" && argv.size() == 2) {
                const auto & k = argv[1];
                task.cmd = kGet;
                task.argv.emplace_back(k);
                PrefetchKeyValue(k, tree_.GetRep(k));
            } else if (argv[0] == "SET" && argv.size() == 3) {
                const auto & k = argv[1];
                const auto & v = argv[2];
                task.cmd = kSet;
                task.argv.emplace_back(k);
                task.argv.emplace_back(v);
                PrefetchKey(k, tree_.GetRep(k));
            } else if (argv[0] == "DEL" && argv.size() == 2) {
                const auto & k = argv[1];
                task.cmd = kDel;
                task.argv.emplace_back(k);
                PrefetchKey(k, tree_.GetRep(k));
            } else {
                task.cmd = kUnsupported;
            }
        }

        void Execute(size_t n, long curr_time, EventLoop<Client> * el) override {
            if (n == 0) {
                return;
            }
            CreateFileIfNeed();
            buf_.clear();
            batch_.clear();

            auto it = tasks_.cbegin();
            for (size_t i = 0; i < n; ++i) {
                const Task & task = *it++;
                if (task.cmd == kSet && !task.c->close) {
                    const auto & k = task.argv[0];
                    const auto & v = task.argv[1];
                    Header header = {static_cast<uint16_t>(k.size()),
                                     static_cast<uint16_t>(v.size())};

                    buf_.append(reinterpret_cast<char *>(&header), sizeof(header));
                    buf_.append(k);
                    buf_.append(v);

                    batch_.emplace_back(offset_);
                    offset_ += sizeof(header) + k.size() + v.size();
                }
            }

            ssize_t nwrite = write(fd_map_[curr_id_], buf_.data(), buf_.size());
            if (nwrite != buf_.size()) {
                LIN_LOG_ERROR("Failed writing. Error message: '%s'", strerror(errno));
                exit(1);
            }

            for (size_t i = 0, j = 0; i < n; tasks_.pop_front(), ++i) {
                Task & task = tasks_.front();
                Client * c = task.c;
                int fd = task.fd;

                --c->ref_count;
                if (c->close) {
                    if (c->ref_count == 0) {
                        el->Release(fd);
                    }
                    continue;
                }

                bool blocked = !c->output.empty();
                auto & argv = task.argv;
                switch (task.cmd) {
                    case kGet: {
                        bool found = tree_.Get(argv[0], &v_);
                        if (found) {
                            RespMachine::AppendBulkString(&c->output, v_);
                        } else {
                            RespMachine::AppendNullArray(&c->output);
                        }
                        break;
                    }

                    case kSet: {
                        uint64_t rep = PackIDLengthAndOffset(static_cast<uint16_t>(curr_id_),
                                                             PackKVLength(argv[0].size(),
                                                                          argv[1].size()),
                                                             batch_[j++]);
                        tree_.Add(argv[0], rep, [rep](KVTrans & trans, uint64_t & ref) -> bool {
                            ref = rep;
                            return true;
                        });
                        RespMachine::AppendSimpleString(&c->output, "OK");
                        break;
                    }

                    case kDel: {
                        tree_.Del(argv[0]);
                        RespMachine::AppendSimpleString(&c->output, "OK");
                        break;
                    }

                    case kUnsupported: {
                        RespMachine::AppendError(&c->output, "Unsupported Command");
                        break;
                    }
                }

                if (!blocked) {
                    nwrite = write(fd, c->output.data(), c->output.size());
                    if (nwrite > 0) {
                        c->output.assign(c->output.data() + nwrite,
                                         c->output.size() - nwrite);
                    }
                    if (!c->output.empty()) {
                        el->AddEvent(fd, kWritable);
                    }
                }
            }
        }

        size_t GetTaskCount() const override {
            return tasks_.size();
        }

    private:
        void PrefetchKey(const Slice & k, const uint64_t * rep) {
            if (SGT_LIKELY(rep != nullptr)) {
                uint16_t id;
                uint16_t length;
                uint32_t offset;
                std::tie(id, length, offset) = UnpackKVRep(*rep);

                uint16_t k_len;
                std::tie(k_len, std::ignore) = UnpackLength(length);

                FilePrefetch(fd_map_[id], offset, sizeof(Header) + k_len);
            }
        }

        void PrefetchKeyValue(const Slice & k, const uint64_t * rep) {
            if (SGT_LIKELY(rep != nullptr)) {
                uint16_t id;
                uint16_t length;
                uint32_t offset;
                std::tie(id, length, offset) = UnpackKVRep(*rep);

                uint16_t k_len;
                uint16_t v_len;
                std::tie(k_len, v_len) = UnpackLength(length);

                FilePrefetch(fd_map_[id], offset, sizeof(Header) + k_len + v_len);
            }
        }

        void CreateFileIfNeed() {
            if (offset_ >= kMaxDataFileSize) {
                ++curr_id_;
                DataFilename(dir_, static_cast<uint64_t>(curr_id_), &buf_);
                int fd = OpenFile(buf_, O_CREAT | O_RDWR | O_TRUNC);
                if (fd < 0) {
                    LIN_LOG_ERROR("Failed opening. Error message: '%s'", strerror(errno));
                    exit(1);
                }

                FileHint(fd, kRandom);
                FileTruncate(fd, kMaxDataFileSize);
                fd_map_[curr_id_] = fd;
                offset_ = 0;
            }
        }

    private:
        std::string dir_;
        std::string buf_;
        std::string v_;
        std::vector<uint32_t> batch_;

        Helper helper_;
        AllocatorImpl allocator_;
        SignatureTreeTpl<KVTrans> tree_;

        std::deque<Task> tasks_;
        std::unordered_map<uint16_t, int> fd_map_;

        int32_t curr_id_ = -1;
        uint32_t offset_ = UINT32_MAX;

        friend class KVTrans;
    };

    bool KVTrans::operator==(const Slice & k) const {
        if (k_.size() != 0) {
            return k_ == k;
        }

        uint16_t id;
        uint16_t length;
        uint32_t offset;
        std::tie(id, length, offset) = UnpackKVRep(rep_);

        uint16_t k_len;
        std::tie(k_len, std::ignore) = UnpackLength(length);

        if ((k_len == k.size()) ||
            (k_len == UINT5_MAX && k.size() > k_len)) {
            const_cast<KVTrans *>(this)->LoadKey(id, k_len, offset);
            return k_ == k;
        } else {
            return false;
        }
    }

    Slice KVTrans::Key() const {
        if (k_.size() != 0) {
            return k_;
        }

        uint16_t id;
        uint16_t length;
        uint32_t offset;
        std::tie(id, length, offset) = UnpackKVRep(rep_);

        uint16_t k_len;
        std::tie(k_len, std::ignore) = UnpackLength(length);

        const_cast<KVTrans *>(this)->LoadKey(id, k_len, offset);
        return k_;
    }

    bool KVTrans::Get(const Slice & k, std::string * v) const {
        assert(k_.size() == 0);
        uint16_t id;
        uint16_t length;
        uint32_t offset;
        std::tie(id, length, offset) = UnpackKVRep(rep_);

        uint16_t k_len;
        uint16_t v_len;
        std::tie(k_len, v_len) = UnpackLength(length);

        Header header;
        std::string & buf = executor_->buf_;
        buf.resize(sizeof(header) + k_len + v_len);

        int fd = executor_->fd_map_[id];
        ssize_t nread = pread(fd, buf.data(), buf.size(), offset);
        if (nread != buf.size()) {
            LIN_LOG_ERROR("Failed preading. Error message: '%s'", strerror(errno));
            exit(1);
        }

        memcpy(&header, buf.data(), sizeof(header));
        size_t have = buf.size();
        size_t need = sizeof(header) + header.k_len + header.v_len;
        assert(need >= have);
        size_t less = need - have;
        if (less > 0) {
            buf.resize(need);

            nread = pread(fd, &buf[have], less, offset + have);
            if (nread != less) {
                LIN_LOG_ERROR("Failed preading. Error message: '%s'", strerror(errno));
                exit(1);
            }
        }
        const_cast<KVTrans *>(this)->k_ = {buf.data() + sizeof(header), header.k_len};

        if (k_ == k) {
            if (v != nullptr) {
                v->assign(k_.data() + k_.size(), header.v_len);
            }
            return true;
        } else {
            return false;
        }
    }

    void KVTrans::LoadKey(uint16_t id, uint16_t k_len, uint32_t offset) {
        std::string & buf = executor_->buf_;
        buf.resize(sizeof(Header) + k_len);

        int fd = executor_->fd_map_[id];
        ssize_t nread = pread(fd, buf.data(), buf.size(), offset);
        if (nread != buf.size()) {
            LIN_LOG_ERROR("Failed preading. Error message: '%s'", strerror(errno));
            exit(1);
        }

        memcpy(&k_len, buf.data(), sizeof(k_len));
        size_t have = buf.size();
        size_t need = sizeof(Header) + k_len;
        assert(need >= have);
        size_t less = need - have;
        if (less > 0) {
            buf.resize(need);

            nread = pread(fd, &buf[have], less, offset + have);
            if (nread != less) {
                LIN_LOG_ERROR("Failed preading. Error message: '%s'", strerror(errno));
                exit(1);
            }
        }
        k_ = {buf.data() + sizeof(Header), k_len};
    }

    std::unique_ptr<Executor>
    OpenExecutorDisk(const std::string & name) {
        std::string index_filename;
        IndexFilename(name, &index_filename);
        auto index_file = OpenMmapRWFile(index_filename, kPageSize);
        if (index_file == nullptr) {
            return nullptr;
        }
        index_file->Hint(kRandom);
        return std::make_unique<ExecutorDiskImpl>(name, std::move(index_file));
    }
}