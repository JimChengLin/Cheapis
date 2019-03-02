#include <deque>
#include <map>

#include "executor.h"

namespace cheapis {
    struct Task {
        rocksdb::autovector<std::string> argv;
        Client * c;
        int fd;
    };

    class ExecutorMemImpl final : public Executor {
    public:
        ~ExecutorMemImpl() override = default;

        void Submit(const rocksdb::autovector<std::string_view> & argv,
                    Client * c, int fd) override {
            Task & task = tasks_.emplace_back();
            for (const auto & arg : argv) { task.argv.emplace_back(arg); }
            task.c = c;
            task.fd = fd;
        }

        void Execute(size_t n, long curr_time, EventLoop<Client> * el) override {
            for (size_t i = 0; i < n; ++i) {
                Task & task = tasks_.front();
                Client * c = task.c;
                int fd = task.fd;

                --c->ref_count;
                if (c->close) {
                    if (c->ref_count == 0) {
                        el->Release(fd);
                    }
                    return;
                }

                bool blocked = !c->output.empty();
                auto & argv = task.argv;
                if (argv[0] == "GET" && argv.size() == 2) {
                    auto it = map_.find(argv[1]);
                    if (it != map_.cend()) {
                        RespMachine::AppendBulkString(&c->output, it->second);
                    } else {
                        RespMachine::AppendNullArray(&c->output);
                    }
                } else if (argv[0] == "SET" && argv.size() == 3) {
                    map_.emplace(std::move(argv[1]), std::move(argv[2]));
                    RespMachine::AppendSimpleString(&c->output, "OK");
                } else if (argv[0] == "DEL" && argv.size() == 2) {
                    map_.erase(argv[1]);
                    RespMachine::AppendSimpleString(&c->output, "OK");
                } else {
                    RespMachine::AppendError(&c->output, "Unsupported Command");
                }

                if (!blocked) {
                    ssize_t nwrite = write(fd, c->output.data(), c->output.size());
                    if (nwrite > 0) {
                        c->output.assign(c->output.data() + nwrite,
                                         c->output.size() - nwrite);
                    }
                    if (!c->output.empty()) {
                        el->AddEvent(fd, kWritable);
                    }
                }

                tasks_.pop_front();
            }
        }

        size_t GetTaskCount() const override {
            return tasks_.size();
        }

    private:
        std::deque<Task> tasks_;
        std::map<std::string, std::string> map_;
    };

    std::unique_ptr<Executor>
    OpenExecutorMem() {
        return std::make_unique<ExecutorMemImpl>();
    }
}