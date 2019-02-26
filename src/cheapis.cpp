#include <cerrno>
#include <deque>
#include <string>

#include "anet.h"
#include "autovector.h"
#include "dakv.h"
#include "env.h"
#include "cheapis.h"
#include "gujia.h"
#include "gujia_impl.h"
#include "resp_machine.h"

#define MAX_ACCEPTS_PER_CALL 1000
#define NET_IP_STR_LEN 46  /* INET6_ADDRSTRLEN is 46, but we need to be sure */
#define EWOULDBLOCK EAGAIN /* Operation would block */

namespace cheapis {
    using namespace gujia;

    struct Client {
        RespMachine resp;
        std::string in_buf;
        std::string out_buf;
        long last_mod_time;
        int ref_cnt = 0;
        int consume_len = 0;
        bool close = false;

        Client() : last_mod_time(GetCurrentTimeInSeconds()) {}
    };

    enum Command {
        kSet,
        kGet,
        kDel,
    };

    struct Task {
        Client * c;
        rocksdb::autovector<std::string> argv;
        int fd;

        Task(Client * c, rocksdb::autovector<std::string_view> args, int fd)
                : c(c),
                  fd(fd) {
            argv.resize(args.size());
            for (unsigned i = 0; i < args.size(); ++i) {
                argv[i] = args[i];
            }
        }
    };

    constexpr char kBindAddr[] = "0.0.0.0";
    constexpr int kPort = 8000;
    constexpr int kBacklog = 511;
    constexpr int kServerCronInterval = 1;
    constexpr int kTCPKeepAlive = 300;
    constexpr unsigned kReadBlockSize = 4096;
    constexpr unsigned kTimeout = 180;

    void Run() {
        const int el_fd = EventLoop<>::Open();
        assert(el_fd >= 0);
        EventLoop<Client> el(el_fd);
        std::deque<Task> tasks;
        DaKV dakv;

        const int acceptor_fd = anetTcpServer(nullptr, kPort, const_cast<char *>(kBindAddr), kBacklog);
        assert(acceptor_fd >= 0 && acceptor_fd < el.GetResources().size());
        el.Acquire(acceptor_fd, std::make_unique<Client>());
        int r = el.AddEvent(acceptor_fd, kReadable);
        assert(r == 0);

        auto server_cron = [&el, acceptor_fd]() {
            auto & acceptor = el.GetResource(acceptor_fd);
            long long curr_time = GetCurrentTimeInSeconds();
            if (curr_time - acceptor->last_mod_time > kServerCronInterval) {
                auto & clients = el.GetResources();
                for (int i = 0; i <= el.GetMaxFD(); ++i) {
                    auto & client = clients[i];
                    if (client != nullptr && i != acceptor_fd) {
                        if (curr_time - client->last_mod_time > kTimeout) {
                            el.Release(i);
                        }
                    }
                }
                acceptor->last_mod_time = GetCurrentTimeInSeconds();
            }
        };

        auto read_query_from_client = [&el, &tasks](Client * c, int fd) {
            auto & in_buf = c->in_buf;
            char buf[kReadBlockSize];
            ssize_t nread = read(fd, buf, kReadBlockSize);
            if (nread == -1) {
                if (errno == EAGAIN) {
                    return;
                } else {
                    if (c->ref_cnt == 0) {
                        el.Release(fd);
                    } else {
                        c->close = true;
                        el.DelEvent(fd, kReadable | kWritable);
                    }
                    return;
                }
            } else if (nread == 0) {
                if (c->ref_cnt == 0) {
                    el.Release(fd);
                } else {
                    c->close = true;
                    el.DelEvent(fd, kReadable | kWritable);
                }
                return;
            }
            in_buf.append(buf, static_cast<size_t>(nread));
            c->last_mod_time = GetCurrentTimeInSeconds();

            while (c->consume_len < in_buf.size()) {
                size_t consume_len = c->resp.Input(in_buf.data() + c->consume_len,
                                                   in_buf.size() - c->consume_len);
                c->consume_len += consume_len;
                switch (c->resp.GetState()) {
                    case RespMachine::kSuccess: { // publish task
                        assert(consume_len != 0);
                        tasks.emplace_back(c, c->resp.GetArgv(), fd);
                        ++c->ref_cnt;
                        c->resp.Reset();
                        in_buf.assign(&in_buf[c->consume_len], &in_buf[in_buf.size()]);
                        c->consume_len = 0;
                        break;
                    }

                    case RespMachine::kProcess: {
                        return;
                    }

                    case RespMachine::kInit:
                    default: { // error
                        if (c->ref_cnt == 0) {
                            el.Release(fd);
                        } else {
                            c->close = true;
                            el.DelEvent(fd, kReadable | kWritable);
                        }
                        printf("parsing error\n");
                        return;
                    }
                }
            }
        };

        auto write_out_buf = [&el](Client * c, int fd) {
            ssize_t nwritten = write(fd, c->out_buf.data(), c->out_buf.size());
            if (nwritten <= 0) {
                if (errno != EAGAIN) {
                    if (c->ref_cnt == 0) {
                        el.Release(fd);
                    } else {
                        c->close = true;
                        el.DelEvent(fd, kReadable | kWritable);
                    }
                }
                return;
            } else {
                c->last_mod_time = GetCurrentTimeInSeconds();
                c->out_buf.assign(c->out_buf.data() + nwritten,
                                  c->out_buf.size() - nwritten);
            }
            if (!c->out_buf.empty()) {
            } else {
                el.DelEvent(fd, kWritable);
            }
        };

        auto process_task = [&el, &dakv](const Task & task) {
            Client * c = task.c;
            --c->ref_cnt;
            if (c->close) {
                if (c->ref_cnt == 0) {
                    el.Release(task.fd);
                }
                return;
            }

            bool did_send = !c->out_buf.empty();
            if (task.argv[0] == "GET") {
                std::string buf;
                int ret = dakv.Get(task.argv[1], &buf);
                if (ret == 0) {
                    RespMachine::AppendBulkString(&c->out_buf, buf.data(), buf.size());
                } else {
                    RespMachine::AppendNullArray(&c->out_buf);
                }
            } else if (task.argv[0] == "SET") {
                dakv.Set(task.argv[1], task.argv[2]);
                RespMachine::AppendSimpleString(&c->out_buf, "OK", 2);
            } else if (task.argv[0] == "DEL") {
                dakv.Del(task.argv[1]);
                RespMachine::AppendSimpleString(&c->out_buf, "OK", 2);
            } else {
                RespMachine::AppendError(&c->out_buf, "Unsupported Command", 19);
            }

            if (!did_send) {
                ssize_t nwritten = write(task.fd, c->out_buf.data(), c->out_buf.size());
                if (nwritten <= 0) {
                } else {
                    c->out_buf.assign(c->out_buf.data() + nwritten,
                                      c->out_buf.size() - nwritten);
                }
                if (!c->out_buf.empty()) {
                    el.AddEvent(task.fd, kWritable);
                }
            }
        };

        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        while (true) {
            r = el.Poll(&tv);
            if (r < 0) {
                perror("Loop exits");
                break;
            }

            const auto & events = el.GetEvents();
            for (int i = 0; i < r; ++i) {
                const auto & event = events[i];
                int efd = EventLoop<>::GetEventFD(event);
                auto & resource = el.GetResource(efd);

                if (efd == acceptor_fd) { // acceptor
                    int cport, cfd, max = MAX_ACCEPTS_PER_CALL;
                    char cip[NET_IP_STR_LEN];

                    while (max--) {
                        cfd = anetTcpAccept(nullptr, acceptor_fd, cip, sizeof(cip), &cport);
                        if (cfd == ANET_ERR) {
                            if (errno != EWOULDBLOCK)
                                perror("Accepting client connection");
                            break;
                        }

                        if (cfd < el.GetResources().size()) {
                            el.Acquire(cfd, std::make_unique<Client>());
                            anetNonBlock(nullptr, efd);
                            anetEnableTcpNoDelay(nullptr, efd);
                            anetKeepAlive(nullptr, cfd, kTCPKeepAlive);
                            el.AddEvent(cfd, kReadable);
                        } else {
                            close(cfd);
                            break;
                        }
                    }
                } else { // processor
                    if (EventLoop<>::IsEventReadable(event)) {
                        read_query_from_client(resource.get(), efd);
                    }
                    if (EventLoop<>::IsEventWritable(event)) {
                        write_out_buf(resource.get(), efd);
                    }
                }
            }

            while (!tasks.empty()) {
                process_task(tasks.front());
                tasks.pop_front();
            }

            server_cron();
        }
    }
}