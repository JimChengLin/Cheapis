#include <cerrno>

#include "anet.h"
#include "env.h"
#include "executor.h"
#include "log.h"
#include "server.h"

namespace cheapis {
    constexpr char kBindAddr[] = "0.0.0.0";
    constexpr unsigned int kPort = 6379;
    constexpr unsigned int kBacklog = 511;
    constexpr unsigned int kCronInterval = 1;
    constexpr unsigned int kMaxAcceptPerCall = 1000;
    constexpr unsigned int kNetIPLength = 46;
    constexpr unsigned int kTCPKeepAlive = 300;
    constexpr unsigned int kReadLength = 4096;
    constexpr unsigned int kTimeout = 360;

    static void ReadFromClient(int fd, Client * c, Executor * executor) {

    }

    static void WriteToClient(int fd, Client * c, EventLoop<Client> * el) {

    }

    static void ExecuteTasks(Executor * executor, EventLoop<Client> * el) {

    }

    static void ServerCron(long * last_cron_time, EventLoop<Client> * el) {

    }

    int ServerMain() {
        const int el_fd = EventLoop<Client>::Open();
        if (el_fd < 0) {
            LIN_LOG_ERROR("Failed creating the event loop. Error message: '%s'",
                          strerror(errno));
            return 1;
        }
        EventLoop<Client> el(el_fd);

        auto executor = OpenExecutorMem();
        if (executor == nullptr) {
            LIN_LOG_ERROR("Failed creating the executor. Error message: '%s'",
                          strerror(errno));
            return 1;
        }

        char err[ANET_ERR_LEN];
        const int ac_fd = anetTcpServer(err, kPort, const_cast<char *>(kBindAddr), kBacklog);
        if (ac_fd < 0) {
            LIN_LOG_ERROR("Failed creating the TCP server. Error message: '%s'", err);
            return 1;
        }
        anetNonBlock(nullptr, ac_fd);

        int r = el.Acquire(ac_fd, std::make_unique<Client>());
        if (r != 0) {
            LIN_LOG_ERROR("Failed acquiring the acceptor's fd");
            return 1;
        }

        r = el.AddEvent(ac_fd, kReadable);
        if (r != 0) {
            LIN_LOG_ERROR("Failed adding the acceptor's readable event. Error message: '%s'",
                          strerror(errno));
            return 1;
        }

        long last_cron_time = GetCurrentTimeInSeconds();
        struct timeval tv = {0};
        while (true) {
            tv.tv_sec = executor->GetTaskCount() ? 0 : kCronInterval;
            r = el.Poll(&tv);
            if (r < 0) {
                LIN_LOG_ERROR("Failed polling. Error message: '%s'",
                              strerror(errno));
                return 1;
            }

            const auto & events = el.GetEvents();
            for (int i = 0; i < r; ++i) {
                const auto & event = events[i];

                int efd = EventLoop<Client>::GetEventFD(event);
                if (efd == ac_fd) { // acceptor
                    int cport, cfd, max = kMaxAcceptPerCall;
                    char cip[kNetIPLength];

                    while (max--) {
                        cfd = anetTcpAccept(err, ac_fd, cip, sizeof(cip), &cport);
                        if (cfd < 0) {
                            if (errno != EAGAIN) {
                                LIN_LOG_WARN("Failed accepting. Error message: '%s'", err);
                            }
                            break;
                        }

                        r = el.Acquire(cfd, std::make_unique<Client>());
                        if (r != 0) {
                            close(cfd);
                            LIN_LOG_WARN("Failed acquiring the client's fd");
                            break;
                        }
                        r = el.AddEvent(cfd, kReadable);
                        if (r != 0) {
                            el.Release(cfd);
                            LIN_LOG_WARN("Failed adding the client's readable event. Error message: '%s'",
                                         strerror(errno));
                            break;
                        }
                        anetNonBlock(nullptr, cfd);
                        anetEnableTcpNoDelay(nullptr, cfd);
                        anetKeepAlive(nullptr, cfd, kTCPKeepAlive);
                        LIN_LOG_DEBUG("Accepted %s:%d", cip, cport);
                    }
                } else { // processor
                    auto & client = el.GetResource(efd);
                    if (EventLoop<Client>::IsEventReadable(event)) {
                        ReadFromClient(efd, client.get(), executor.get());
                    }
                    if (EventLoop<Client>::IsEventWritable(event)) {
                        WriteToClient(efd, client.get(), &el);
                    }
                }
            }

            ExecuteTasks(executor.get(), &el);
            ServerCron(&last_cron_time, &el);
        }
    }
}