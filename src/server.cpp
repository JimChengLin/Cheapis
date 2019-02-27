#include <cerrno>
#include <string>

#include "anet.h"
#include "env.h"
#include "executor.h"
#include "gujia.h"
#include "gujia_impl.h"
#include "log.h"
#include "resp_machine.h"
#include "server.h"

namespace cheapis {
    using namespace gujia;

    struct Client {
        RespMachine resp;
        std::string input;
        std::string output;
        long last_mod_time;
        unsigned int ref_count = 0;
        unsigned int consume_len = 0;
        bool close = false;

        explicit Client(long last_mod_time)
                : last_mod_time(last_mod_time) {}
    };

    constexpr char kBindAddr[] = "0.0.0.0";
    constexpr unsigned int kPort = 6379;
    constexpr unsigned int kBacklog = 511;
    constexpr unsigned int kCronInterval = 1;
    constexpr unsigned int kMaxAcceptPerCall = 1000;
    constexpr unsigned int kNetIPLen = 46;
    constexpr unsigned int kTCPKeepAlive = 300;
    constexpr unsigned int kReadBlockSize = 4096;
    constexpr unsigned int kTimeout = 360;

    static void ReadFromClient() {

    }

    static void WriteToClient() {

    }

    int ServerMain() {
        const int el_fd = EventLoop<>::Open();
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
            LIN_LOG_ERROR("Cannot acquire the acceptor's fd");
            return 1;
        }

        r = el.AddEvent(ac_fd, kReadable);
        if (r != 0) {
            LIN_LOG_ERROR("Failed adding the acceptor's readable event. Error message: '%s'",
                          strerror(errno));
            return 1;
        }

        struct timeval tv = {0};
        tv.tv_sec = kCronInterval;
        while (true) {
            r = el.Poll(&tv);
            if (r < 0) {
                LIN_LOG_ERROR("Failed polling. Error message: '%s'",
                              strerror(errno));
                return 1;
            }

            const auto & events = el.GetEvents();
            for (int i = 0; i < r; ++i) {
                const auto & event = events[i];

                int efd = EventLoop<>::GetEventFD(event);
                if (efd == ac_fd) { // acceptor
                    int cport, cfd, max = kMaxAcceptPerCall;
                    char cip[kNetIPLen];

                    while (max--) {
                        cfd = anetTcpAccept(err, ac_fd, cip, sizeof(cip), &cport);
                        if (cfd < 0) {
                            if (errno != EAGAIN) {
                                LIN_LOG_WARN("Accepting client connection: %s",
                                             err);
                            }
                            break;
                        }

                        r = el.Acquire(cfd, std::make_unique<Client>());
                        if (r != 0) {
                            close(cfd);
                            LIN_LOG_WARN("Cannot acquire the client's fd");
                            break;
                        } else {
                            anetNonBlock(nullptr, cfd);
                            anetEnableTcpNoDelay(nullptr, cfd);
                            anetKeepAlive(nullptr, cfd, kTCPKeepAlive);
                            el.AddEvent(cfd, kReadable);
                            LIN_LOG_DEBUG("Accepted %s:%d", cip, cport);
                        }
                    }
                } else { // processor
                    auto & resource = el.GetResource(efd);
                    if (EventLoop<>::IsEventReadable(event)) {

                    }
                    if (EventLoop<>::IsEventWritable(event)) {

                    }
                }
            }
        }
    }
}