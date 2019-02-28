#pragma once
#ifndef CHEAPIS_EXECUTOR_H
#define CHEAPIS_EXECUTOR_H

#include <memory>
#include <string>
#include <string_view>

#include "autovector.h"
#include "server.h"

namespace cheapis {
    class Executor {
    public:
        Executor() = default;

        virtual ~Executor() = default;

    public:
        virtual void Submit(const rocksdb::autovector<std::string_view> & in,
                            std::string * out,
                            int fd) = 0;

        virtual void Execute(size_t n, EventLoop<Client> * el) = 0;

        virtual size_t GetTaskCount() const = 0;
    };

    std::unique_ptr<Executor>
    OpenExecutorMem();

    std::unique_ptr<Executor>
    OpenExecutorDisk(const std::string & name);
}

#endif //CHEAPIS_EXECUTOR_H