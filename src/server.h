#pragma once
#ifndef CHEAPIS_SERVER_H
#define CHEAPIS_SERVER_H

#include <string>

#include "gujia.h"
#include "gujia_impl.h"
#include "resp_machine.h"

namespace cheapis {
    using namespace gujia;

    int ServerMain();

    struct Client {
        RespMachine resp;
        std::string input;
        std::string output;
        long last_mod_time;
        unsigned int ref_count = 0;
        unsigned int consume_len = 0;
        bool close = false;

        explicit Client(long last_mod_time = -1)
                : last_mod_time(last_mod_time) {}
    };
}

#endif //CHEAPIS_SERVER_H