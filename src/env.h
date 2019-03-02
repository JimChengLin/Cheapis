#pragma once
#ifndef CHEAPIS_ENV_H
#define CHEAPIS_ENV_H

#include <sys/time.h>

namespace cheapis {
    inline time_t GetCurrentTimeInSeconds() {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return tv.tv_sec;
    }
}

#endif //CHEAPIS_ENV_H