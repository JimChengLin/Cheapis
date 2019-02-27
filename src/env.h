#pragma once
#ifndef CHEAPIS_ENV_H
#define CHEAPIS_ENV_H

#include <ctime>
#include <sys/time.h>

namespace cheapis {
    inline time_t GetCurrentTimeInSeconds() {
        return time(nullptr);
    }

    inline suseconds_t GetCurrentMicroseconds() {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return tv.tv_usec;
    }

    inline struct tm GetLocalTime() {
        struct tm curr_tm;
        time_t t = time(nullptr);
        localtime_r(&t, &curr_tm);
        return curr_tm;
    }
}

#endif //CHEAPIS_ENV_H