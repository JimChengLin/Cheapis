#pragma once
#ifndef CHEAPIS_ENV_H
#define CHEAPIS_ENV_H

#include <ctime>

namespace cheapis {
    inline long GetCurrentTimeInSeconds() {
        return time(nullptr);
    }
}

#endif //CHEAPIS_ENV_H