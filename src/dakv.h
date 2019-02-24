#pragma once
#ifndef CHEAPIS_DAKV_H
#define CHEAPIS_DAKV_H

#include <map>

#include "slice.h"

namespace cheapis {
    using namespace sgt;

    class DaKV {
    public:
        int Get(const Slice & k, std::string * v) const {
            auto it = engine_.find(k);
            if (it == engine_.cend()) {
                return -1;
            }
            if (v != nullptr) {
                *v = it->second;
            }
            return 0;
        }

        int Set(const Slice & k, const Slice & v) {
            engine_[k.ToString()] = v.ToString();
            return 0;
        }

        int Del(const Slice & k) {
            auto it = engine_.find(k);
            if (k == it->first) {
                engine_.erase(it);
                return 0;
            }
            return -1;
        }

    private:
        std::map<std::string, std::string, SliceComparator> engine_;
    };
}

#endif //CHEAPIS_DAKV_H