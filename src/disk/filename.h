#pragma once
#ifndef CHEAPIS_FILENAME_H
#define CHEAPIS_FILENAME_H

#include <string>

namespace cheapis {
    inline void DataFilename(const std::string & dir, uint64_t id, std::string * name) {
        name->assign(dir);
        name->append("/cheapis-dakv-");
        name->append(std::to_string(id));
        name->append(".data");
    }

    inline void IndexFilename(const std::string & dir, std::string * name) {
        name->assign(dir);
        name->append("/cheapis-dakv.index");
    }
}

#endif //CHEAPIS_FILENAME_H