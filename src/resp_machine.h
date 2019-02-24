#pragma once
#ifndef RESPMACHINE_RESP_MACHINE_H
#define RESPMACHINE_RESP_MACHINE_H

#include <cstddef>
#include <string_view>

#include "autovector.h"

class RespMachine {
public:
    enum State {
        kError = -1,
        kInvalidMultiBulkLengthError = kError - 1,
        kDollarSignNotFoundError = kError - 2,
        kInvalidBulkLength = kError - 3,
        kSuccess = 0,
        kInit,
        kProcess,
    };

    enum ReqType {
        kInline,
        kMultiBulk,
        kUnknown,
    };

public:
    size_t Input(const char * s, size_t n);

    State GetState() const { return state_; }

    const rocksdb::autovector<std::string_view> &
    GetArgv() const { return argv_; }

    void Reset();

public:
    static void AppendSimpleString(std::string * buf, const char * s, size_t n);

    static void AppendError(std::string * buf, const char * s, size_t n);

    static void AppendInteger(std::string * buf, long long ll);

    static void AppendBulkString(std::string * buf, const char * s, size_t n);

    static void AppendArrayLength(std::string * buf, long long len);

    static void AppendNullBulkString(std::string * buf);

    static void AppendNullArray(std::string * buf);

private:
    size_t ProcessInlineInput(const char * s, size_t n);

    size_t ProcessMultiBulkInput(const char * s, size_t n);

private:
    State state_ = kInit;
    ReqType req_type_ = kUnknown;
    rocksdb::autovector<std::string_view> argv_;
    int multi_bulk_len_ = 0;
    int bulk_len_ = -1;
};

#endif //RESPMACHINE_RESP_MACHINE_H