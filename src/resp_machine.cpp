#include <cassert>
#include <string>

#include "resp_machine.h"
#include "util.h"

size_t RespMachine::Input(const char * s, size_t n) {
    state_ = kProcess;
    if (req_type_ == kUnknown) {
        req_type_ = (*s == '*' ? kMultiBulk : kInline);
    }
    if (req_type_ == kMultiBulk) {
        return ProcessMultiBulkInput(s, n);
    } else {
        assert(req_type_ == kInline);
        return ProcessInlineInput(s, n);
    }
}

size_t RespMachine::ProcessInlineInput(const char * s, size_t n) {
    std::string_view sv(s, n);

    /* Search for end of line */
    auto pos = sv.find('\n');

    /* Nothing to do without a \r\n */
    if (pos == std::string::npos) {
        return 0;
    }
    size_t consume_len = pos + 1;

    sv = {s, pos};
    /* Handle the \r\n case. */
    if (!sv.empty() && sv.back() == '\r') {
        sv.remove_suffix(1);
    }

    pos = 0;
    while (true) {
        auto next_pos = sv.find(' ', pos);
        if (next_pos == std::string::npos) {
            argv_.emplace_back(sv.data() + pos, sv.size() - pos);
            break;
        }
        argv_.emplace_back(sv.data() + pos, next_pos - pos);
        pos = next_pos + 1;
    }
    state_ = kSuccess;
    return consume_len;
}

size_t RespMachine::ProcessMultiBulkInput(const char * s, size_t n) {
    std::string_view sv(s, n);

    size_t consume_len = 0;
    if (multi_bulk_len_ == 0) {
        /* Multi bulk length cannot be read without a \r\n
         * Buffer should also contain \n */
        auto pos = sv.find("\r\n");
        if (pos == std::string::npos) {
            return 0;
        }

        /* skip prefix */
        sv = {s + 1, pos - 1};
        long long ll;
        int ok = string2ll(sv.data(), sv.size(), &ll);
        if (!ok) {
            state_ = kInvalidMultiBulkLengthError;
            return 0;
        }
        consume_len = pos + 2;

        if (ll <= 0) {
            state_ = kSuccess;
            return consume_len;
        }
        multi_bulk_len_ = static_cast<int>(ll);
    }

    while (multi_bulk_len_ != 0) {
        assert(multi_bulk_len_ > 0);

        /* Read bulk length if unknown */
        if (bulk_len_ == -1) {
            sv = {s + consume_len, n - consume_len};
            auto pos = sv.find("\r\n");
            if (pos == std::string::npos) {
                return consume_len;
            }

            sv = {sv.data(), pos};
            if (sv.front() != '$') {
                state_ = kDollarSignNotFoundError;
                return 0;
            }
            sv.remove_prefix(1);

            long long ll;
            int ok = string2ll(sv.data(), sv.size(), &ll);
            if (!ok || ll < 0) {
                state_ = kInvalidBulkLength;
                return 0;
            }

            consume_len += pos + 2;
            bulk_len_ = static_cast<int>(ll);
        }

        /* Read bulk argument */
        sv = {s + consume_len, n - consume_len};
        int bulk_read_len = bulk_len_ + 2; /* +2 == trailing \r\n */
        if (sv.size() < bulk_read_len) {
            break;
        } else {
            argv_.emplace_back(sv.data(), bulk_len_);
            consume_len += bulk_read_len;
            bulk_len_ = -1;
            --multi_bulk_len_;
        }
    }

    if (multi_bulk_len_ == 0) {
        state_ = kSuccess;
    }
    return consume_len;
}

void RespMachine::Reset() {
    state_ = kInit;
    req_type_ = kUnknown;
    argv_.clear();
    multi_bulk_len_ = 0;
    bulk_len_ = -1;
}

void RespMachine::AppendSimpleString(std::string * buf, const char * s, size_t n) {
    buf->push_back('+');
    buf->append(s, n);
    buf->append("\r\n");
}

void RespMachine::AppendError(std::string * buf, const char * s, size_t n) {
    buf->push_back('-');
    buf->append(s, n);
    buf->append("\r\n");
}

void RespMachine::AppendInteger(std::string * buf, long long ll) {
    buf->push_back(':');
    char lls[32];
    int lls_len = ll2string(lls, sizeof(lls), ll);
    buf->append(lls, static_cast<size_t>(lls_len));
    buf->append("\r\n");
}

void RespMachine::AppendBulkString(std::string * buf, const char * s, size_t n) {
    buf->push_back('$');
    char lls[32];
    int lls_len = ll2string(lls, sizeof(lls), static_cast<long long>(n));
    buf->append(lls, static_cast<size_t>(lls_len));
    buf->append("\r\n");
    buf->append(s, n);
    buf->append("\r\n");
}

void RespMachine::AppendArrayLength(std::string * buf, long long len) {
    buf->push_back('*');
    char lls[32];
    int lls_len = ll2string(lls, sizeof(lls), len);
    buf->append(lls, static_cast<size_t>(lls_len));
    buf->append("\r\n");
}

void RespMachine::AppendNullBulkString(std::string * buf) {
    buf->append("$-1\r\n");
}

void RespMachine::AppendNullArray(std::string * buf) {
    buf->append("*-1\r\n");
}