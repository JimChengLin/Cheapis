#pragma once
#ifndef CHEAPIS_LOG_H
#define CHEAPIS_LOG_H

#include <cstdio>
#include <ctime>
#include <sys/time.h>

#define LIN_LOG_IMPL(level, file, line, function, fmt, args...)              \
    do {                                                                     \
        struct timeval tv = {0};                                             \
        gettimeofday(&tv, nullptr);                                          \
        time_t t = tv.tv_sec;                                                \
        struct tm curr_tm = {0};                                             \
        localtime_r(&t, &curr_tm);                                           \
        long usec = tv.tv_usec;                                              \
        printf(                                                              \
            "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] %-5s %s:%d:%s " fmt "\n", \
            curr_tm.tm_year + 1900, curr_tm.tm_mon + 1, curr_tm.tm_mday,     \
            curr_tm.tm_hour, curr_tm.tm_min, curr_tm.tm_sec, usec,           \
            level, file, line, function, ##args);                            \
    } while (false)

#if !defined(NDEBUG)
#define LIN_DEBUG
#endif

#define LIN_INFO
#define LIN_WARN
#define LIN_ERROR

#if defined(LIN_DEBUG)
#define LIN_LOG_DEBUG(fmt, args...) LIN_LOG_IMPL("DEBUG", __FILE__, __LINE__, __FUNCTION__, fmt, ##args)
#else
#define LIN_LOG_DEBUG(fmt, args...)
#endif

#if defined(LIN_INFO)
#define LIN_LOG_INFO(fmt, args...) LIN_LOG_IMPL("INFO", __FILE__, __LINE__, __FUNCTION__, fmt, ##args)
#else
#define LIN_LOG_INFO(fmt, args...)
#endif

#if defined(LIN_WARN)
#define LIN_LOG_WARN(fmt, args...) LIN_LOG_IMPL("WARN", __FILE__, __LINE__, __FUNCTION__, fmt, ##args)
#else
#define LIN_LOG_WARN(fmt, args...)
#endif

#if defined(LIN_ERROR)
#define LIN_LOG_ERROR(fmt, args...) LIN_LOG_IMPL("ERROR", __FILE__, __LINE__, __FUNCTION__, fmt, ##args)
#else
#define LIN_LOG_ERROR(fmt, args...)
#endif

#endif //CHEAPIS_LOG_H