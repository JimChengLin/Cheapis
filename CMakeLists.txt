cmake_minimum_required(VERSION 3.8)
project(Cheapis)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -flto -Wall")

include_directories(sig_tree/src)

add_executable(Cheapis main.cpp
        src/anet.c
        src/anet.h
        src/env.cpp
        src/env.h
        src/executor.h
        src/executor_disk_impl.cpp
        src/executor_mem_impl.cpp
        src/fmacros.h
        src/gujia.h
        src/gujia_impl.h
        src/log.h
        src/resp_machine.cpp
        src/resp_machine.h
        src/server.cpp
        src/server.h
        src/util.c
        src/util.h)