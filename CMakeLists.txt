cmake_minimum_required(VERSION 3.8)
project(Cheapis)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -Wall")

include_directories(sig_tree/src)

add_executable(Cheapis main.cpp
        src/anet.c
        src/anet.h
        src/cheapis.cpp
        src/cheapis.h
        src/dakv.cpp
        src/dakv.h
        src/env.cpp
        src/env.h
        src/fmacros.h
        src/gujia.h
        src/gujia_impl.h
        src/resp_machine.cpp
        src/resp_machine.h
        src/util.c
        src/util.h)