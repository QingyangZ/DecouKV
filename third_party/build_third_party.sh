#!/bin/bash
PROJ_PATH=$(pwd)
cd ${PROJ_PATH}/googletest && mkdir -p build && cd build && cmake .. && make -j
cd ${PROJ_PATH}/yaml-cpp && mkdir -p build && cd build && cmake .. && make -j


