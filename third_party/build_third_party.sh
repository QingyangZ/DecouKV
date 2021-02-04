#!/bin/bash
PROJ_PATH=$(pwd)
PROJ_PATH=${PROJ_PATH%YCSB-C*}YCSB-C
cd ${PROJ_PATH}/third_party/googletest && mkdir -p build && cd build && cmake .. && make -j
cd ${PROJ_PATH}/third_party/yaml-cpp && mkdir -p build && cd build && cmake .. && make -j


