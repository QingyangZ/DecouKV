#!/bin/bash
PROJ_PATH=$(pwd)
cd ${PROJ_PATH}/leveldb && mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build . -j
cd ${PROJ_PATH}/rocksdb && mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build . -j


