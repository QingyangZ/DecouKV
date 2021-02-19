#!/bin/bash
PROJ_PATH=$(pwd)
CORE_NUM=cat /proc/cpuinfo| grep "cpu cores"| uniq
cd ${PROJ_PATH}/leveldb && mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build . -j ${CORE_NUM}
cd ${PROJ_PATH}/rocksdb && mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build . -j ${CORE_NUM}



