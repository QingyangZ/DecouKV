#!/bin/bash
cd ./googletest && mkdir -p build && cd build && cmake .. && make -j
cd ../..
cd ./yaml-cpp && mkdir -p build && cd build && cmake .. && make -j


