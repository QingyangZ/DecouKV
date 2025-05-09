#! /bin/bash
rm -rf ./test_results/*
cd ./db_impl/rocksdb/build
make clean
cd ../../../db_impl/adoc
make clean
cd ../../db_impl/decoukv/build
make clean
cd ../../../db_impl/matrixkv
make clean
cd ../../build
make clean