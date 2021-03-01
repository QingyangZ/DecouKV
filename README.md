# YCSB-C ![build and test](https://github.com/ZacharyLiu-CS/YCSB-C/workflows/build%20and%20test/badge.svg)

Yahoo! Cloud Serving Benchmark in C++, a C++ version of YCSB (https://github.com/brianfrankcooper/YCSB/wiki)

## Quick Start
To init submodules

```
$ git submodules init
$ git submodules update
```
To install packages

```
$ sudo apt install libsnappy-dev libgflags-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev libpthread-stubs0-dev
```

To build third-party and db imples
```
$ cd third-party && ./build_third_party.sh
$ cd db_impl && ./build_db_impl.sh
```

To build YCSB-C on Ubuntu, for example:

```
$ mkdir -p build && cd build
$ cmake -DCMAKE_BUILD_TYPE=Release ..
$ cmake --build . -j
```

To run test:

```
$ cd build && ctest
```

Run Workload A with a leveldb
implementation of the database, for example:
```
$ cd build
$ ./ycsbc -db leveldb -dbpath . -P ../workloads/workloada.spec -threads 4
```

Note that we do not have load and run commands as the original YCSB. Specify
how many records to load by the recordcount property. Reference properties
files in the workloads dir.

