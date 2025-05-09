#! /bin/bash

cd ~/zqy/YCSB-C/build
rm -r /mnt/sdc/zqy/rocksdb/100GB_index
mkdir /mnt/sdc/zqy/rocksdb/100GB_index
rm -r /mnt/pmem0/zqy/ycsb/rocksdb/blob
mkdir /mnt/pmem0/zqy/ycsb/rocksdb/blob
nohup taskset -c 11-14 ./ycsbc -db rocksdb -dbpath /mnt/sdc/zqy/rocksdb/100GB_index -P ../workloads/load.spec -threads 16 -skipload false > /mnt/sdc/zqy/rocksdb/result_test/100GB_core4_index_load_uniform.log 2>&1
# cp -r /mnt/sdc/zqy/rocksdb/100GB_index /mnt/sde/zqy/index/100GB_index 
# cp -r /mnt/pmem0/zqy/ycsb/rocksdb/blob /mnt/pmem0/zqy/ycsb/index/blob