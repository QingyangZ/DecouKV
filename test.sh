#! /bin/bash
threads_num=64
# test_db="rocksdb" # rocksdb, decoukv, matrixkv, adoc
systems=("RocksDB" "MatrixKV" "ADOC" "DecouKV")
core_num=8
# ycsb_workload="workloada" # workloada, workloadb, workloadc, workloadd, workloade, workloadf
db_path="/mnt/sdc/zqy/test_data" # SSD directory
pmem_path="/mnt/pmem0/test" # pmem path
workloads=("workloada" "workloadb" "workloadc" "workloadd" "workloade" "workloadf")

mkdir -p ./test_results
throughput_file="./test_results/ycsb_throughput.csv"
latency_file="./test_results/ycsb_latency.csv"
echo "Sys,Load,A,B,C,D,E,F" > "$throughput_file"
echo "Sys,Load,A,B,C,D,E,F" > "$latency_file"

mkdir -p build
for test_db in "${systems[@]}"
do
  if [ "$test_db" = "RocksDB" ]; then
    mkdir -p ./db_impl/rocksdb/build
    cd ./db_impl/rocksdb/build
    cmake -DCMAKE_BUILD_TYPE=Release .. && make -j 32
    cd ../../../build
  elif [ "$test_db" = "DecouKV" ]; then
    mkdir -p ./db_impl/decoukv/build
    cd ./db_impl/decoukv/build
    cmake -DCMAKE_BUILD_TYPE=Release .. && make -j 32
    cd ../../../build
  elif [ "$test_db" = "MatrixKV" ]; then
    cd ./db_impl/matrixkv
    make static_lib -j 32
    cd ../../build
  elif [ "$test_db" = "ADOC" ]; then
    cd ./db_impl/adoc
    make static_lib -j 32
    cd ../../build
  fi
  echo "complete $test_db successfully"

  cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_DB=$test_db .. && make -j 32
  echo "complete ycsb successfully"

  throughput_row="$test_db"
  latency_row="$test_db"
  load_recorded=0

  for ycsb_workload in "${workloads[@]}"
    do
    echo "$test_db loading"
    python3 ../resource_monitor.py $test_db load &
    MONITOR_PID=$!
    # Load data
    nohup taskset -c 1-$core_num ./ycsbc -db rocksdb -dbpath $db_path -P ../workloads/load.spec -threads $threads_num -skipload false > ../test_results/${test_db}_core${core_num}_threads${threads_num}_load.log 2>&1
    kill $MONITOR_PID
    echo "$test_db loading complete"

    if [ $load_recorded -eq 0 ]; then
      load_log_file="../test_results/${test_db}_core${core_num}_threads${threads_num}_load.log"
      load_throughput=$(grep "Throughput:" "$load_log_file" | awk '{for(i=1;i<=NF;i++) if($i=="Throughput:") { printf("%.3f", $(i+1)/1000); exit }}') 
      load_throughput=${load_throughput:-0.000}
      load_latency=$(grep "Average:" "$load_log_file" | awk '{for(i=1;i<=NF;i++) if($i=="Average:") { printf("%.3f", $(i+1)/1000); exit }}')
      load_latency=${load_latency:-0.000}
      throughput_row="${throughput_row},${load_throughput}"
      latency_row="${latency_row},${load_latency}"
      load_recorded=1
    fi

    echo "start $test_db $ycsb_workload test"
    python3 ../resource_monitor.py $test_db $ycsb_workload &
    MONITOR_PID=$!
    # Run test
    nohup taskset -c 1-$core_num ./ycsbc -db rocksdb -dbpath $db_path -P ../workloads/$ycsb_workload.spec -threads $threads_num -skipload true > ../test_results/${test_db}_core${core_num}_threads${threads_num}_${ycsb_workload}_run.log 2>&1
    kill $MONITOR_PID
    echo "$test_db $ycsb_workload test complete"

    test_log_file="../test_results/${test_db}_core${core_num}_threads${threads_num}_${ycsb_workload}_run.log"
    throughput=$(grep "Throughput:" "$test_log_file" | awk '{for(i=1;i<=NF;i++) if($i=="Throughput:") { printf("%.3f", $(i+1)/1000); exit }}')
    throughput=${throughput:-0.000}
    latency=$(grep "Average:" "$test_log_file" | awk '{for(i=1;i<=NF;i++) if($i=="Average:") { printf("%.3f", $(i+1)/1000); exit }}')
    latency=${latency:-0.000}
    throughput_row="${throughput_row},${throughput}"
    latency_row="${latency_row},${latency}"

    [ -d "$db_path" ] && rm -rf "$db_path"
    [ -d "$pmem_path" ] && rm -rf "$pmem_path"
    
  done
  cd ..
  echo "$throughput_row" >> "$throughput_file"
  echo "$latency_row" >> "$latency_file"
  echo "complete $test_db throughput and latency"

  # drawing utilization
  python3 draw_utilization.py $test_db load $core_num
  python3 draw_utilization.py $test_db workloada $core_num
  echo "draw utilization complete"
done

# rm -r /mnt/sdc/zqy/test
# rm -r /mnt/pmem0/zqy/ycsb/rocksdb/rocksdb
# rm -r /mnt/sde/zqy/rocksdb_4K
# rm -r /mnt/pmem0/zqy/ycsb/rocksdb/rocksdb_4K

# cd ~/zqy/YCSB-C/build
# nohup numactl --cpunodebind=0 --physcpubind=50,52,54,56 ./ycsbc -db rocksdb -dbpath /mnt/sdc/zqy/test -P ../workloads/workloada.spec -threads 64 -skipload false > /mnt/sde/zqy/data/sde/index_zipfian/500GB_core4_load+a.log 2>&1
# cp -r /mnt/sdc/zqy/test /mnt/sde/zqy/rocksdb_4K
# cp -r /mnt/pmem0/zqy/ycsb/rocksdb/rocksdb /mnt/pmem0/zqy/ycsb/rocksdb/rocksdb_4K
# rm -r /mnt/sdc/zqy/test
# rm -r /mnt/pmem0/zqy/ycsb/rocksdb/rocksdb
# cp -r /mnt/sde/zqy/rocksdb_4K /mnt/sdc/zqy/test
# cp -r /mnt/pmem0/zqy/ycsb/rocksdb/rocksdb_4K /mnt/pmem0/zqy/ycsb/rocksdb/rocksdb

# for core_num in 48
# do
#   dir=$((core_num-40))
#   for i in i
#   do 
#     rm -r /mnt/sdc/zqy/test
#     rm -r /mnt/pmem0/zqy/ycsb/rocksdb/rocksdb
#     # cp -r /mnt/sde/zqy/rocksdb_4K /mnt/sdc/zqy/test
#     # cp -r /mnt/pmem0/zqy/ycsb/rocksdb/rocksdb_4K /mnt/pmem0/zqy/ycsb/rocksdb/rocksdb
#     # rm -r /mnt/pmem0/zqy/ycsb/rocksdb/blob
#     cd ~/zqy/YCSB-C/build
#     nohup taskset -c 41-$core_num ./ycsbc -db rocksdb -dbpath /mnt/sdc/zqy/test -P ../workloads/workload$i.spec -threads $threads_num -skipload false > /mnt/sde/zqy/data/sde/adoc_zipfian/100GB_core${dir}_load+${i}_1.log 2>&1
#     # nohup numactl --cpunodebind=0 --physcpubind=50,52,54,56 ./ycsbc -db rocksdb -dbpath /mnt/sdc/zqy/test -P ../workloads/workload$i.spec -threads $threads_num -skipload false > /mnt/sde/zqy/data/sde/index_zipfian/500GB_core${dir}_${i}.log 2>&1
#     # nohup taskset -c 11-$core_num ./ycsbc -db rocksdb -dbpath /mnt/sdc/zqy/rocksdb -P ../workloads/workload$i.spec -threads $threads_num -skipload false > /mnt/sde/zqy/data/index_4K/100GB_core${dir}_${i}_uniform.log 2>&1
#     # nohup taskset -c 11-$core_num ./ycsbc -db rocksdb -dbpath /mnt/sdc/tht/db -P ../workloads/workload$i.spec -threads $threads_num -skipload true > /mnt/sdc/zqy/rocksdb/result_5.1/100GB_core${dir}_rocksdb_load_uniform.log 2>&1
    
#     # rm -r /mnt/pmem0/zqy/ycsb/rocksdb/blob
#     # rm -r /mnt/sdc/zqy/rocksdb/100GB_index
#     # cp -r /mnt/sde/zqy/index/100GB_index /mnt/sdc/zqy/rocksdb/100GB_index
#     # rm -r /mnt/pmem0/zqy/ycsb/rocksdb/blob
#     # # mkdir /mnt/pmem0/zqy/ycsb/rocksdb/blob
#     # cp -r /mnt/pmem0/zqy/ycsb/index/blob /mnt/pmem0/zqy/ycsb/rocksdb/blob
#     # cd ~/zqy/YCSB-C/build
#     # nohup taskset -c 11-$core_num ./ycsbc -db rocksdb -dbpath /mnt/sdc/zqy/rocksdb/100GB_index -P ../workloads/workload$i.spec -threads $threads_num -skipload true > /mnt/sdc/zqy/rocksdb/result_5.2/100GB_core${dir}_index_${i}_uniform.log 2>&1
#     # nohup taskset -c 11-$core_num ./ycsbc -db rocksdb -dbpath /mnt/sdc/zqy/rocksdb/100GB_index -P ../workloads/workload$i.spec -threads $threads_num -skipload false > /mnt/sdc/zqy/rocksdb/result_3.3/100GB_core${dir}_index_load_uniform.log 2>&1
#   done
# done
