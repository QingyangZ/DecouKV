# DecouKV Artifact

This repository contains the artifact for the paper **"Mitigating Resource Usage Dependency in Sorting-based KV Stores on Hybrid Storage Devices via Operation Decoupling"**, accepted at USENIX ATC '25.

## Overview

This artifact allows reviewers to reproduce the main experimental results of the paper, including:

1. DecouKV successfully improves CPU utilization compared to other systems and reduces utilization fluctuations under write-intensive workloads (Load, YCSB-A).
2. DecouKV achieves higher throughput than other systems under write-intensive workloads (Load, YCSB-A, -F); it also provides moderate improvements under read- or scan-intensive workloads (YCSB-B, -C, -D, -E).
3. DecouKV successfully reduces average latency compared to other systems under write-intensive workloads (Load, YCSB-A, -F);  it also brings slight improvements under read- or scan-intensive workloads (YCSB-B, -C, -D, -E).

Experiments are conducted on four key-value store systems, including RocksDB, MatrixKV, ADOC and DecouKV.

## Quick Start
**To Clone the repository**

```bash
git clone https://github.com/your-org/decoukv.git
cd decoukv
```

**To init submodules**

```
$ git submodule init
$ git submodule update
```
**To install packages**

```
$ sudo apt install libsnappy-dev libgflags-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev libpthread-stubs0-dev libnuma-dev libstdc++-dev libpmem-dev liburing-dev
```

**To run test**
1. **Configure Fast (PMEM) and Slow (SSD) Device Paths**

Please edit the `test.sh` script before running.
Specifically, modify line 7 and 8 to set the correct paths:
    * Set `db_path` to the directory on your SSD (slow device)
    * Set `pmem_path` to the directory on your PMEM (fast device)

2. **Run all benchmarks and generate CPU utilization plots**:

```bash
nohup ./test.sh > test.log 2>&1 &
```
This will:
    - Compile all four systems

    - Run **Load**, **YCSB-A to F** workloads

    - Output CPU utilization fluctuation plot (Figure 11) into `./test_results/`

This step may take over 24 hours to complete. You can monitor the script’s progress in the test.log file.
Reducing the data volume (default: 100GB) can shorten the runtime, but may affect the final results.

3. **Generate throughput latency figures**:

```bash
./draw_throughput.sh   # Generates Figure 14
```

4. **Generate latency figures**:

```bash
./draw_latency.sh      # Generates Figure 13
```

5. **Clean Up Experimental Data and Build Files**

```bash
./clean.sh 
```

## Running Other Experiments

Other experiments in the paper can be reproduced by modifying the following parameters:

* In `test.sh`:
You can adjust the number of threads, selected systems, number of CPU cores, and the YCSB workloads to run.

* In the `workload/` directory:
Each YCSB workload file can be modified to change parameters such as:

    -Database size (recordcount)

    -Number of operations (operationcount)

    -Read/write ratio

    -Key distribution (Uniform of Zipfian)

* In `db_config.yaml`:
System-level configurations can be customized, including:

    -SSTable size

    -MemTable size

    -And other internal parameters

These options provide flexibility to explore additional workloads or system behaviors beyond the default setup.