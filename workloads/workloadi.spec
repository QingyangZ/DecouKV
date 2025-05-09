# Nutanix production workloads. 
# Yahoo! Cloud System Benchmark
# Workload H: insert
#   Application example: user database, where user records are read and modified by the user or to record user activity.
#                        
#   Read/read-modify-write ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=100000000
operationcount=100000000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0.41
updateproportion=0.57
scanproportion=0.2
insertproportion=0
readmodifywriteproportion=0

requestdistribution=zipfian

maxscanlength=100

scanlengthdistribution=uniform

