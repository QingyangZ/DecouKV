import subprocess
import psutil
import threading
import time 

class CPUMonitor:
    def __init__(self,process_name) -> None:
        self.process_name = process_name
        self.process_id = self._get_processid_by_name()
        if self.process_id != -1:
            self.process = psutil.Process(self.process_id)
        
    def _get_processid_by_name(self) -> int:
        for proc in psutil.process_iter():
            if proc.name() == self.process_name:
                print(proc.pid)
                return proc.pid
        return -1
        
    def get_cpu_usage(self) -> float:
        if self.process_id == -1:
            self.__init__(self.process_name)
            return 0
        if self.process.is_running() == False:
           return 0
        cpu_usage = self.process.cpu_percent()
        return cpu_usage

class DiskMonitor:
    def __init__(self, mount_path, disk_dev, interval_seconds) -> None:
        self.mount_path = mount_path
        self.disk_dev = disk_dev
        self.interval_seconds = interval_seconds
        self.disk = psutil.disk_io_counters(perdisk=True)[self.disk_dev]


    def get_disk_space(self):
        disk_usage = psutil.disk_usage(self.mount_path)
        disk_space = disk_usage.used / 1024**3  
        return "{:.2f}".format(disk_space)

    def get_disk_utilization(self):
        tmp = psutil.disk_io_counters(perdisk=True)[self.disk_dev]
        write_bytes = tmp.write_bytes - self.disk.write_bytes
        read_bytes = tmp.read_bytes - self.disk.read_bytes
        self.disk = tmp
        convert = lambda x: "{:.2f}".format( x  / (self.interval_seconds * 1024*1024))
        write_speed = convert(write_bytes) 
        read_speed =  convert(read_bytes)
        return write_speed, read_speed


# Event for signaling main thread
#start_event = threading.Event()
#end_event = threading.Event()
#file_contents = ["Empty","Started", "Ended"]

# Background thread function
#def check_cycle(file_path):
#    while True:
#        with open(file_path, "r") as f:
#            content = f.readline().strip()
#        start_cycle = content == file_contents[1] 
#        end_cycle = content == file_contents[2]
        # Signal main thread  if cycle detected     
#        if start_cycle:
#            start_event.set()  
            
#        if end_cycle:
#            end_event.set()
#            break
#        time.sleep(1)

def is_process_running(process_name):
    """检查是否存在名为 process_name 的进程"""
    for proc in psutil.process_iter(['name']):
        if process_name in proc.info['name']:
            return True
    return False

def main(): 
    nowTime = lambda:int(round(time.time() * 1000))
    start_time = nowTime()

    cpu = CPUMonitor(process_name="ycsbc")
    disk = DiskMonitor(mount_path="/mnt/sdc", disk_dev="sdc", interval_seconds=1)

    #threading.Thread(target=check_cycle, args=["/mnt/sdc/tht/test2.sh"]).start()
    time.sleep(0.5)
    with open("/mnt/sde/zqy/data/sde/adoc/load+a_4core.txt", 'w') as f:
        f.write('"Time(s)" "CPU(%)" "Storage Size(GB)" "Write Speed(MB/s)" "Read Speed(MB/s)"\n')
    #if not end_event.is_set():
    #    start_event.wait()
    while 1:#not end_event.is_set():
        interval_time = round((nowTime()-start_time)/1000)
        cpu_percentage = cpu.get_cpu_usage()
        disk_space = disk.get_disk_space()
        disk_write_sped, disk_read_speed = disk.get_disk_utilization()
        with open("./test_results/cpu_utilization.txt", 'a') as f:
            f.write("{}\t   {}\t   {}\t    {}\t   {}\n".format(interval_time, cpu_percentage, disk_space, disk_write_sped, disk_read_speed))
        time.sleep(0.9604)
        if not is_process_running("ycsbc"):
            break
        
if __name__ == "__main__":
    main() 