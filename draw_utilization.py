import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import argparse

parser = argparse.ArgumentParser(description="Monitor CPU usage of a DB benchmark process.")
parser.add_argument("test_db", type=str, choices=["RocksDB", "MatrixKV", "ADOC", "DecouKV"],
                    help="Database type to test (e.g., rocksdb, matrixkv)")
parser.add_argument("workload", type=str, help="Workload name (e.g., a, b, ycsb-long)")
parser.add_argument("core_num", type=float, help="Number of CPU cores used for the test")

args = parser.parse_args()

# 读取CSV文件
data = pd.read_csv(f"./test_results/cpu_utilization_{args.test_db}_{args.workload}.txt", delim_whitespace=True)

core_num = args.core_num
start_point = 0
# 将"CPU(%)"列除以10
data['CPU(%)'] = data['CPU(%)'] / core_num
# data['Combined Speed'] = (data['Write Speed(MB/s)'] + data['Read Speed(MB/s)']) / 4.6

# 找到x值大于或等于20000的第一个点
start_index = data[data['Time(s)'] >= start_point].index[0]

# 取从这个点开始的100个点
subset = data.loc[start_index:start_index+1550]

# 将横坐标减去20000
subset['Time(s)'] = subset['Time(s)'] - start_point

# 设置图像格式和参数
matplotlib.rc('pdf', fonttype=42)
font_of_legend = {'family': 'DejaVu Sans', 'weight': 'normal', 'size': 22}
font_of_tick = {'family': 'DejaVu Sans', 'weight': 'normal', 'size': 24}
font_of_label = {'family': 'DejaVu Sans', 'weight': 'normal', 'size': 24}

# 设置图像大小为长条形
plt.figure(figsize=(5, 4))

# 计算CPU利用率的平均值
cpu_avg = subset['CPU(%)'].mean()
cpu_std = subset['CPU(%)'].std()
cpu_range = subset['CPU(%)'].max() - subset['CPU(%)'].min()

# 计算磁盘利用率的平均值
# disk_avg = subset['Combined Speed'].mean()
# print(disk_avg)

# 绘制水平虚线，表示磁盘利用率的平均值
# plt.axhline(disk_avg, color='black', linestyle='--', linewidth=5, label=f'Disk Avg: {disk_avg:.1f}')


# 绘制图形
plt.plot(subset['Time(s)'], subset['CPU(%)'], color='#C43302', linewidth=0.8)
# 绘制水平虚线，表示CPU利用率的平均值
plt.axhline(cpu_avg, color='black', linestyle='--', linewidth=3, label=f'Avg: {cpu_avg:.1f}  Rng: {cpu_range:.1f}')

# 设置标签并调整标签与轴刻度的距离
plt.xlabel('Elapsed Time (Sec)', font=font_of_label, labelpad=0)  # labelpad 控制横坐标标签与刻度的距离
plt.ylabel('CPU Utilization(%)', font=font_of_label, labelpad=0)  # 同样通过 labelpad 调整纵坐标标签与刻度的距离
# plt.title('CPU Utilization', font=font_of_label)

# 设置x轴和y轴的范围
plt.xlim(0, 1501)
plt.ylim(0, 100)

# 设置y轴的刻度为0到100，步长为20
plt.yticks(range(0, 101, 20), font=font_of_tick)
plt.xticks(range(0, 1501, 500), font=font_of_tick)
plt.xticks(font=font_of_tick)

# 调整图像底部和左右边距，确保标签显示不被截断
plt.subplots_adjust(left=0.20, bottom=0.20, right=0.92, top=0.95)

plt.legend(prop=font_of_label, frameon=False, bbox_to_anchor=(0.47, -0.090), handlelength=0, loc='lower center')
# 保存图像为 PDF
plt.savefig(f"./test_results/cpu_utilization_{args.test_db}_{args.workload}.png", format='png', bbox_inches='tight', dpi=300)

plt.show()