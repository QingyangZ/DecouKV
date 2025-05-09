import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np

# 读取CSV文件
file_path = './test_results/ycsb_latency.csv'
data = pd.read_csv(file_path)

# 将表格的行列进行翻转
data_transposed = data.set_index('Sys').transpose()

# 设置图像格式和参数
plt.rcParams['xtick.direction'] = 'in'
plt.rcParams['ytick.direction'] = 'in'

matplotlib.rc('pdf', fonttype=42)
fig, ax = plt.subplots(figsize=(10, 4))
# fig, ax = plt.subplots()
matplotlib.rcParams['savefig.dpi'] = 10
matplotlib.rcParams['figure.subplot.left'] = 0
matplotlib.rcParams['figure.subplot.bottom'] = 0
matplotlib.rcParams['figure.subplot.right'] = 1
matplotlib.rcParams['figure.subplot.top'] = 1

font_of_legend = {'family': 'DejaVu Sans', 'weight': 'normal', 'size': 20}
font_of_tick = {'family': 'DejaVu Sans', 'weight': 'normal', 'size': 20}
font_of_label = {'family': 'DejaVu Sans', 'weight': 'normal', 'size': 20}

# 获取横坐标标签
data_labels = data_transposed.index.tolist()
x = np.arange(len(data_labels))

# 设置柱状图的颜色、边框颜色和填充样式
colors = ['none', 'grey', 'none', 'black']
hatchcolors = ['#F66F69', '#FFC24B', 'black', '#000000']
hatchs = ['', '', '||', '']
width = 0.14

# 绘制柱状图
for i, column in enumerate(data_transposed.columns):
    color_idx = i % len(hatchcolors)  # 确保索引不超出范围
    ax.bar(x + (i - 1.5) * width, data_transposed[column], width, label=column,
           color=colors[i], edgecolor='black', hatch=hatchs[color_idx], lw=1, zorder=0)
    ax.bar(x + (i - 1.5) * width, data_transposed[column], width,
           color='none', edgecolor='black', hatch='')

# 设置标签并调整标签与轴刻度的距离
# ax.set_xlabel('Workload', font=font_of_label, labelpad=10)
ax.set_ylabel('Throughput (KOPS)', font=font_of_label, labelpad=0)
# ax.set_title('YCSB Workload Throughput', font=font_of_label)

# 设置x轴和y轴的范围
ax.set_ylim(0, data_transposed.max().max() * 1.08)

# 设置刻度的字体
plt.xticks(x, labels=data_labels, font=font_of_tick, rotation=0)
plt.yticks(font=font_of_tick)

# 设置图例并居中显示，向下移动一点避免遮挡标题
# plt.legend(prop=font_of_legend, loc='upper center', bbox_to_anchor=(0.5, 1.2), ncol=len(data_transposed.columns), handletextpad=0.1, columnspacing=0.2)
plt.legend(
    prop=font_of_legend,
    loc='upper center',  # 设置图例位置
    bbox_to_anchor=(0.5, 1.2),  # 将图例放置在顶部中心
    ncol=len(data_transposed.columns),  # 设置列数
    handletextpad=0.1,  # 设置图例标签与图例符号之间的间距
    columnspacing=0.5,  # 设置列之间的间距
    frameon=False,  # 显示边框
    # edgecolor='black',  # 设置整个图例的边框颜色
    # linewidth=1,  # 设置边框的线宽
    handlelength=1,  # 设置每个图例项的长度
    # handleheight=2,  # 设置每个图例项的高度
    # handleedgewidth=2,  # 设置每个图例符号的边框宽度
    # handleedgecolor='black'  # 设置每个图例符号的边框颜色
)
# 设置网格线
# plt.grid(linestyle='--', axis='y')

# 保存图形为 PDF 格式
plt.savefig('./test_results/ycsb_latency.pdf', format='pdf', bbox_inches='tight', dpi=300)

# 显示图形
plt.show()