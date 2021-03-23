from pandas import read_csv
from matplotlib import pyplot
import numpy as np


# load dataset
def drawplot():
    real = read_csv(r'data/historical_data/mrp_nvmeof_data/rawdata/35.csv')
    real_throughput = real['Goodput'].apply(lambda x: x / 100000000)

    big = read_csv(r'data/historical_data/mrp_nvmeof_data/rawdata/35.csv')
    big_throughput = big['network_throughput'].apply(lambda x: x / 100000000)

    # set font
    font = {
        'weight': 'normal',
        'size': 14,
    }

    pyplot.figure()
    pyplot.plot(real_throughput, color='red', label='num35_goodput')

    pyplot.plot(big_throughput, color='green', label='num35_throughput')
    pyplot.xlabel('Time interval 15s', font)
    pyplot.xticks(fontsize=10)
    pyplot.yticks(fontsize=10)
    pyplot.legend(loc='best', prop={'size': 14})
    pyplot.title('compare static num35 with num100')
    # pyplot.savefig(file_path + name + "_all.png",dpi = 300)
    pyplot.show()


def drawbar():
    real = read_csv('result/real-time_rsme.csv')
    op = read_csv('result/throughput_rsme.csv')

    real_rsme = [real['rmse'].iloc[0], real['rmse'].iloc[1], real['rmse'].iloc[2], real['rmse'].iloc[3],
                 real['rmse'].iloc[4],
                 real['rmse'].iloc[5]]
    throughput_rsme = [op['rmse'].iloc[0], op['rmse'].iloc[1], op['rmse'].iloc[2], op['rmse'].iloc[3],
                       op['rmse'].iloc[4],
                       op['rmse'].iloc[5]]
    tick_label = ["20", "34", "39", "68", "50", "85"]
    index = np.arange(len(tick_label))
    bar_width = 0.4
    for a, b in zip(index, real_rsme):
        pyplot.text(a, b, str('{:.2f}'.format(b)), ha='center', va='bottom', fontsize=7)
    pyplot.bar(index, real_rsme, bar_width, color="c", align="center", tick_label=tick_label)
    for a, b in zip(index + bar_width, throughput_rsme):
        pyplot.text(a, b, str('{:.2f}'.format(b)), ha='center', va='bottom', fontsize=7)
    pyplot.bar(index + bar_width, throughput_rsme, bar_width, color="b", align="center", tick_label=tick_label)
    pyplot.legend(['real-time prediction rsme', 'throughput optimization rsme'])
    pyplot.xlabel("Num_workers")
    pyplot.ylabel("RSME")
    pyplot.title("Bandwidth 10g to 100g")
    # pyplot.savefig('D:/N_Prediction/data/compare/picture/comparetime.jpg',dpi = 600)
    pyplot.show()


if __name__ == '__main__':
    drawplot()
    # drawbar()
