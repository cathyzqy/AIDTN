import pandas as pd
import datetime
import csv
import os


def normalize_data(path_raw, path_normalize, path_normalize_metric,cluster):
    # select features and normalize data to train the bi-lstm model
    normalize_metric = pd.read_csv(path_normalize_metric)
    files = os.listdir(path_raw)
    if cluster == 'mrp_nvmeof':
        features_order = ['NVMe_total_util', 'CPU', 'Memory_used', 'NVMe_from_transfer', 'num_workers']
    else:
        features_order = ['NVMe_total_util', 'CPU', 'Memory_used', 'Goodput', 'num_workers']
    for file in files:
        file_read_path = path_raw + file
        data = pd.read_csv(file_read_path, header=0)
        data = data[features_order]
        name = data[features_order[4]].iloc[0]
        # according to the real value to normalized data
        data[features_order[0]] = data[features_order[0]].apply(lambda x: x / normalize_metric[features_order[0]].iloc[0])
        data[features_order[1]] = data[features_order[1]].apply(lambda x: x / normalize_metric[features_order[1]].iloc[0])
        data[features_order[2]] = data[features_order[2]].apply(lambda x: x / normalize_metric[features_order[2]].iloc[0])
        data[features_order[3]] = data[features_order[3]].apply(lambda x: x / normalize_metric[features_order[3]].iloc[0])
        data[features_order[4]] = data[features_order[4]].apply(lambda x: x / normalize_metric[features_order[4]].iloc[0])

        data.to_csv(path_normalize + str(name) + '.csv', index=None)


# use the normalize data to generate the XGBoost data
def xgboost_train_data(path_read_file, path_save):
    files = os.listdir(path_read_file)
    count = 0
    for file in files:
        filename = path_read_file + file
        savename = path_save + 'xgboost.csv'
        with open(filename, 'r') as fin, open(savename, 'a+', newline='') as fout:
            reader = csv.reader(fin, skipinitialspace=True)
            writer = csv.writer(fout, delimiter=',')
            if count == 0:
                for j, row in enumerate(reader):
                    writer.writerow(row)

            for i, rows in enumerate(reader):
                if i > 0:
                    writer.writerow(rows)
            fin.close()
            fout.close()
        count += 1


if __name__ == "__main__":
    cluster = 'prp'
    path_raw = 'data/historical_data/'+str(cluster)+'/testdata/'
    path_normalize = 'data/historical_data/'+str(cluster)+'/normalize_data/'
    path_normalize_metric = 'data/historical_data/'+str(cluster)+'/'+str(cluster)+'_data_normalize_metrics.csv'
    path_save = 'data/historical_data/'+str(cluster)+'/xgboost_data/'
    normalize_data(path_raw, path_normalize, path_normalize_metric, cluster)
    xgboost_train_data(path_normalize, path_save)
