## AIDTN

It's a dynamic transfer data system which can optimize the throughput during the transfer and can reduce the transfer tieme. This repo includes the train the model and make the predition code and necessary datasets to run the experiments/tests. 

### File Structrue

------

data/: Contains the /historical data, /real-time data , /train_test_data.  Each folder contains 3 cluster(mrp,prp,mrp_nvmeof) data.

log/: The train model log.

result/: Contains the predicted throughput during the transfer.

historical_data_generation.py: Generate the historical data to do train the model.

extract_data.py: Extract data from monitoring.

extractor.py: Extract the data during the transfer.

Preprocessed_data.py: Normalize the data.

BI-LSTM.py: Train the Bi-Lstm model.

XGBoost.py: Train the XGBoost model.

real_time_prediction.py: Predict the real-time resource usage during the transfer.

N_prediction.py: Predict the num_workers during the transfer.

dynamic_transfer.py: Use the controller to do the dynamic transfer with optimization.

visualization.py: Evaluation the result.

### Generate the historical data

------

run historical_data_generation.py to generate data in different resource utilization.

run extract_data.py to extrac the data from the monitoring.

### Normalize the data 

------

run Preprocessed_data.py to normalize the data to train the Bi-Lstm model and XGBoost model. We use Min-maxnormalization method to normalize the data.

### Training

------

run BI-LSTM.py to train the real-time model.

run XGBoost.py to train the optimization model.

### Runnig dynamic transfer

------

run dynamic_transfer.py to do the dynamic transfer.

