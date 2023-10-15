import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import random
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.layers import Dense, Dropout, LSTM
from tensorflow.keras.models import Sequential

# Set seed for reproducibility
random.seed(42)

# Paths
fno_codes_path = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Symbol_source/fno_codes.parquet"
fno_codes_df = pd.read_parquet(fno_codes_path)
stock_codes = fno_codes_df['codes'].tolist()

# Randomly select 20 stocks for training
selected_stocks = random.sample(stock_codes, 20)

# Define time frames
time_frames = ['5', '15', '60', '1D']

# Placeholder for training data
combined_data = []

for stock in selected_stocks:
    for time_frame in time_frames:
        data_path = f"D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Historical_data_parquet/{stock}_{time_frame}.parquet"
        data = pd.read_parquet(data_path)
        training_data = data[(data['date'] >= '2022-09-01') & (data['date'] <= '2023-08-22')]
        combined_data.append(training_data)

# Concatenate data from all stocks and timeframes
all_data = pd.concat(combined_data, ignore_index=True)

# Preprocess data
scaler = MinMaxScaler()
scaled_data = scaler.fit_transform(all_data['close'].values.reshape(-1, 1))

# Prepare sequences for LSTM
X = []
y = []
time_intervals_to_train = 24
prediction_interval = 1

for i in range(time_intervals_to_train, len(scaled_data) - prediction_interval):
    X.append(scaled_data[i - time_intervals_to_train:i, 0])
    y.append(scaled_data[i + prediction_interval, 0])

X = np.array(X)
y = np.array(y)
X = np.reshape(X, (X.shape[0], X.shape[1], 1))

# Build the LSTM model
model = Sequential()
model.add(LSTM(128, return_sequences=True, input_shape=(X.shape[1], 1)))
model.add(Dropout(0.4))
model.add(LSTM(64, return_sequences=True))
model.add(Dropout(0.3))
model.add(LSTM(32))
model.add(Dropout(0.2))
model.add(Dense(1))

model.compile(optimizer='adam', loss='mse')
model.fit(X, y, epochs=10, batch_size=32)

# User input for evaluation
stock_code = input(f"Select a stock code for evaluation: ")
time_frame = input(f"Select a time frame from {time_frames} for evaluation: ")

# Loading the selected stock's data for testing
data_path = f"D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Historical_data_parquet/{stock_code}_{time_frame}.parquet"
test_data = pd.read_parquet(data_path)
actual_prices = test_data[(test_data['date'] > '2023-08-22')]['close'].values

# Prepare test data
test_inputs = scaler.transform(test_data['close'].values.reshape(-1, 1))
X_test = []

for i in range(time_intervals_to_train, len(test_inputs)):
    X_test.append(test_inputs[i - time_intervals_to_train:i, 0])

X_test = np.array(X_test)
X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))

# Predict
predicted_prices = model.predict(X_test)
predicted_prices = scaler.inverse_transform(predicted_prices)

# Plot
plt.figure(figsize=(12, 6))
plt.plot(actual_prices, label="Actual Prices")
plt.plot(predicted_prices, label="Predicted Prices")
plt.title(f"Stock Price Prediction for {stock_code} with {time_frame} timeframe")
plt.xlabel("Time")
plt.ylabel("Stock Price")
plt.legend()
plt.show()
