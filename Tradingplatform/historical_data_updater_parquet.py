'''
This is the program that one has to run every-day at the end of trading session to keep the database updated
All analyses must be done on a copy of this set only. The data is in parquet format and is not readable like excel
but is very fast to process.
'''

import os
import pandas as pd
from datetime import datetime
import numpy as np
from multiprocessing import Pool
from fyers_api import fyersModel
from fyers_api import accessToken
import time

client_id = "XCAKLNQVED-100"
secret_key = "XEM3UJYKFB"
redirect_url = "https://trade.fyers.in/api-login/redirect-uri/index.html"


def get_access_token():
    # Check if the access_token.txt file exists
    if os.path.exists("access_token.txt"):
        # Get the last modified timestamp of the file
        file_timestamp = os.path.getmtime("access_token.txt")
        current_timestamp = time.time()

        # Check if the file is more than 24 hours old
        if current_timestamp - file_timestamp > 86400:  # 86400 seconds = 24 hours
            os.remove("access_token.txt")  # Delete the old token file

    if not os.path.exists("access_token.txt"):
        session = accessToken.SessionModel(client_id=client_id, secret_key=secret_key,
                                           redirect_uri=redirect_url, response_type='code',
                                           grant_type='authorization_code')
        response = session.generate_authcode()
        print("Login url", response)
        auth_code = input("Enter auth code: ")
        session.set_token(auth_code)
        access_token = session.generate_token()["access_token"]
        with open("access_token.txt", "w") as f:
            f.write(access_token)
    else:
        # Read the cached token
        with open("access_token.txt", "r") as f:
            access_token = f.read()

    return access_token


# Define constants
time_resolutions = ["5", "15", "60", "1D"]
file_path = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Symbol_source/fno_codes.xlsx"
df = pd.read_excel(file_path, engine='openpyxl')
codes_list = df["CODE"].iloc[:191].tolist()

axt = get_access_token()
fyers = fyersModel.FyersModel(client_id=client_id, token=axt,
                              log_path="D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners")
print(fyers.get_profile())


def calculate_emas(data):
    """Calculate EMAs for the dataframe."""
    data['EMA25'] = data['Close'].ewm(span=25, adjust=False).mean()
    data['EMA50'] = data['Close'].ewm(span=50, adjust=False).mean()
    data['EMA100'] = data['Close'].ewm(span=100, adjust=False).mean()
    data['EMA200'] = data['Close'].ewm(span=200, adjust=False).mean()
    return data


def update_stock_data(name):
    existing_data = []
    parquet_file_path_template = ("D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Historical_data_parquet"
                                  "/{}_{}.parquet")

    for resolution in time_resolutions:
        file_path = parquet_file_path_template.format(name, resolution)

        # If file exists, get the latest date from the Parquet file
        if os.path.exists(file_path):
            existing_data = pd.read_parquet(file_path)
            latest_date = existing_data['Timestamp'].max()
        else:
            latest_date = None

        # Fetch stock data from the latest date to today
        stock = {
            "symbol": f"NSE:{name}-EQ",
            "resolution": resolution,
            "date_format": "1",
            "range_from": latest_date.strftime('%Y-%m-%d') if latest_date else datetime.now().strftime('%Y-%m-%d'),
            "range_to": datetime.now().strftime('%Y-%m-%d'),
            "cont_flag": "1"
        }

        info = fyers.history(data=stock)

        if 'candles' not in info or not info['candles']:
            continue

        stock_candles = info['candles']
        column_names = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
        new_data = pd.DataFrame(stock_candles, columns=column_names)
        new_data['Timestamp'] = pd.to_datetime(new_data['Timestamp'], unit='s')

        # Combine only the new data (avoiding overlapping dates)
        if latest_date:
            new_data = new_data[new_data['Timestamp'] > latest_date]

        combined_data = pd.concat([existing_data, new_data], ignore_index=True) if latest_date else new_data

        # Drop duplicates based on the Timestamp column
        combined_data.drop_duplicates(subset=['Timestamp'], keep='last', inplace=True)

        # Recalculate the EMAs
        combined_data = calculate_emas(combined_data)

        # Save the combined data back to Parquet
        combined_data.to_parquet(file_path, index=False)


'''


'''
if __name__ == "__main__":
    with Pool() as pool:
        pool.map(update_stock_data, codes_list)

'''
# Example usage
file_path = 'D:\EDUCATION_MID_LIFE\PYTHON\Trading screeners\gpt4\Historical_data_parquet_testing\AARTIIND_1D.parquet'
print_last_n_rows_of_parquet(file_path)

'''
