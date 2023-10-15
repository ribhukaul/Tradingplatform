'''
Program name: FYERS Histoical Data Retriever.
Description: This program is used for downloading historical data of stocks from Indian markets (NSE,BSE)
             using FYERS-API. The user needs an account with Fyers demat broker (India) and also activate its APIs.
Author:      Ribhu KAul
Date created: 02 October 2023
Last modified: 09 October 2023
Key points: To change the type/list of stocks to be downloaded, update or change the fno_codes.xlsx
            you need client id and secret key from https://myapi.fyers.in/dashboard
            Use this program when no other option is left, it takes 3 hours to download
'''


from fyers_api import fyersModel
from fyers_api import accessToken
from datetime import datetime, timedelta
from IPython.display import display
import pandas as pd
import os
import numpy as np
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


axt = get_access_token()

fyers = fyersModel.FyersModel(client_id=client_id, token=axt,
                              log_path="D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners")
print(fyers.get_profile())

t1 = "5"
t2 = "15"
t3 = "60"
t4 = "1D"

file_path = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Symbol_source/fno_codes.xlsx"
df = pd.read_excel(file_path, engine='openpyxl')
codes_list = df["CODE"].iloc[:191].tolist()

print(codes_list)

# Define the offsets from today for Loop 1
offsets = [-392, -294, -196, -98, 0]

# Define the resolutions for Loop 3
time_resolutions = [t1, t2, t3, t4]

# Start of Loop 1: Loop through the codes
for name in codes_list[:191]:
    name = name.upper()
    # Check if the Excel file already exists
    excel_file_path = (f"D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/"
                       f"gpt4/Historical_data/{name}_Stock_Data_FYERS.xlsx")

    if os.path.exists(excel_file_path):
        existing_data = pd.read_excel(excel_file_path, sheet_name=None, engine='openpyxl')
    else:
        existing_data = {}

    # Start of Loop 2: Loop through the different offsets
    for offset in offsets:

        # Calculate the start and end dates based on the offset
        start_date = (datetime.today() + timedelta(days=offset - 98)).strftime('%Y-%m-%d')
        end_date = (datetime.today() + timedelta(days=offset)).strftime('%Y-%m-%d')

        with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:

            # Start of Loop 3: Loop through the resolutions
            for resolution in time_resolutions:
                stock = {
                    "symbol": f"NSE:{name}-EQ",
                    "resolution": resolution,
                    "date_format": "1",
                    "range_from": start_date,
                    "range_to": end_date,
                    "cont_flag": "1"
                }
                info = fyers.history(data=stock)
                if 'candles' not in info or not info['candles']:
                    print(
                        f"Did not receive valid data for stock {name} with resolution {resolution} "
                        f"for the date range {start_date} to {end_date}.")
                    continue
                stock_candles = info['candles']
                column_names = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
                stock_candles_df = pd.DataFrame(stock_candles, columns=column_names)
                stock_candles_df['Timestamp'] = pd.to_datetime(stock_candles_df['Timestamp'], unit='s')

                if resolution in existing_data:
                    combined_data = pd.concat([existing_data[resolution], stock_candles_df])
                    combined_data = combined_data.drop_duplicates(subset=['Timestamp'], keep='last')
                    combined_data = combined_data.sort_values(by="Timestamp")
                else:
                    combined_data = stock_candles_df

                combined_data['EMA25'] = np.nan
                combined_data['EMA25'].iloc[24:] = combined_data['Close'].iloc[24:].ewm(span=25, adjust=False).mean()

                combined_data['EMA50'] = np.nan
                combined_data['EMA50'].iloc[49:] = combined_data['Close'].iloc[49:].ewm(span=50, adjust=False).mean()

                combined_data['EMA100'] = np.nan
                combined_data['EMA100'].iloc[99:] = combined_data['Close'].iloc[99:].ewm(span=100, adjust=False).mean()

                combined_data['EMA200'] = np.nan
                combined_data['EMA200'].iloc[199:] = combined_data['Close'].iloc[199:].ewm(span=200,
                                                                                           adjust=False).mean()

                combined_data.to_excel(writer, sheet_name=resolution, index=False)


# Use the below code to calculate EMAs incase there is some discontinuity in EMA values
'''
import pandas as pd
import time


def calculate_ema(data, periods):
    ema = data['Close'].ewm(span=periods, adjust=False).mean()
    ema.iloc[:periods - 1] = None  # Setting values before period count to NaN
    return ema


# Read stock symbols
symbols_df = pd.read_excel("D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Symbol_source/fno_codes.xlsx")
symbols = symbols_df['CODE'].tolist()

start_time = time.time()
total_files = len(symbols)

# Loop through each stock symbol
for idx, symbol in enumerate(symbols[63:], start=64):
    file_path = f"D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Historical_data/{symbol}_Stock_Data_FYERS.xlsx"

    # Read resolutions from the Excel file
    with pd.ExcelFile(file_path) as xls:
        resolutions = xls.sheet_names

    # Loop through each resolution
    for resolution in resolutions:
        data_df = pd.read_excel(file_path, sheet_name=resolution)

        # Calculate EMAs
        data_df['EMA25'] = calculate_ema(data_df, 25)
        data_df['EMA50'] = calculate_ema(data_df, 50)
        data_df['EMA100'] = calculate_ema(data_df, 100)
        data_df['EMA200'] = calculate_ema(data_df, 200)

        # Reorder the columns
        columns_order = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'EMA25', 'EMA50', 'EMA100',
                         'EMA200'] + [col for col in data_df.columns if
                                      col not in ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'EMA25',
                                                  'EMA50', 'EMA100', 'EMA200']]
        data_df = data_df[columns_order]

        # Overwrite the specific sheet with updated data
        with pd.ExcelWriter(file_path, engine='openpyxl', mode='a') as writer:
            book = writer.book
            if resolution in book.sheetnames:
                idx_sheet = book.sheetnames.index(resolution)
                book.remove(book.worksheets[idx_sheet])
            data_df.to_excel(writer, sheet_name=resolution, index=False)

    # Calculate elapsed time and estimated remaining time
    elapsed_time = time.time() - start_time
    estimated_remaining_time = (elapsed_time / idx) * (total_files - idx)
    estimated_remaining_minutes = estimated_remaining_time / 60
    print(f"Processed {idx}/{total_files}. Estimated time remaining: {estimated_remaining_minutes:.2f} minutes")


print("EMAs recalculated and saved for all stock symbols and resolutions!")

'''

