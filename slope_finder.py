'''
This is a groundbreaking code, very useful in the market/stock direction analysis. This code populates the whole
database with angles of EMAs (100 & 200). These angles are a crude shortcut to finding maxima/minima on different EMAs.
For that we would need to find tangents to the EMAs but aren't these angles representing slopes? They are.

Logic:          The code creates 2 vectors by looking back candle_dif candles back in time. Time serves as x-axis, EMA
                value as y-axis, and we create 2 vectors. Now we compute the angle and return it.

Sensitivity:    Change 'candle_dif' or use different EMAs. Smaller the candle_dif, higher the sensitivity to price
                direction change. Smaller the EMA (say 25) more sensitive the angle becomes.

Usage:          Very useful to determine the past and present direction of the market. Earlier, I relied on diff of EMAs,
                (say d = EMA100-EMA200) but this has a huge problem you can have d>0 & d = (say 50) for a rising stock
                or for a sideways stock as well. And then, one has to check the rate of change of EMAs to determine
                sudden rise in prices. Change in angle easily tells us that.
'''

import pandas as pd
import math


def compute_angle(d, candle_dif=15):
    """Compute the angle given the difference in EMA values and the candlestick difference."""
    if d == 0:
        return 0
    angle_rad = math.atan(d / candle_dif)
    angle_deg = math.degrees(angle_rad)
    return angle_deg


# Paths
parquet_file_path_template = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Historical_data_parquet/{}_{}.parquet"
fno_codes_path = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Symbol_source/fno_codes.parquet"

# Load stock codes
stock_codes_df = pd.read_parquet(fno_codes_path)
stock_codes = stock_codes_df["CODE"].tolist()

# Timeframes to be processed
timeframes = ["5", "15", "60", "1d"]

for stock_code in stock_codes:
    for timeframe in timeframes:
        # Construct file path
        file_path = parquet_file_path_template.format(stock_code, timeframe)

        # Load data
        df = pd.read_parquet(file_path)

        # Compute angles for last 200 rows
        for idx in range(len(df) - 200, len(df)):
            d_100 = df.loc[idx, 'EMA100'] - df.loc[idx - 1, 'EMA100']
            d_200 = df.loc[idx, 'EMA200'] - df.loc[idx - 1, 'EMA200']

            angle_100 = compute_angle(d_100)
            angle_200 = compute_angle(d_200)

            df.loc[idx, 'Angle_EMA100'] = angle_100
            df.loc[idx, 'Angle_EMA200'] = angle_200
            df.loc[idx, 'Angle_Difference'] = angle_100 - angle_200

        # Save updated data back to parquet file
        df.to_parquet(file_path)

print("All files have been updated!")
