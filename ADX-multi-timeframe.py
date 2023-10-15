# Needs improvement

import pandas as pd
import os
import webbrowser
import tempfile
import talib
import datetime


def check_adx_crossover(stock_code, timeframe):
    file_path = parquet_file_path_template.format(stock_code, timeframe)

    if not os.path.exists(file_path):
        print(f"No data for stock code: {stock_code} in {timeframe} timeframe")
        return False

    # Read the parquet file
    df = pd.read_parquet(file_path)

    # Filter for the last 8 days based on the intervals of the respective timeframe
    interval_multiplier = 78 if timeframe == "5" else (260 if timeframe == "15" else 65)
    df = df.tail(8 * interval_multiplier)

    # Calculate ADX, +DI and -DI
    # Calculate ADX, +DI and -DI
    df['+DI'] = talib.PLUS_DI(df['High'], df['Low'], df['Close'], timeperiod=14)
    df['-DI'] = talib.MINUS_DI(df['High'], df['Low'], df['Close'], timeperiod=14)
    df['ADX'] = talib.ADX(df['High'], df['Low'], df['Close'], timeperiod=14)

    # Check if the last +DI is greater than -DI
    if df.iloc[-1]['+DI'] > df.iloc[-1]['-DI']:
        return True
    return False


parquet_file_path_template = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Historical_data_parquet/{}_{}.parquet"

# Load stock_codes from fno_codes.parquet
fno_codes_path = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Symbol_source/fno_codes.parquet"
stock_codes_df = pd.read_parquet(fno_codes_path)
stock_codes = stock_codes_df["CODE"].tolist()

bullish_stocks = []

# Check for ADX crossovers
for stock_code in stock_codes:
    if (check_adx_crossover(stock_code, "5") and check_adx_crossover(stock_code, "15") and
            check_adx_crossover(stock_code, "60")):
        bullish_stocks.append(stock_code)

# Generate HTML output
html_output = "<html><head><title>ADX Crossover Analysis</title></head><body>"
html_output += "<h2>Stocks with ADX Crossover:</h2>"
for stock in bullish_stocks:
    html_output += f"<p><strong>{stock}</strong></p>"
html_output += "</body></html>"

# Save the HTML to a temporary file and open it in the browser
with tempfile.NamedTemporaryFile('w', delete=False, suffix='.html') as f:
    url = 'file://' + f.name
    f.write(html_output)
webbrowser.open(url)

# Save to Excel
current_date = datetime.datetime.now().strftime('%Y_%m_%d')
filename = f"D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/adx_crossover_analysis_{current_date}.xlsx"
df_bullish_stocks = pd.DataFrame(bullish_stocks, columns=['Stock Code'])
df_bullish_stocks.to_excel(filename, index=False)
