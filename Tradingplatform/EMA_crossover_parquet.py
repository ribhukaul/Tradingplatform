import pandas as pd
import os
import webbrowser
import tempfile
import datetime


def get_ema_crossovers(stock_code, timeframe):
    file_path = parquet_file_path_template.format(stock_code, timeframe)

    if not os.path.exists(file_path):
        print(f"No data for stock code: {stock_code} in {timeframe} timeframe")
        return None

    # Read the parquet file
    df = pd.read_parquet(file_path)

    # Filter for the last 5 days (based on the intervals of the respective timeframe)
    interval_multiplier = 78 if timeframe == "5" else (260 if timeframe == "15" else 65)
    df = df.tail(5 * interval_multiplier)

    # Identify crossovers by finding where the signs are different when subtracting the two EMAs
    df['crossover'] = (df['EMA25'] - df['EMA50']).apply(
        lambda x: 'bullish' if x > 0 else ('bearish' if x < 0 else 'none'))

    # Check if the last crossover is bullish
    last_crossover = df.iloc[-1]['crossover']
    return last_crossover


parquet_file_path_template = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Historical_data_parquet/{}_{}.parquet"

# Load stock_codes from fno_codes.parquet
fno_codes_path = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Symbol_source/fno_codes.parquet"
stock_codes_df = pd.read_parquet(fno_codes_path)
stock_codes = stock_codes_df["CODE"].tolist()

bullish_stocks = []

# Check for bullish crossovers
for stock_code in stock_codes:
    bullish_5min = get_ema_crossovers(stock_code, "5")
    if bullish_5min == 'bullish':
        bullish_15min = get_ema_crossovers(stock_code, "15")
        if bullish_15min == 'bullish':
            bullish_60min = get_ema_crossovers(stock_code, "60")
            if bullish_60min == 'bullish':
                bullish_stocks.append((stock_code, '5min', '15min', '60min'))
            else:
                bullish_stocks.append((stock_code, '5min', '15min'))

# Generate HTML output
html_output = "<html><head><title>Bullish Stocks Analysis</title></head><body>"
html_output += "<h2>Bullish stocks:</h2>"

for stock in bullish_stocks:
    timeframes = ", ".join(stock[1:])
    html_output += f"<p><strong>{stock[0]}</strong> in timeframes: {timeframes}</p>"

html_output += "</body></html>"

# Save the HTML to a temporary file and open it in the browser
with tempfile.NamedTemporaryFile('w', delete=False, suffix='.html') as f:
    url = 'file://' + f.name
    f.write(html_output)

webbrowser.open(url)

# Save to Excel
current_date = datetime.datetime.now().strftime('%Y_%m_%d')
filename = (f"D:\EDUCATION_MID_LIFE\PYTHON\Trading screeners\gpt4\screener_outputs"
            f"/multi-timeframe-ema-based-bullish_stocks_analysis_{current_date}.xlsx")
df_bullish_stocks = pd.DataFrame(bullish_stocks, columns=['Stock Code', '5min', '15min', '60min'])
df_bullish_stocks.to_excel(filename, index=False)


