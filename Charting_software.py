import pandas as pd
import os
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
import plotly.graph_objects as go

# Paths
parquet_file_path_template = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Historical_data_parquet/{}_{}.parquet"
fno_codes_path = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Symbol_source/fno_codes.parquet"

# Load stock codes
stock_codes_df = pd.read_parquet(fno_codes_path)
stock_codes = stock_codes_df["CODE"].tolist()


def refresh_plot():
    stock_code = stock_var.get()
    timeframe = timeframe_var.get()
    file_path = parquet_file_path_template.format(stock_code, timeframe)

    if not os.path.exists(file_path):
        messagebox.showerror("Error", f"No data for stock code: {stock_code} in {timeframe} timeframe")
        return

    df = pd.read_parquet(file_path)
    df.index = pd.to_datetime(df.index)  # Convert the index to datetime

    # Filter the dataframe using the date range
    try:
        start_date = datetime.strptime(start_date_var.get(), '%Y-%m-%d')
        end_date = datetime.strptime(end_date_var.get(), '%Y-%m-%d')
        df = df[(df.index >= start_date) & (df.index <= end_date)]
    except ValueError:
        messagebox.showerror("Error", "Invalid date format. Please use YYYY-MM-DD.")
        return

    # Rest of the code.

    # Using Plotly to plot candlestick
    fig = go.Figure(data=[go.Candlestick(x=df['Timestamp'],
                                         open=df['Open'],
                                         high=df['High'],
                                         low=df['Low'],
                                         close=df['Close'],
                                         showlegend=True),
                         go.Scatter(x=df['Timestamp'], y=df['EMA25'], mode='lines', name='EMA25'),
                         go.Scatter(x=df['Timestamp'], y=df['EMA50'], mode='lines', name='EMA50'),
                         go.Scatter(x=df['Timestamp'], y=df['EMA100'], mode='lines', name='EMA100'),
                         go.Scatter(x=df['Timestamp'], y=df['EMA200'], mode='lines', name='EMA200')])

    fig.update_layout(title=f'{stock_code} {timeframe}', xaxis_title='Date', yaxis_title='Price')
    fig.show()


# GUI
root = tk.Tk()
root.title("Stock and Timeframe Selection")

frame = ttk.Frame(root)
frame.pack(padx=10, pady=10)

# Dropdowns
stock_var = tk.StringVar()
stock_dropdown = ttk.Combobox(frame, textvariable=stock_var, values=stock_codes, width=15)
stock_dropdown.grid(column=0, row=0)
stock_dropdown.set(stock_codes[0])

timeframes = ["5", "15", "60", "1D"]
timeframe_var = tk.StringVar()
timeframe_dropdown = ttk.Combobox(frame, textvariable=timeframe_var, values=timeframes, width=15)
timeframe_dropdown.grid(column=1, row=0)
timeframe_dropdown.set(timeframes[2])

# Date entries
start_date_label = ttk.Label(frame, text="Start Date (YYYY-MM-DD):")
start_date_label.grid(column=0, row=1, sticky='w')
start_date_var = tk.StringVar()
start_date_entry = ttk.Entry(frame, textvariable=start_date_var, width=15)
start_date_entry.grid(column=1, row=1)

end_date_label = ttk.Label(frame, text="End Date (YYYY-MM-DD):")
end_date_label.grid(column=0, row=2, sticky='w')
end_date_var = tk.StringVar()
end_date_entry = ttk.Entry(frame, textvariable=end_date_var, width=15)
end_date_entry.grid(column=1, row=2)

# Refresh button
refresh_btn = ttk.Button(frame, text="Generate Plot", command=refresh_plot)
refresh_btn.grid(column=2, row=0, padx=5, pady=5)

root.mainloop()
