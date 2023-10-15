'''
This program was used to convert all the Excel data of NSE fno stocks that was downloaded using Fyers historical data
retriever to parquet format. This is done because Excel data is very slow to process when done in large quantities.
'''

"""
import os
import pandas as pd
from multiprocessing import Pool
import time

# Define paths and constants
input_directory = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Historical_data/"
output_directory = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Historical_data_parquet/"
time_resolutions = ["5", "15", "60", "1D"]

# Make sure the output directory exists
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# List all the Excel files in the directory
excel_files = [f for f in os.listdir(input_directory) if f.endswith("_Stock_Data_FYERS.xlsx")]

# To store start times for estimation
start_time = time.time()
times_taken = []


# Function to convert an Excel file to Parquet format
def convert_excel_to_parquet(excel_file):
    local_start_time = time.time()

    input_file_path = os.path.join(input_directory, excel_file)
    stock_name = excel_file.replace('_Stock_Data_FYERS.xlsx', '')

    for resolution in time_resolutions:
        data = pd.read_excel(input_file_path, sheet_name=resolution, engine='openpyxl')

        # Define the path for the Parquet file
        parquet_file_path = os.path.join(output_directory, f"{stock_name}_{resolution}.parquet")

        # Save data as Parquet
        data.to_parquet(parquet_file_path, index=False)

    local_end_time = time.time()
    elapsed_time = local_end_time - local_start_time
    times_taken.append(elapsed_time)

    avg_time_per_file = sum(times_taken) / len(times_taken)
    remaining_files = len(excel_files) - len(times_taken)
    estimated_time_remaining = avg_time_per_file * remaining_files

    print(
        f"Converted {excel_file} to Parquet in {elapsed_time:.2f} seconds. Estimated time remaining: {estimated_time_remaining:.2f} seconds.")

    return elapsed_time


if __name__ == "__main__":
    # Use all available CPU cores
    num_processes = os.cpu_count()

    with Pool(num_processes) as pool:
        pool.map(convert_excel_to_parquet, excel_files)

    total_time = time.time() - start_time
    print(f"Conversion completed in {total_time:.2f} seconds!")
    """
import pandas as pd

# Input and output paths
input_path = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Symbol_source/fno_codes.xlsx"
output_path = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Symbol_source/fno_codes.parquet"

# Read the Excel file
data = pd.read_excel(input_path, engine='openpyxl')

# Convert and save as Parquet
data.to_parquet(output_path, index=False)

print(f"Data from {input_path} has been successfully converted to {output_path} in Parquet format!")

