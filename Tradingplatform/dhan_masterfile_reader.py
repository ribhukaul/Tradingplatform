import pandas as pd

try:
    # Path to your Excel file
    file_path = 'D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Symbol_source/api-scrip-master-Dhan.csv'

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)

    # Display the first few rows of the DataFrame
    print(df.iloc[0])

    # Filter the dataframe based on two conditions
    filtered_df = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'EQUITY') &
                     (df['SEM_TICK_SIZE'] == 5)]
    print(filtered_df)

    # Save the filtered dataframe as an Excel file
    filtered_df.to_excel("D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/"
                         "Symbol_source/Equity_Dhan.xlsx", index=True)

except Exception as e:
    print(f"An error occurred: {e}")
