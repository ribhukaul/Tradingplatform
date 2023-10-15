import pandas as pd
import os
import webbrowser


def generate_html_for_parquet(file_path):
    """Generate HTML for the last n rows of the given Parquet file."""
    if os.path.exists(file_path):
        df = pd.read_parquet(file_path)
        return f"<h3>{file_path}</h3>{df.tail(7).to_html()}<hr>"


if __name__ == "__main__":
    # Your provided path template
    parquet_file_path_template = ("D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Historical_data_parquet"
                                  "/{}_60.parquet")

    # Your provided codes list from the Excel file
    file_path = "D:/EDUCATION_MID_LIFE/PYTHON/Trading screeners/gpt4/Symbol_source/fno_codes.xlsx"
    df = pd.read_excel(file_path, engine='openpyxl')
    codes_list = df["CODE"].iloc[:10].tolist()

    html_parts = ["<html><head><title>Parquet Viewer</title></head><body>"]

    # For each code, read the Parquet file and append the HTML representation
    for code in codes_list:
        file_path = parquet_file_path_template.format(code)
        html_content = generate_html_for_parquet(file_path)
        if html_content:
            html_parts.append(html_content)

    html_parts.append("</body></html>")

    # Save the HTML content to a temporary file and open it in a browser
    with open("temp_parquet_view.html", "w", encoding="utf-8") as f:
        f.write("\n".join(html_parts))

    webbrowser.open("temp_parquet_view.html")
