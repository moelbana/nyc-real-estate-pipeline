import pandas as pd
import os
import re

XLSX_DIR = 'data/raw_xlsx'
CSV_DIR = 'data/raw_csv'

SHEET_NAMES = {
    '2024_bronx.xlsx': 'Bronx',
    '2024_brooklyn.xlsx': 'Brooklyn',  
    '2024_manhattan.xlsx': 'Manhattan',  
    '2024_queens.xlsx': 'Queens',
    '2024_staten_island.xlsx': 'Staten Island'
}

def clean_header(col_name):
    """
    This function handles the header column.
    """
    if isinstance(col_name, str):
        return re.sub(r'\s+', ' ', col_name.replace('\n', ' ')).strip()
    return col_name

def convert_files():
    """
    finds all .xlsx files from source directory and convert them to csv files,
    then save them in the target directory. 
    """
    files_to_convert = []
    for file in os.listdir(XLSX_DIR):
        if file.endswith('.xlsx'):
            files_to_convert.append(file)
        else:
            print("Empty directory")
    
    for xlsx_file in files_to_convert:
        input_path = os.path.join(XLSX_DIR, xlsx_file)
        csv_file = xlsx_file.replace('.xlsx', '.csv')
        output_path = os.path.join(CSV_DIR, csv_file)

        sheet_name = SHEET_NAMES.get(xlsx_file)

        # Reading .xlsx files
        print(f"Reading {input_path} sheet: {sheet_name}...")
        df = pd.read_excel(input_path, sheet_name=sheet_name, skiprows=6)

        # Clean the header
        df.columns = [clean_header(col) for col in df.columns]

        # Drop the empty row 
        df.dropna(how='all', inplace=True)

        # Writing to a .csv file
        df.to_csv(output_path, index=False, header=True)

        print(f"file converted and saved.")
    
    print('All files converted and saved')

if __name__ == "__main__":
    convert_files()