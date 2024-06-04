import os
import pandas as pd
import sqlite3

def ensure_directory(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except Exception as e:
            print(f"Error creating directory {path}: {e}")
            exit(1)

def process_weather_data(csv_file, output_folder, db_path):
    ensure_directory(output_folder)
    df = pd.read_csv(csv_file, on_bad_lines='skip')
    print("Columns in the CSV file:", df.columns)

    
    df.columns = df.columns.str.strip()
    print("Columns after stripping whitespace:", df.columns)

    if 'country' in df.columns:
        print("Filtering for Greece...")
        df_greece = df[df['country'].str.strip() == 'Greece']
        print("All observations for Greece:\n", df_greece.to_string())  # Print all rows for Greece
    else:
        print("The CSV file does not contain the 'country' column.")
        return

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__)) 
    data_dir = os.path.join(base_dir, 'data')
    project_dir = os.path.join(base_dir, 'project')

    weather_csv = '/Users/alexisarvanitidis/Downloads/daily_weather_data.csv'  # local path

    process_weather_data(weather_csv, os.path.join(project_dir, 'weather_data'), os.path.join(data_dir, 'weather_data_greece.sqlite'))

if __name__ == "__main__":
    main()
