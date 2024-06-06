# Import necessary libraries
import os
import argparse
import pandas as pd
import sqlite3
from kaggle.api.kaggle_api_extended import KaggleApi

def ensure_directory(path):
    # Check if directory exists, else create one
    if not os.path.exists(path):
        try:
            os.makedirs(path)  
            print(f"Directory created: {path}")
        except Exception as e:
            # If there is an error creating the directory, print error message
            print(f"Error creating directory {path}: {e}")
            exit(1)
    else:
        # If the directory already exists, print a message 
        print(f"Directory already exists: {path}")

def download_csv_from_kaggle(dataset, file_name, output_folder):
    # Initialize the Kaggle API
    api = KaggleApi()
    api.authenticate()
    # Download the file from kaggle
    api.dataset_download_file(dataset, file_name, path=output_folder, unzip=True)
    # Construct path to the downloaded CSV file
    csv_path = os.path.join(output_folder, file_name)
    return csv_path 

def process_weather_data(csv_file, output_folder):
    # Ensure output directory exists, else createe
    ensure_directory(output_folder)
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file, on_bad_lines='skip')
    print("Columns in the CSV file:", df.columns)

    df.columns = df.columns.str.strip()
    print("Columns after stripping whitespace:", df.columns)

    # Check if the 'country' column exists in the DataFrame
    if 'country' in df.columns:
        print("Filtering for Greece...")
        # Filter the DataFrame to include only rows for Greece
        df_greece = df[df['country'].str.strip() == 'Greece']
        print("Filtered DataFrame head:", df_greece.head())

        # Convert the date column to datetime format, for making it easier to merge with the other dataset
        df_greece['date'] = pd.to_datetime(df_greece['date'], format='%d-%m-%Y')

        # Extract the week number and year from date column
        df_greece['week'] = df_greece['date'].dt.isocalendar().week
        df_greece['year'] = df_greece['date'].dt.isocalendar().year

        # DATA TRANSFORMATION
        # Drop the country column as we are only focusing on Greece
        df_greece.drop(columns=['country'], inplace=True)

        # Drop the data column
        df_greece.drop(columns=['date'], inplace=True)

        # Drop the Latitude column as not relevant to current analysis
        df_greece.drop(columns=['Latitude'], inplace=True)

        # Drop the Longitude column as not relevant to current analysis
        df_greece.drop(columns=['Longitude'], inplace=True)

        # Rename columns to more descriptive name
        df_greece.rename(columns={
        'tavg': 'temp.avg',
        'tmin': 'temp.min',
        'tmax': 'temp.max',
        'wdir': 'winddir',
        'wspd': 'windspd',
        'pres': 'pressure'
    }, inplace=True)

        # Construct the full path for the output CSV
        output_csv_path = os.path.join(output_folder, 'greece_weather_weekly.csv')
        # Save the modified DataFrame to the output CSV 
        df_greece.to_csv(output_csv_path, index=False)
        print(f"All observations for Greece with weeks saved to {output_csv_path}")
    else:
        # If country column does not exist, print error message
        print("The CSV file does not contain the 'country' column.")
        return

def main():
    # Define the Kaggle dataset
    dataset = 'balabaskar/historical-weather-data-of-all-country-capitals'
    file_name = 'daily_weather_data.csv'

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Process weather data from Kaggle.')
    parser.add_argument('--output-dir', type=str, required=True, help='Directory to save the processed CSV file.')
    args = parser.parse_args()

    # Output where CSV is saved
    output_dir = args.output_dir

    # Download the CSV file from Kaggle
    csv_file = download_csv_from_kaggle(dataset, file_name, output_dir)

    # Process the downloaded weather data
    process_weather_data(csv_file, output_dir)

if __name__ == "__main__":
    main()  
