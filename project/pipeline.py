# Import necessary libraries
import os
import pandas as pd
import argparse
from kaggle.api.kaggle_api_extended import KaggleApi
from io import StringIO
import requests

# Ensure the directory exists, elsee create it
def ensure_directory(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
            print(f"Directory created: {path}")
        except Exception as e:
            print(f"Error creating directory {path}: {e}")
            exit(1)
    else:
        print(f"Directory already exists: {path}")

# Download a file from Kaggle
def download_csv_from_kaggle(dataset, file_name, output_folder):
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_file(dataset, file_name, path=output_folder, unzip=True)
    csv_path = os.path.join(output_folder, file_name)
    return csv_path

# Process weather data: filter for Greece + aggregate by week
def process_weather_data(csv_file, output_folder):
    ensure_directory(output_folder)
    df = pd.read_csv(csv_file, on_bad_lines='skip')
    print("Columns in the CSV file:", df.columns)

    # Whitespace from column names
    df.columns = df.columns.str.strip()
    print("Columns after stripping whitespace:", df.columns)

    if 'country' in df.columns:
        print("Filtering for Greece...")
        df_greece = df[df['country'].str.strip() == 'Greece']
        print("Filtered DataFrame head:", df_greece.head())

        # Convert date column to datetime format
        df_greece['date'] = pd.to_datetime(df_greece['date'], format='%d-%m-%Y')

        # Extract week number and year
        df_greece['week'] = df_greece['date'].dt.isocalendar().week
        df_greece['year'] = df_greece['date'].dt.isocalendar().year

        # Filter for data starting from week 41 of 2018 until week 41 of 2022
        df_greece = df_greece[
            ((df_greece['year'] > 2018) | ((df_greece['year'] == 2018) & (df_greece['week'] >= 41))) &
            ((df_greece['year'] < 2022) | ((df_greece['year'] == 2022) & (df_greece['week'] <= 41)))
        ]

        # Drop unnecessary columns
        df_greece.drop(columns=['date', 'country', 'Latitude', 'Longitude'], inplace=True)

        # Rename columns to more descriptive names
        df_greece.rename(columns={
            'tavg': 'temp.avg',
            'tmin': 'temp.min',
            'tmax': 'temp.max',
            'wdir': 'winddir',
            'wspd': 'windspd',
            'pres': 'pressure'
        }, inplace=True)

        # Group by 'year' and 'week' + calculate the avg. temp. and sum other columns
        df_grouped = df_greece.groupby(['year', 'week'], as_index=False).agg({
            'temp.avg': 'mean',
            'temp.min': 'mean',
            'temp.max': 'mean',
            'winddir': 'mean',
            'windspd': 'mean',
            'pressure': 'mean'
        })

        # Round the aggregated values to one decimal place
        df_grouped = df_grouped.round(1)

        # Save the DataFrame to a new CSV file
        output_csv_path = os.path.join(output_folder, 'greece_weather_weekly_aggregated.csv')
        df_grouped.to_csv(output_csv_path, index=False)
        print(f"All observations for Greece with weeks saved to {output_csv_path}")
    else:
        print("The CSV file does not contain the 'country' column.")
        return

# Process fire alerts data: filter + aggregate by week
def process_fire_alerts_data(csv_file, output_folder):
    ensure_directory(output_folder)
    df = pd.read_csv(csv_file, on_bad_lines='skip')
    print("Columns in the CSV file:", df.columns)

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    print("Columns after stripping whitespace:", df.columns)

    # Drop the 'iso' and 'confidence__cat' columns
    columns_to_drop = ['iso', 'confidence__cat']
    df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)

    # Filter the DataFrame again starting from week 41 of 2018 until week 41 of 2022
    df = df[
        ((df['alert__year'] > 2018) | ((df['alert__year'] == 2018) & (df['alert__week'] >= 41))) &
        ((df['alert__year'] < 2022) | ((df['alert__year'] == 2022) & (df['alert__week'] <= 41)))
    ]
    print(f"Filtered DataFrame length: {len(df)}")
    print("Filtered DataFrame head:", df.head())

    # Group by 'alert__year' and 'alert__week' + sum 'alert__count'
    df_grouped = df.groupby(['alert__year', 'alert__week'], as_index=False).agg({
        'alert__count': 'sum'
    })
    print("Grouped DataFrame head:", df_grouped.head())

    # Save the grouped DataFrame to a new CSV file
    output_csv_path = os.path.join(output_folder, 'processed_fire_alerts_aggregated.csv')
    df_grouped.to_csv(output_csv_path, index=False)
    print(f"Processed fire alerts saved to {output_csv_path}")

# Merge the processed weather and fire alerts datasets
def merge_datasets(weather_csv, fire_alerts_csv, output_folder):
    ensure_directory(output_folder)
    
    # Read the CSV files into pandas DataFrames
    weather_df = pd.read_csv(weather_csv)
    fire_alerts_df = pd.read_csv(fire_alerts_csv)
    
    print("Weather DataFrame columns:", weather_df.columns)
    print("Fire Alerts DataFrame columns:", fire_alerts_df.columns)
    
    print("Weather DataFrame dtypes:\n", weather_df.dtypes)
    print("Fire Alerts DataFrame dtypes:\n", fire_alerts_df.dtypes)
    
    # Ensure the 'year' and 'week' columns are ints
    weather_df['year'] = weather_df['year'].astype(int)
    weather_df['week'] = weather_df['week'].astype(int)
    fire_alerts_df['alert__year'] = fire_alerts_df['alert__year'].astype(int)
    fire_alerts_df['alert__week'] = fire_alerts_df['alert__week'].astype(int)
    
    print("Unique years in weather data:", weather_df['year'].unique())
    print("Unique weeks in weather data:", weather_df['week'].unique())
    print("Unique years in fire alerts data:", fire_alerts_df['alert__year'].unique())
    print("Unique weeks in fire alerts data:", fire_alerts_df['alert__week'].unique())
    
    # Merge the DataFrames on the 'year' and 'week' columns
    merged_df = pd.merge(weather_df, fire_alerts_df, left_on=['year', 'week'], right_on=['alert__year', 'alert__week'], how='inner')
    merged_df.drop(columns=['alert__year', 'alert__week'], inplace=True)
    
    # Save the merged DataFrame to a new CSV file
    output_csv_path = os.path.join(output_folder, 'merged_weather_fire_alerts.csv')
    merged_df.to_csv(output_csv_path, index=False)
    print(f"Merged data saved to {output_csv_path}")
    print(f"Merged DataFrame length: {len(merged_df)}")
    print("Merged DataFrame head:", merged_df.head())

def main():
    # Define the Kaggle datasets
    weather_dataset = 'balabaskar/historical-weather-data-of-all-country-capitals'
    weather_file_name = 'daily_weather_data.csv'
    fire_alerts_dataset = 'path-to-your-fire-alerts-dataset'
    fire_alerts_file_name = 'viirs_fire_alerts__count.csv'

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Process weather and fire alerts data from Kaggle.')
    parser.add_argument('--output-dir', type=str, required=True, help='Directory to save the processed CSV files.')
    args = parser.parse_args()

    # Output directory
    output_dir = args.output_dir
    ensure_directory(output_dir)

    # Download the CSV files from Kaggle
    weather_csv = download_csv_from_kaggle(weather_dataset, weather_file_name, output_dir)
    fire_alerts_csv = download_csv_from_kaggle(fire_alerts_dataset, fire_alerts_file_name, output_dir)

    # Process the weather data
    process_weather_data(weather_csv, output_dir)

    # Process the fire alerts data
    process_fire_alerts_data(fire_alerts_csv, output_dir)
    
    # Paths to the processed CSV files
    processed_weather_csv = os.path.join(output_dir, 'greece_weather_weekly_aggregated.csv')
    processed_fire_alerts_csv = os.path.join(output_dir, 'processed_fire_alerts_aggregated.csv')
    
    # Merge the datasets
    merge_datasets(processed_weather_csv, processed_fire_alerts_csv, output_dir)

if __name__ == "__main__":
    main()
