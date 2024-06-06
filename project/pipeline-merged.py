import os
import pandas as pd

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

def process_fire_alerts_data(csv_file, output_folder):
    ensure_directory(output_folder)
    df = pd.read_csv(csv_file, on_bad_lines='skip')
    print("Columns in the CSV file:", df.columns)

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    print("Columns after stripping whitespace:", df.columns)

    # Drop the 'iso' and 'confidence__cat' columns if they exist
    columns_to_drop = ['iso', 'confidence__cat']
    df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)

    # Filter the DataFrame for the years between 2018 and 2022, starting from week 41 of 2018 until week 41 of 2022
    df = df[
        ((df['alert__year'] > 2018) | ((df['alert__year'] == 2018) & (df['alert__week'] >= 41))) &
        ((df['alert__year'] < 2022) | ((df['alert__year'] == 2022) & (df['alert__week'] <= 41)))
    ]
    print(f"Filtered DataFrame length: {len(df)}")
    print("Filtered DataFrame head:", df.head())

    # Group by 'alert__year' and 'alert__week', and sum 'alert__count'
    df_grouped = df.groupby(['alert__year', 'alert__week'], as_index=False).agg({
        'alert__count': 'sum'
    })
    print("Grouped DataFrame head:", df_grouped.head())

    # Save the grouped DataFrame to a new CSV file in the output folder
    output_csv_path = os.path.join(output_folder, 'processed_fire_alerts_aggregated.csv')
    df_grouped.to_csv(output_csv_path, index=False)
    print(f"Processed fire alerts saved to {output_csv_path}")

def merge_datasets(weather_csv, fire_alerts_csv, output_folder):
    # Ensure the output directory exists
    ensure_directory(output_folder)
    
    # Read the CSV files into pandas DataFrames
    weather_df = pd.read_csv(weather_csv)
    fire_alerts_df = pd.read_csv(fire_alerts_csv)
    
    # Print columns for debugging purposes
    print("Weather DataFrame columns:", weather_df.columns)
    print("Fire Alerts DataFrame columns:", fire_alerts_df.columns)
    
    # Check the data types of the columns
    print("Weather DataFrame dtypes:\n", weather_df.dtypes)
    print("Fire Alerts DataFrame dtypes:\n", fire_alerts_df.dtypes)
    
    # Ensure the 'year' and 'week' columns are integers
    weather_df['year'] = weather_df['year'].astype(int)
    weather_df['week'] = weather_df['week'].astype(int)
    fire_alerts_df['alert__year'] = fire_alerts_df['alert__year'].astype(int)
    fire_alerts_df['alert__week'] = fire_alerts_df['alert__week'].astype(int)
    
    # Print the unique values for the merging columns
    print("Unique years in weather data:", weather_df['year'].unique())
    print("Unique weeks in weather data:", weather_df['week'].unique())
    print("Unique years in fire alerts data:", fire_alerts_df['alert__year'].unique())
    print("Unique weeks in fire alerts data:", fire_alerts_df['alert__week'].unique())
    
    # Merge the DataFrames on the 'year' and 'week' columns
    merged_df = pd.merge(weather_df, fire_alerts_df, left_on=['year', 'week'], right_on=['alert__year', 'alert__week'], how='inner')
    
    # Drop the duplicate 'year' and 'week' columns from the fire alerts DataFrame
    merged_df.drop(columns=['alert__year', 'alert__week'], inplace=True)
    
    # Save the merged DataFrame to a new CSV file in the output folder
    output_csv_path = os.path.join(output_folder, 'merged_weather_fire_alerts.csv')
    merged_df.to_csv(output_csv_path, index=False)
    print(f"Merged data saved to {output_csv_path}")
    print(f"Merged DataFrame length: {len(merged_df)}")
    print("Merged DataFrame head:", merged_df.head())

def main():
    # Define the path to the Downloads directory
    downloads_dir = '/Users/alexisarvanitidis/Downloads'  # Change this as needed
    
    # Define the paths to the CSV files
    weather_csv = os.path.join(downloads_dir, 'greece_weather_weekly_aggregated.csv')
    fire_alerts_csv = os.path.join(downloads_dir, 'processed_fire_alerts_aggregated.csv')
    
    # Process the data and merge the datasets
    process_fire_alerts_data(fire_alerts_csv, downloads_dir)
    merge_datasets(weather_csv, fire_alerts_csv, downloads_dir)

if __name__ == "__main__":
    main()
