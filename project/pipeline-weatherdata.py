import os
import pandas as pd

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

def process_weather_data(csv_file, output_folder):
    ensure_directory(output_folder)
    df = pd.read_csv(csv_file, on_bad_lines='skip')
    print("Columns in the CSV file:", df.columns)

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    print("Columns after stripping whitespace:", df.columns)

    if 'country' in df.columns:
        print("Filtering for Greece...")
        df_greece = df[df['country'].str.strip() == 'Greece']
        print("Filtered DataFrame head:", df_greece.head())

        # Convert the date column to datetime format
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

        # Group by 'year' and 'week', and calculate the average temperature and sum other relevant columns
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

        # Save the DataFrame to a new CSV file in the output folder
        output_csv_path = os.path.join(output_folder, 'greece_weather_weekly_aggregated.csv')
        df_grouped.to_csv(output_csv_path, index=False)
        print(f"All observations for Greece with weeks saved to {output_csv_path}")
    else:
        print("The CSV file does not contain the 'country' column.")
        return

def main():
    downloads_dir = '/Users/alexisarvanitidis/Downloads'  # Path to Downloads directory
    weather_csv = os.path.join(downloads_dir, 'daily_weather_data.csv')  # Path to the CSV file in Downloads

    process_weather_data(weather_csv, downloads_dir)

if __name__ == "__main__":
    main()
