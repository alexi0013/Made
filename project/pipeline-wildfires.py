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

def process_fire_alerts_data(csv_file, output_folder):
    ensure_directory(output_folder)
    df = pd.read_csv(csv_file, on_bad_lines='skip')
    print("Columns in the CSV file:", df.columns)

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    print("Columns after stripping whitespace:", df.columns)

    # Drop the 'iso' and 'confidence__cat' columns
    df.drop(columns=['iso', 'confidence__cat'], inplace=True)

    # Filter the DataFrame for the years between 2018 and 2022, starting from week 41 of 2018 until week 41 of 2022
    df = df[
        ((df['alert__year'] > 2018) | ((df['alert__year'] == 2018) & (df['alert__week'] >= 41))) &
        ((df['alert__year'] < 2022) | ((df['alert__year'] == 2022) & (df['alert__week'] <= 41)))
    ]
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

def main():
    # Define the path to the Downloads directory
    downloads_dir = '/Users/alexisarvanitidis/Downloads'  # Change this as needed

    # Define the path to the fire alerts CSV file in Downloads
    fire_alerts_csv = os.path.join(downloads_dir, 'viirs_fire_alerts__count.csv')  # Change this as needed

    # Process the fire alerts data
    process_fire_alerts_data(fire_alerts_csv, downloads_dir)

if __name__ == "__main__":
    main()
