import os
import pandas as pd
import argparse

def test_pipeline(output_dir):
    try:
        # Define paths to output files
        weather_file = os.path.join(output_dir, 'greece_weather_weekly_aggregated.csv')
        fire_alerts_file = os.path.join(output_dir, 'processed_fire_alerts_aggregated.csv')
        merged_file = os.path.join(output_dir, 'merged_weather_fire_alerts.csv')

        # Check if output files exist
        assert os.path.exists(weather_file), f"File not found: {weather_file}"
        assert os.path.exists(fire_alerts_file), f"File not found: {fire_alerts_file}"
        assert os.path.exists(merged_file), f"File not found: {merged_file}"

        # Load CSV files
        weather_df = pd.read_csv(weather_file)
        fire_alerts_df = pd.read_csv(fire_alerts_file)
        merged_df = pd.read_csv(merged_file)

        # Check the columns of the weather data
        expected_weather_columns = ['year', 'week', 'temp.avg', 'temp.min', 'temp.max', 'winddir', 'windspd', 'pressure']
        assert list(weather_df.columns) == expected_weather_columns, "Weather data columns do not match"

        # Check the columns of the fire alerts data
        expected_fire_alerts_columns = ['alert__year', 'alert__week', 'alert__count']
        assert list(fire_alerts_df.columns) == expected_fire_alerts_columns, "Fire alerts data columns do not match"

        # Check the columns of the merged data
        expected_merged_columns = ['year', 'week', 'temp.avg', 'temp.min', 'temp.max', 'winddir', 'windspd', 'pressure', 'alert__count']
        assert list(merged_df.columns) == expected_merged_columns, "Merged data columns do not match"

        # Check for NaN values in key columns
        assert weather_df[['temp.avg', 'temp.min', 'temp.max', 'winddir', 'windspd', 'pressure']].isnull().sum().sum() == 0, "NaN values found in weather data"
        assert fire_alerts_df['alert__count'].isnull().sum() == 0, "NaN values found in fire alerts data"
        assert merged_df[['temp.avg', 'temp.min', 'temp.max', 'winddir', 'windspd', 'pressure', 'alert__count']].isnull().sum().sum() == 0, "NaN values found in merged data"

        print("All tests passed successfully!")

    except AssertionError as e:
        print(f"Test failed: {e}")

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Test the data pipeline.')
    parser.add_argument('--output-dir', type=str, required=True, help='Directory containing the output CSV files.')
    args = parser.parse_args()

    # Run tests
    test_pipeline(args.output_dir)
