import pandas as pd
import pyarrow.parquet as pq

def convert_parquet_to_csv(parquet_file_path, csv_file_path):
    # Read the Parquet file as a pandas DataFrame
    df = pq.read_table(parquet_file_path).to_pandas()

    # Convert DataFrame to CSV
    df.to_csv(csv_file_path, index=False)

# Provide the file paths for the Parquet and CSV files
parquet_file_path = 'data/game_state_frame_data.parquet'
csv_file_path = 'data/game_state_frame_data.csv'

# Call the function to convert Parquet to CSV
convert_parquet_to_csv(parquet_file_path, csv_file_path)
