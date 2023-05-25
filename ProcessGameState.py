import pyarrow.parquet as pq

def extract_data_from_parquet(file_path):
    # Open the Parquet file
    parquet_file = pq.ParquetFile(file_path)

    # Get the number of row groups in the Parquet file
    num_row_groups = parquet_file.num_row_groups

    # Iterate over each row group
    for row_group in range(num_row_groups):
        # Read the row group as a pandas DataFrame
        df = parquet_file.read_row_group(row_group).to_pandas()

        # Process the data as needed
        # Iterate over each row in the DataFrame
        for index, row in df.iterrows():
            # Access and process the values for each column
            column1_value = row['column1']  # Assuming 'column1' is the name of the column
            column2_value = row['column2']  # Assuming 'column2' is the name of the column
            column3_value = row['column3']  # Assuming 'column3' is the name of the column
            column4_value = row['column4']  # Assuming 'column4' is the name of the column

            # Print the values or perform any other processing
            print(column1_value)
            print(column2_value)
            print(column3_value)
            print(column4_value)

# Provide the file path of the Parquet file
parquet_file_path = 'C:\Users\sarrdah.k\PycharmProjects\EvilGenuisesAssignment\data\game_state_frame_data.parquet'

# Call the function to extract data from the Parquet file
extract_data_from_parquet(parquet_file_path)
