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
        for _, row in df.iterrows():
            # Access and process the values for each attribute
            round_num = row['round_num']
            tick = row['tick']
            side = row['side']
            team = row['team']
            hp = row['hp']
            armor = row['armor']
            is_alive = row['is_alive']
            x = row['x']
            y = row['y']
            z = row['z']
            inventory = row['inventory']
            total_utility = row['total_utility']
            equipment_value = row['equipment_value_freezetime_end']
            area_name = row['area_name']
            seconds = row['seconds']
            clock_time = row['clock_time']
            talive = row['t_alive']
            ct_alive = row['ct_alive']
            bomb_planted = row['bomb_planted']
            map_name = row['map_name']
            utility_used = row['utility_used']
            player = row['player']

            # Print the values with labels
            print("Round Number:", round_num)
            print("Tick:", tick)
            print("Side:", side)
            print("Team:", team)
            print("HP:", hp)
            print("Armor:", armor)
            print("Is Alive:", is_alive)
            print("X Coordinate:", x)
            print("Y Coordinate:", y)
            print("Z Coordinate:", z)
            print("Inventory:", inventory)
            print("Total Utility:", total_utility)
            print("Equipment Value:", equipment_value)
            print("Area Name:", area_name)
            print("Seconds:", seconds)
            print("Clock Time:", clock_time)
            print("T Alive:", talive)
            print("CT Alive:", ct_alive)
            print("Bomb Planted:", bomb_planted)
            print("Map Name:", map_name)
            print("Utility Used:", utility_used)
            print("Player:", player)

# Provide the file path of the Parquet file
parquet_file_path = 'data/game_state_frame_data.parquet'

# Call the function to extract data from the Parquet file
extract_data_from_parquet(parquet_file_path)
