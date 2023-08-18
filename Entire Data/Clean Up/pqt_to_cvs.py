import os
import pandas as pd

# Set working directory
path = '/Users/mau/Library/CloudStorage/Dropbox/Mac/Documents/Dissertation/Chapter 2/Entire_Data/By month'

# Iterate through the following folders: '2_June', '3_July', '4_August', '5_September', '6_October'
months = ['2_June', '3_July', '4_August', '5_September', '6_October']
for folder in months:
    # Set the working directory to the folder
    os.chdir(f'{path}/{folder}/Ending Balances/Per_Player')
    print(folder)

    # Iterate through each parquet file in the folder
    for file in os.listdir():
        # Check if the file has the .parquet extension
        if file.endswith('.parquet'):
            print(file)
            
            # Read the parquet file
            df = pd.read_parquet(file, engine='pyarrow')

            # Transfomr 'total_duration' from timedelta to seconds
            df['total_duration'] = df['total_duration'].dt.total_seconds()

            # Transfomr 'ave_time_per_machine' from timedelta to seconds
            df['ave_time_per_machine'] = df['ave_time_per_machine'].dt.total_seconds()
            
            # Save the csv file in the folder
            csv_file_name = file[:-8] + '.csv'
            df.to_csv(f'{path}/{folder}/Ending Balances/Per_Player/csv/{csv_file_name}', index=False)
  



