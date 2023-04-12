import os
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Set working directory
path = '/Volumes/Seagate Portable Drive/SlotData2'
os.chdir(path)

filter = ['starteventdatetime', 'endeventdatetime', 'playercashableamt',
       'wageredamt', 'coinout', 'grosswin', 'theoreticalwin',
       'cardedwageredcashableamt', 'egmpaidgamewonamt', 'handpaidgamewonamt', 
       'currencyinamt', 'ticketvaluein', 'ticketsin', 'ticketvalueout', 'ticketsout', 'maxbet',
       'slotdenominationname', 'assetnumber', 'zone', 'bank', 'paytablekey', 'theoreticalpaybackpercent',
       'playerkey', 'age', 'rank', 'gender', 'zipcode', 'BirthYear']

itr = pd.read_stata('Handle Pulls and Player Demographic Data.dta', columns=filter, chunksize=1000000)

# Function to reduce memory usage of a dataframe
def reduce_mem_usage(df):
    start_mem = df.memory_usage().sum() / 1024 ** 2
    print('Memory usage of dataframe is {:.2f} MB'.format(start_mem))
    
    # Iterate through each column of the dataframe
    for col in df.columns:
        col_type = df[col].dtype
        
        # If the column is not an object type, a category type or a datetime type
        if col_type != object and col_type.name != 'category' and 'datetime' not in col_type.name:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)  
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float32)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)

    end_mem = df.memory_usage().sum() / 1024 ** 2
    print('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
    print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))
    
    return df

# Set working directory to save the dataframe
path = '/Users/mau/Library/CloudStorage/Dropbox/Mac/Documents/Dissertation/Chapter 2/Entire_Data_2'
os.chdir(path)

# Define an empty list to store the parquet file paths:
output_paths = []

# Iterate through each dataframe in the list
for i, df in enumerate(itr):
    # Reduce the memory usage of the dataframe
    df = reduce_mem_usage(df)
    
    # Convert the chunk to an Arrow table
    table = pa.Table.from_pandas(df)
    # Define the output path for the parquet file
    output_path = f"output_chunk_{i}.parquet"
    # Write the table to a compressed parquet file
    pq.write_table(table, output_path, compression="snappy")
    # Add the output path to the list of output paths
    output_paths.append(output_path)

