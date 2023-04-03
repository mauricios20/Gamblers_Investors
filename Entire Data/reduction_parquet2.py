import pyarrow.parquet as pq
import pyarrow as pa
import os
import pandas as pd

# Set working directory
path = "/Users/mau/Library/CloudStorage/Dropbox/Mac/Documents/Dissertation/Chapter 2/Entire_Data_2"
os.chdir(path)


# Load data into a DataFrame
dtf = pd.read_parquet("output_chunk_32.parquet")

print(dtf['gender'].unique())
print(dtf['age'].unique())
print(dtf['BirthYear'].unique())

print(dtf['playerkey'].unique())

print(dtf.head(4))


