import pyarrow.parquet as pq
import pandas as pd

# Define the paths
input_parquet_path = r'C:\Users\LENOVO\Desktop\parquet_files\Parquets\mdm_siniestro_dif_20240611212129.parquet'
output_parquet_path = r'C:\Users\LENOVO\Desktop\parquet_files\Parquets\mdm_siniestro_dif_100_rows.parquet'

# Open the Parquet file for reading
pq_file = pq.ParquetFile(input_parquet_path)

# Get the schema of the Parquet file
schema = pq_file.schema

# Define the number of rows to extract
rows_to_extract = 100

# Initialize an empty list to store row groups
row_groups = []

# Loop through row groups until we have enough rows
for i in range(pq_file.num_row_groups):
    if len(row_groups) >= rows_to_extract:
        break
    # Read a row group into a Table
    table = pq_file.read_row_group(i, columns=[col.name for col in schema])
    # Convert the Table to a pandas DataFrame
    df = table.to_pandas()
    # Append the DataFrame to the list of row groups
    row_groups.append(df)

# Concatenate the list of DataFrames into a single DataFrame with up to 100 rows
df_100 = pd.concat(row_groups[:rows_to_extract], ignore_index=True)

# Write the selected rows to a new Parquet file
df_100.to_parquet(output_parquet_path, index=False, engine='pyarrow')

print(f"Extracted {df_100.shape[0]} rows and saved to {output_parquet_path}")
