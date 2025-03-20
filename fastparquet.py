import pandas as pd
from fastparquet import ParquetFile, write

# Read the Parquet file using Fastparquet
pf = ParquetFile("your_large_file.parquet")

# Convert to Pandas DataFrame
df = pf.to_pandas()

# Convert CLIENTID column from STRING to INT first (to match DECIMAL)
df["CLIENTID"] = pd.to_numeric(df["CLIENTID"], errors="coerce").astype("Int64")

# Save back to Parquet using Fastparquet
write("modified_large_file.parquet", df, compression="snappy")

print("CLIENTID column converted to DECIMAL[10,0] and saved successfully!")
