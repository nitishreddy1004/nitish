import pyarrow.parquet as pq
import pyarrow as pa

# Read the Parquet file
table = pq.read_table("your_large_file.parquet")

# Convert CLIENTID column from STRING to DECIMAL[10,0]
schema = table.schema.set(
    table.schema.get_field_index("CLIENTID"),
    pa.field("CLIENTID", pa.decimal128(10, 0))  # DECIMAL[10,0]
)

# Apply schema change by converting column values
new_columns = []
for col in table.column_names:
    if col == "CLIENTID":
        # Convert string to int first, then to decimal
        new_col = pa.array([int(x) if x else None for x in table[col].to_pandas()], pa.decimal128(10, 0))
    else:
        new_col = table[col]
    new_columns.append(new_col)

# Create new table with modified schema
new_table = pa.Table.from_arrays(new_columns, schema=schema)

# Write back to a new Parquet file
pq.write_table(new_table, "modified_large_file.parquet")

print("Conversion to DECIMAL[10,0] completed successfully!")
