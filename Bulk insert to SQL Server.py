import os
import pyodbc
import time

server = r"servername"
db = r"CV Project"

conn_str = f"""
DRIVER={{SQL Server}};
SERVER={server};
DATABASE={db};
Trusted_Connection=yes
"""

# Path to the directory containing CSV files
csv_directory = r"path to files"

# Opening a connection to the database
cnxn = pyodbc.connect(conn_str)
cursor = cnxn.cursor()

# fix the start time
start_time = time.time()

# Create a new table in the database
table_name = "CryptoData"
create_table_query = f"""
CREATE TABLE {table_name} (
    [Name] VARCHAR(80),
    [Symbol] VARCHAR(80),
    [Date] DATE,
    [Open] FLOAT,
    [High] FLOAT,
    [Low] FLOAT,
    [Close] FLOAT,
    [Adj Close] FLOAT,
    [Volume] VARCHAR(80)
)
"""
cursor.execute(create_table_query)
cnxn.commit()
print(len(os.listdir(csv_directory)))
# Go through all the CSV files in the directory
for csv_file in os.listdir(csv_directory):
    if csv_file.endswith(".csv"):
        csv_file_path = os.path.join(csv_directory, csv_file)

        # Create a Bulk Insert SQL query
        bulk_insert_query = f"""
        BULK INSERT {table_name}
        FROM '{csv_file_path}'
        WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', FIRSTROW = 2)
        """

        # Bulk Insert
        cursor.execute(bulk_insert_query)
        cnxn.commit()

# Measuring the time after the import is completed
end_time = time.time()
elapsed_time = end_time - start_time

print(f"Import completed in {elapsed_time} seconds")
# Import completed in 36.28531527519226 seconds

# Close connection with db
cnxn.close()





