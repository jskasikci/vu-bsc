import pandas as pd
from datetime import datetime

parquet_file_path = 'output/simple/raw-output/0/seed=0/service.parquet'
server_df = pd.read_parquet(parquet_file_path)

min_timestamp = server_df['timestamp'].min()
max_timestamp = server_df['timestamp'].max()
makespan_ms = max_timestamp - min_timestamp

min_timestamp_date = datetime.utcfromtimestamp(min_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
max_timestamp_date = datetime.utcfromtimestamp(max_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

print(f"{makespan_ms}")
