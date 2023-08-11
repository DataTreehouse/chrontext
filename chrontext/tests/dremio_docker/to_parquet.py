import pandas as pd
df = pd.read_csv("ts.csv")
df['ts'] = pd.to_datetime(df['ts'])
df.to_parquet("ts.parquet")