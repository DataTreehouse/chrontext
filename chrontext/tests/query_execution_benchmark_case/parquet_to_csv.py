import os
import pandas as pd

df_map = {}
folders = ["timeseries_double", "timeseries_boolean"]

for fold in folders:
    for root, subdirs, files in os.walk(fold):
        if len(files) > 0:
            split_root = str(root).split("/")
            print(split_root)
            identifier = split_root[-1]
            if identifier not in df_map:
                df_map[identifier] = []
            for f in files:
                df = pd.read_parquet(str(root) + "/" + f)[["value", "timestamp"]].copy()
                df_map[identifier].append(df)
                print(df)

for identifier in df_map:
    df = pd.concat(df_map[identifier])
    print(df)
    df.to_csv(identifier + ".csv", index=False)