import pandas as pd
import os

def csv_to_parquet(csv_path, parquet_path=None):

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    if parquet_path is None:
        parquet_path = os.path.splitext(csv_path)[0] + ".parquet"

    df = pd.read_csv(csv_path)
    df.to_parquet(parquet_path, index=False)
    return parquet_path


parquet_file = csv_to_parquet("data/movies_crime.csv")
print(f"Saved Parquet file at: {parquet_file}")
