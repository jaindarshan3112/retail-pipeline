import pandas as pd

df = pd.read_parquet(r"C:\Users\divya\retail-pipeline\local_data\extracts\20260524T201412\order_items.parquet")
print(df.dtypes)
print(df["created_at"].head(3))