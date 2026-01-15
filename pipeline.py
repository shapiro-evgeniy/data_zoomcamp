import sys
import pandas as pd

print("Python version:", sys.version)
print('arguments:', sys.argv)

month = int(sys.argv[1])
print(f'Processing data for month: {month}')

df = pd.DataFrame({
    'A': [1,2],
    'B': [3,4]
})

df.to_parquet(f'output_month_{month}.parquet')

print(df.head())