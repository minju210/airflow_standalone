import pandas as pd
import sys

READ_PATH = sys.argv[1]
SAVE_PATH = sys.argv[2]

df = pd.read_csv(READ_PATH, 
                 on_bad_lines='skip', 
                 names=['dt', 'cmd', 'cnt'])

df['dt'] = df['dt'].str.replace('^', '')
df['cmd'] = df['cmd'].str.replace('^', '')
df['cnt'] = df['cnt'].str.replace('^', '')

df['cnt'] = pd.to_numeric(df['cnt'], errors='coerce')

df['cnt'] = df['cnt'].fillna(0).astype(int)

df.to_parquet(f'{SAVE_PATH}', partition_cols=['dt'])
