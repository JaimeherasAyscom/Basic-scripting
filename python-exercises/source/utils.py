# utils.py

import pandas as pd 

def csv_to_df(csv_file):
    return pd.read_csv(csv_file)


def drop_duplicates(df):
    return df.drop_duplicates()

def to_numeric(col):
    return pd.to_numeric(col, errors="coerce")

def to_lowercase(col):
    return col.str.strip().str.lower()