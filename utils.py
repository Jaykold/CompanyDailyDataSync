import pandas as pd

def save_to_csv(df: pd.DataFrame, filepath: str):
    df.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")