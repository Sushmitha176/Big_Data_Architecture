import pandas as pd
import time

def stream_data(df: pd.DataFrame, chunk_size: int = 10):
    """Yield chunks of data to simulate real-time streaming"""
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start:start + chunk_size]
        time.sleep(1)  # simulate delay
