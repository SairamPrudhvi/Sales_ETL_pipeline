# etl/rejects.py

import os
from datetime import datetime
from typing import List
import pandas as pd


REJECTS_DIR = "rejected_data"
os.makedirs(REJECTS_DIR, exist_ok=True)


def save_rejected_batches(
    rejected_batches: List[pd.DataFrame],
    prefix: str = "rejected"
) -> None:
    """
    Save rejected DataFrames to CSV files with timestamp.
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    for idx, df in enumerate(rejected_batches, start=1):
        if df is None or df.empty:
            continue

        file_name = f"{prefix}_batch_{idx}_{timestamp}.csv"
        file_path = os.path.join(REJECTS_DIR, file_name)

        df.to_csv(file_path, index=False)
