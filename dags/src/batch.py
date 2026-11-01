from datetime import datetime
from pathlib import Path

import pandas as pd

from airflow.models import Variable

DATASET_PATH = "/opt/airflow/data/Ads_CTR_Optimisation.csv"
OUTPUT_DIR = Path("/opt/airflow/data/batches")
BATCH_SIZE = 500

def publish_batch():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(DATASET_PATH)

    index = int(Variable.get("next_batch_index", default_var=0))

    start = index * BATCH_SIZE
    end = start + BATCH_SIZE

    if start >= len(df):
      print("All batches have been published")
      return

    batch = df.iloc[start:end]

    file_path = OUTPUT_DIR / f"batch_{index:03d}.csv"

    batch.to_csv(file_path, index=False)

    Variable.set("next_batch_index", index + 1)

    return {
      "batch_id": index,
      "batch_size": BATCH_SIZE,
      "file_path": str(file_path),
      "num_ads": df.shape[1]
    }