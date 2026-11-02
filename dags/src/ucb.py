import os
import pickle
import base64
import json
from pathlib import Path

import pandas as pd

from src.lib.ucb import calculate_ucb

STATE_PATH = (
  Path(__file__).resolve().parent
  / "../../state/ucb_state.json"
).resolve()

def load_batch(**context):
  conf = context["dag_run"].conf

  batch_id = int(conf["batch_id"])
  file_path = conf["file_path"]

  df = pd.read_csv(file_path)
  serialized_data = pickle.dumps(df)

  return base64.b64encode(serialized_data).decode("ascii")

def load_state(**context):
  STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

  if STATE_PATH.exists():
    state = json.loads(STATE_PATH.read_text())
  else:
    conf = context["dag_run"].conf

    num_ads = int(conf["num_ads"])

    state = {
      "sum_of_rewards": [0] * num_ads,
      "selection_counts": [0] * num_ads
    }

  return state

def process_batch(data_b64, state, **context):
  conf = context["dag_run"].conf

  num_experiments_completed = int(conf["batch_id"]) * int(conf["batch_size"])
  nAds = int(conf["num_ads"])

  data_bytes = base64.b64decode(data_b64)
  batch = pickle.loads(data_bytes)

  for n in range(len(batch)):
    max_ucb = -float("inf")
    selected_ad = -1

    # Find the ad with the highest Upper Confidence Bound
    for ad in range(nAds):
      ucb = calculate_ucb(num_experiments_completed + n, ad, state)

      if ucb > max_ucb:
        max_ucb = ucb
        selected_ad = ad

    if (selected_ad == -1):
        raise "No ad was selected!"

    reward = int(batch.iloc[n, selected_ad]) # 0 or 1

    state["sum_of_rewards"][selected_ad] += reward
    state["selection_counts"][selected_ad] += 1

  return state

def save_state(state):
  STATE_PATH.write_text(
    json.dumps(state)
  )
