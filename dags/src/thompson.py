import os
import pickle
import base64
import json
import random
from pathlib import Path

import pandas as pd

from src.lib.ucb import calculate_ucb

STATE_PATH = (
  Path(__file__).resolve().parent
  / "../../state/thompson_state.json"
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
      "ad": [i for i in range(1, num_ads + 1)],
      "selection_counts": [0] * num_ads,
      "num_rewards_0": [0] * num_ads,
      "num_rewards_1": [0] * num_ads
    }

  return state

def process_batch(data_b64, state, **context):
  conf = context["dag_run"].conf

  num_experiments_completed = int(conf["batch_id"]) * int(conf["batch_size"])
  nAds = int(conf["num_ads"])

  data_bytes = base64.b64decode(data_b64)
  batch = pickle.loads(data_bytes)

  for n in range(len(batch)):

    highest_sample = -float("inf")
    selected_ad = 0

    for ad in range(nAds):
      sample = random.betavariate(
        alpha=state["num_rewards_1"][ad] + 1,
        beta=state["num_rewards_0"][ad] + 1,
      )

      if sample > highest_sample:
        highest_sample = sample
        selected_ad = ad

    reward = int(batch.iloc[n, selected_ad]) # int() is to avoid serialization errors of numpy.int_32

    state["selection_counts"][selected_ad] += 1

    if reward == 0:
      state["num_rewards_0"][selected_ad] += 1
    else:
      state["num_rewards_1"][selected_ad] += 1

  return state

def save_state(state):
  STATE_PATH.write_text(
    json.dumps(state)
  )
