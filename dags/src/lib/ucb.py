import numpy as np
from math import log, sqrt

def calculate_ucb(round_num, ad, state):
  if state["selection_counts"][ad] > 0:
    avg_reward = state["sum_of_rewards"][ad] / state["selection_counts"][ad]
    confidence_interval_deviation = sqrt((3 / 2) * log(round_num) / state["selection_counts"][ad])
    ucb = avg_reward + confidence_interval_deviation
  else:
    # Force each ad to be selected at least once
    ucb = float("inf")

  return ucb

# def analyze_convergence(nAds, states):
#   summary = {
#     "checkpoints": [],
#     "selected_ad": [],
#     "selection_percentage": [],
#   }

#   selection_history = []

#   for i, state in enumerate(states):
#     winning_ad = np.argmax(state["selection_counts"])

#     summary["checkpoint"].append(state["start_round"])
#     summary["selected_ad"].append(winning_ad + 1)
#     summary["selection_percentage"].append(
#       (state["selection_counts"][winning_ad] / np.sum(state["selection_counts"])) * 100
#     )

#     selection_history.append(state["selection_counts"])

#   selection_history = np.array(selection_history)

#   plt.figure(figsize=(12, 6))

#   plt.stackplot(
#     summary["checkpoints"],
#     selection_history.T,
#     labels=[f"Ad {i+1}" for i in range(nAds)]
#   )

#   plt.xlabel("Rounds")
#   plt.ylabel("Number of Selections")
#   plt.title("UCB Ad Selection Counts Over Time")

#   plt.legend(loc="upper left", bbox_to_anchor=(1.02, 1))
#   plt.tight_layout()
#   plt.show()
