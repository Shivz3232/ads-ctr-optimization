# Ads CTR Optimization with UCB and Thompson Sampling on Airflow

This project uses Apache Airflow to run an online ad selection experiment. It treats picking an ad as a **multi-armed bandit** problem and solves it with two algorithms side by side: **Upper Confidence Bound (UCB)** and **Thompson Sampling**. Data does not arrive all at once. It comes in batches, as it would in production, and each algorithm's state carries over from one batch to the next.

## What is the workflow doing

### Dataset

[data/Ads_CTR_Optimisation.csv](data/Ads_CTR_Optimisation.csv) has **10,000 rows** and **10 columns** (`Ad 1` … `Ad 10`).

- Each row is one user visit (one "round").
- Each cell is `1` if that user *would* click the ad if it were shown, and `0` otherwise.

In a real system we only see the reward for the ad we actually showed. The simulation follows that rule: in each round the algorithm picks one ad and only reads the cell for that ad.

### UCB formula used

For every round `n` and every ad `i`, we compute:

$$
\bar{r}_i(n) = \frac{R_i(n)}{N_i(n)}
\qquad
\Delta_i(n) = \sqrt{\frac{3}{2} \cdot \frac{\ln(n)}{N_i(n)}}
\qquad
UCB_i(n) = \bar{r}_i(n) + \Delta_i(n)
$$

where

- `R_i(n)` is the total reward (clicks) ad `i` has received so far (`sum_of_rewards`)
- `N_i(n)` is the number of times ad `i` has been selected so far (`selection_counts`)
- `n` is the global round number, counted across all batches

The ad with the highest `UCB_i(n)` is shown. If an ad has never been selected (`N_i = 0`), its UCB is set to infinity so that every ad gets tried at least once. The first term (average reward) rewards **exploitation**. The second term (confidence width) shrinks as an ad is shown more often, which drives **exploration** of ads we know less about.

The implementation is in [dags/src/lib/ucb.py](dags/src/lib/ucb.py) and [dags/src/ucb.py](dags/src/ucb.py).

### Thompson Sampling used

Thompson Sampling takes a Bayesian approach. Each ad's click-through rate is modeled as a Beta distribution built from the clicks and non-clicks it has received so far. For every round and every ad `i`, we draw a random sample:

$$
\theta_i \sim \text{Beta}\left(N_i^{1} + 1,\; N_i^{0} + 1\right)
$$

where

- `N_i^1` is the number of times ad `i` was clicked (`num_rewards_1`)
- `N_i^0` is the number of times ad `i` was shown and not clicked (`num_rewards_0`)

The ad with the highest sample `θ_i` is shown. The `+1` on both parameters gives a uniform `Beta(1, 1)` prior, so ads with no history are equally likely to win a round. As an ad collects data its distribution narrows around its true CTR: good ads keep producing high samples (**exploitation**), while uncertain ads still produce a high sample now and then (**exploration**). Unlike UCB, the choice is random rather than deterministic, and it does not need the global round number.

The implementation is in [dags/src/thompson.py](dags/src/thompson.py).

### Simple breakdown of the steps

The pipeline is split into three DAGs.

**1. `data_ingestion`** ([dags/ingestion_dag.py](dags/ingestion_dag.py)): runs every minute (`* * * * *`)

```
                      ┌──> trigger_ucb_task
publish_batch_task ───┤
                      └──> trigger_thompson_task
```

It reads the Airflow Variable `next_batch_index`, writes the next 500 rows to `data/batches/batch_XXX.csv`, and advances the Variable. It then triggers both optimization DAGs with the same batch metadata (`batch_id`, `batch_size`, `file_path`, `num_ads`) as the DAG run `conf`, using run IDs of the form `ucb_batch_<batch_id>` and `thompson_batch_<batch_id>`. Both algorithms therefore see exactly the same data.

**2. `ucb_optimization`** ([dags/ucb_dag.py](dags/ucb_dag.py)) and **3. `thompson_sampling`** ([dags/thompson_dag.py](dags/thompson_dag.py)): no schedule, run only when triggered

Both DAGs share the same shape:

```
load_batch_task ──┐
                  ├──> process_batch_task ──> save_state_task
load_state_task ──┘
```

Each run loads the batch named in `conf`, loads the algorithm's running state from `state/` (or creates a zeroed state on the first run), plays every row of the batch through the algorithm, and saves the updated state for the next batch.

| DAG | State file | State fields |
|---|---|---|
| `ucb_optimization` | `state/ucb_state.json` | `sum_of_rewards`, `selection_counts` |
| `thompson_sampling` | `state/thompson_state.json` | `selection_counts`, `num_rewards_1`, `num_rewards_0` |

The ingestion DAG sends 500 rows every minute, so the full dataset is used up after **20 batches (about 20 minutes)**.

### Results

Example: after 5 batches (2,500 rounds), the two state files looked like this.

`state/ucb_state.json`:

```json
{
  "sum_of_rewards":   [39, 33, 6, 13, 238, 1, 22, 51, 18, 2],
  "selection_counts": [252, 229, 116, 148, 916, 91, 186, 297, 169, 96]
}
```

`state/thompson_state.json`:

```json
{
  "ad":               [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  "selection_counts": [82, 77, 55, 26, 2018, 19, 34, 122, 41, 26],
  "num_rewards_0":    [70, 65, 49, 25, 1470, 19, 32, 99, 37, 25],
  "num_rewards_1":    [12, 12, 6, 1, 548, 0, 2, 23, 4, 1]
}
```

| | UCB | Thompson Sampling |
|---|---|---|
| Share of rounds on Ad 5 | 916 / 2,500 (~37%) | 2,018 / 2,500 (~81%) |
| Total clicks | 423 | 609 |
| Overall CTR | ~16.9% | ~24.4% |

Both algorithms identify **Ad 5** as the best ad, but Thompson Sampling commits to it much sooner and earns about 44% more clicks over the same 2,500 rounds. UCB spends more rounds exploring because its confidence bonus shrinks slowly.

### Note on real-world usage

In a real system, the data ingestion DAG would not slice a static CSV. It would be triggered by an external source, such as a message queue, an event stream, or a sensor watching a storage bucket, or it would at least read from one. This project simulates that arrival pattern with a scheduled DAG that sends one batch at a time.

## How to run

**Prerequisites:** Docker and Docker Compose, with at least 4 GB of memory allocated to Docker.

1. Prepare the environment. This creates the `logs/` and `state/` folders and writes your `AIRFLOW_UID` to `.env`:

   ```bash
   ./setup.sh
   ```

2. Initialize the Airflow database and create the admin user:

   ```bash
   docker compose up airflow-init
   ```

3. Start Airflow:

   ```bash
   docker compose up
   ```

4. Open the Airflow UI at [http://localhost:8080](http://localhost:8080) and log in with `airflow` / `airflow`.

5. **First** unpause the `ucb_optimization` and `thompson_sampling` DAGs, **then** unpause the `data_ingestion` DAG.

   Order matters. DAGs start paused, and a DAG triggered while paused only queues the run instead of executing it. Once `data_ingestion` is unpaused, it publishes one batch every minute and each batch triggers one run of each optimization DAG.

6. Watch progress in the UI, or check `state/ucb_state.json` and `state/thompson_state.json` to see the selection counts change after each batch.

### Resetting the experiment

`setup.sh` clears `state/` and `logs/`, but the `next_batch_index` Airflow Variable lives in the Postgres database. To start over completely:

```bash
docker compose down -v   # -v removes the Postgres volume, which resets next_batch_index
./setup.sh
```

Alternatively, delete or reset `next_batch_index` under **Admin → Variables** in the UI and delete both files in `state/`.

## Additional concepts explored

These concepts were explored in addition to the default lab included in the coursework.

- **Multiple DAGs**: ingestion and the two optimization algorithms are separate DAGs with separate responsibilities.
- **DAG context/config**: `ucb_optimization` and `thompson_sampling` read their inputs from `dag_run.conf`, which is passed in by the triggering DAG.
- **Task variables**: the Airflow Variable `next_batch_index` keeps track of how far ingestion has progressed across runs. XCom passes data (batch metadata, the serialized DataFrame, and the algorithm state) between tasks.
- **TriggerDagRunOperator**: one DAG fans out to trigger two others, using Jinja-templated `conf` and run IDs.
- **Periodic runs**: the ingestion DAG runs on a cron schedule (`* * * * *`) with `catchup=False` and `max_active_runs=1`.

## AI Help

- Used for polishing this README
- Used for understanding the airflow documentation
