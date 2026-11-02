# Ads CTR Optimization with Upper Confidence Bound (UCB) on Airflow

This project uses Apache Airflow to run an online ad selection experiment. It treats picking an ad as a **multi-armed bandit** problem and solves it with the **Upper Confidence Bound (UCB)** algorithm. Data does not arrive all at once. It comes in batches, as it would in production, and the UCB state carries over from one batch to the next.

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

The implementation is in [dags/src/lib/ucb.py](dags/src/lib/ucb.py).

### Simple breakdown of the steps

The pipeline is split into two DAGs.

**1. `data_ingestion`** ([dags/ingestion_dag.py](dags/ingestion_dag.py)): runs every 2 minutes (`*/2 * * * *`)

| Task | What it does |
|---|---|
| `publish_batch_task` | Reads the Airflow Variable `next_batch_index` and slices the next 500 rows from the dataset. It writes them to `data/batches/batch_XXX.csv`, increments the Variable, and returns the batch metadata (`batch_id`, `batch_size`, `file_path`, `num_ads`) through XCom. |
| `trigger_ucb_task` | Uses `TriggerDagRunOperator` to start the `ucb_optimization` DAG. It passes the batch metadata as the DAG run `conf` and uses a run ID of the form `ucb_batch_<batch_id>`. |

**2. `ucb_optimization`** ([dags/ucb_dag.py](dags/ucb_dag.py)): no schedule, runs only when triggered

```
load_batch_task ──┐
                  ├──> process_batch_task ──> save_state_task
load_state_task ──┘
```

| Task | What it does |
|---|---|
| `load_batch_task` | Reads the batch CSV named in the DAG run `conf` and passes it on through XCom as a pickled, base64-encoded DataFrame. |
| `load_state_task` | Loads the running UCB state (`sum_of_rewards`, `selection_counts`) from `state/ucb_state.json`. If the file does not exist yet, it creates a zeroed state sized to `num_ads`. |
| `process_batch_task` | For each row in the batch, computes the UCB for every ad, picks the ad with the highest value, reads its reward, and updates the state. The global round number is `batch_id * batch_size + row_index`, so the `ln(n)` term keeps growing across batches. |
| `save_state_task` | Writes the updated state back to `state/ucb_state.json` for the next batch to use. |

The ingestion DAG sends 500 rows every 2 minutes, so the full dataset is used up after **20 batches (about 40 minutes)**.

Example: after 7 batches (3,500 rounds), `state/ucb_state.json` looked like this:

```json
{
  "sum_of_rewards":   [50, 35, 6, 32, 379, 1, 25, 81, 19, 2],
  "selection_counts": [321, 262, 132, 249, 1465, 106, 220, 441, 193, 111]
}
```

**Ad 5** already has about 42% of all selections and the highest click-through rate (~26%). The algorithm is converging on the best ad.

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

5. **First** unpause the `ucb_optimization` DAG, **then** unpause the `data_ingestion` DAG.

   Order matters. DAGs start paused, and a DAG triggered while paused only queues the run instead of executing it. Once `data_ingestion` is unpaused, it publishes one batch every 2 minutes and each batch triggers a `ucb_optimization` run.

6. Watch progress in the UI, or check `state/ucb_state.json` to see the selection counts change after each batch.

### Resetting the experiment

`setup.sh` clears `state/` and `logs/`, but the `next_batch_index` Airflow Variable lives in the Postgres database. To start over completely:

```bash
docker compose down -v   # -v removes the Postgres volume, which resets next_batch_index
./setup.sh
```

Alternatively, delete or reset `next_batch_index` under **Admin → Variables** in the UI and delete `state/ucb_state.json`.

## Additional concepts explored

These concepts were explored in addition to the default lab included in the coursework.

- **Multiple DAGs**: ingestion and optimization are separate DAGs with separate responsibilities.
- **DAG context/config**: `ucb_optimization` reads its inputs from `dag_run.conf`, which is passed in by the triggering DAG.
- **Task variables**: the Airflow Variable `next_batch_index` keeps track of how far ingestion has progressed across runs. XCom passes data (batch metadata, the serialized DataFrame, and the UCB state) between tasks.
- **TriggerDagRunOperator**: one DAG triggers another, using Jinja-templated `conf` and run IDs.
- **Periodic runs**: the ingestion DAG runs on a cron schedule (`*/2 * * * *`) with `catchup=False` and `max_active_runs=1`.

## AI Help

- Used for polishing this README
- Used for understanding the airflow documentation
