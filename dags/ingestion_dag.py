from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from src.batch import publish_batch

default_args = {
  'owner': 'Shivu Donmardi Gorva',
  'start_date': datetime(2026, 11, 1),
  'retries': 0,
  'retry_delay': timedelta(minutes=5)
}

with DAG(
  'data_ingestion',
  default_args=default_args,
  description='A Dag to batch CTR data',
  schedule_interval='* * * * *',
  catchup=False,
  max_active_runs=1
) as dag:
  publish_batch_task = PythonOperator(
    task_id='publish_batch_task',
    python_callable=publish_batch
  )

  conf = {
    "batch_id": "{{ ti.xcom_pull(task_ids='publish_batch_task')['batch_id'] }}",
    "batch_size": "{{ ti.xcom_pull(task_ids='publish_batch_task')['batch_size'] }}",
    "file_path": "{{ ti.xcom_pull(task_ids='publish_batch_task')['file_path'] }}",
    "num_ads": "{{ ti.xcom_pull(task_ids='publish_batch_task')['num_ads'] }}",
  }

  trigger_ucb_task = TriggerDagRunOperator(
    task_id="trigger_ucb_task",
    trigger_dag_id="ucb_optimization",
    trigger_run_id=(
      "ucb_batch_{{ ti.xcom_pull(task_ids='publish_batch_task')['batch_id'] }}"
    ),
    conf=conf,
    wait_for_completion=False,
  )

  trigger_thompson_task = TriggerDagRunOperator(
    task_id="trigger_thompson_task",
    trigger_dag_id="thompson_sampling",
    trigger_run_id=(
      "thompson_batch_{{ ti.xcom_pull(task_ids='publish_batch_task')['batch_id'] }}"
    ),
    conf=conf,
    wait_for_completion=False,
  )

  publish_batch_task >> [trigger_ucb_task, trigger_thompson_task]

# If this script is run directly, allow command-line interaction with the DAG
if __name__ == "__main__":
  dag.test()