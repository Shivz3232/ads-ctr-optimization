from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.thompson import load_batch, load_state, process_batch, save_state

default_args = {
  'owner': 'Shivu Donmardi Gorva',
  'start_date': datetime(2026, 11, 1),
  'retries': 0,
  'retry_delay': timedelta(minutes=5)
}

with DAG(
  'thompson_sampling',
  default_args=default_args,
  description='',
  catchup=False,
  schedule_interval=None,
  max_active_runs=1
) as dag:
  load_batch_task = PythonOperator(
    task_id='load_batch_task',
    python_callable=load_batch
  )

  load_state_task = PythonOperator(
    task_id='load_state_task',
    python_callable=load_state
  )

  process_batch_task = PythonOperator(
    task_id='process_batch_task',
    python_callable=process_batch,
    op_args=[load_batch_task.output, load_state_task.output]
  )

  save_state_task = PythonOperator(
    task_id='save_state_task',
    python_callable=save_state,
    op_args=[process_batch_task.output]
  )

  [load_batch_task, load_state_task] >> process_batch_task >> save_state_task


# If this script is run directly, allow command-line interaction with the DAG
if __name__ == "__main__":
    dag.test()