from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow import DAG


from user_etl import run_user_etl

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 22, 19, 10, 0, 0),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="spotify_dag",
    default_args=default_args,
    description="User ramdon extract",
    schedule_interval=timedelta(minutes=2),
)

run_etl = PythonOperator(
    task_id="ramdon_user_etl",
    python_callable=run_user_etl,
    dag=dag,
)

run_etl
