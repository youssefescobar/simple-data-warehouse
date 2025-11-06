from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import BashOperator, PythonOperator
import sys

sys.path.append('/opt/airflow/dw')

default_args= {
    'description': 'A dag to automate ELT, warehouse pipeline',
    'start_date': datetime(2025, 11, 6),
    'catchup': False,
}

dag = DAG(
    dag_id='simple warehouse - elt orchestrator',
    default_args = default_args,
    schdule = timedelta(days=1)
)

with dag:
    task1 = PythonOperator(
        task_id = 'Extracting and loading data into bronze',
        python_callable
        
    )