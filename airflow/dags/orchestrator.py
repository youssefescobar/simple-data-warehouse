from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator
import sys

sys.path.append('/opt/airflow/dw')

default_args= {
    'description': 'A dag to automate ELT, warehouse pipeline',
    'start_date': datetime(2025, 11, 6),
    'catchup': False,
}

DBT_PROJECT_DIR = "/opt/airflow/dw/xyz_store_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dw"


dag = DAG(
    dag_id='wh-elt-pipeline',
    default_args = default_args,
    schedule = timedelta(days=1)
)

with dag:
    task1 = BashOperator(
        task_id = 'Extracting-and-loading-bronze',
        bash_command="python /opt/airflow/dw/el.py"
    )
    
    task2 = BashOperator(
        task_id = 'dbt-run-models',
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt debug --profiles-dir {DBT_PROFILES_DIR} && \
            dbt run --profiles-dir {DBT_PROFILES_DIR}
        """
    )
    
    task3 = BashOperator(
        task_id = 'dbt-test-models',
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt debug --profiles-dir {DBT_PROFILES_DIR} && \
            dbt test --profiles-dir {DBT_PROFILES_DIR}
        """
    )
    
    task4 = BashOperator(
        task_id = 'dbt-return-sample', 
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt debug --profiles-dir {DBT_PROFILES_DIR} && \
            dbt show --select gold.dim_customers --limit 1 --profiles-dir {DBT_PROFILES_DIR}
        """
    )
    
    task1 >> task2 >> [task3, task4]