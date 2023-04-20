from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from groups.group_downloads import download_tasks
from groups.group_transforms import transforms_tasks

with DAG('group_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
 
    downloads = download_tasks()
   
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
 
    transforms = transforms_tasks()
 
    downloads >> check_files >> transforms