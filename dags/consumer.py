from datetime import datetime

from airflow.decorators import task

from airflow import DAG, Dataset

my_file = Dataset(uri="/tmp/my_file.txt")


with DAG("consumer", schedule=[my_file], start_date=datetime(2023, 3, 1), catchup=False):

    @task
    def read_dataset():
        with open(my_file.uri, "r") as f:
            print(f.read())

    read_dataset()
