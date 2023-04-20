from datetime import datetime

from airflow.decorators import task

from airflow import DAG, Dataset

my_file = Dataset(uri="/tmp/my_file.txt")


with DAG("producer", schedule="@daily", start_date=datetime(2023, 3, 1), catchup=False):
    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, "a+") as f:
            f.write("producer update")

    update_dataset()
