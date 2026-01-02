from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys

BASE_DIR = "/mnt/c/Users/ayush/python_data_engineering/Advanced_ETL/ETL_Pipeline"
sys.path.insert(0,BASE_DIR)

from tryspark import main as spark_main

with DAG(
        dag_id = "ETL_Task",
        start_date = datetime(2025,1,1),
        schedule = None,
        catchup = None,
    
    )as dag:

    spark_task = PythonOperator(
        task_id = "Training_Demo",
        python_callable = spark_main,
        op_args = [
           f"{BASE_DIR}/input",
           f"{BASE_DIR}/processed",
           ]
 )

