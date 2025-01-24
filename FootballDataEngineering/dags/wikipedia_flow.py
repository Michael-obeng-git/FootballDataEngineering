import pandas as pd
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email import EmailOperator
from airflow.datasets import Dataset
from airflow.hooks.postgres_hook import PostgresHook
import logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.wikipedia_pipelines import extract_wikipedia_data, transform_wikipedia_data, load_data_to_postgres, write_wikipedia_data


# Define the Airflow DAG
dag = DAG(
    dag_id='wikipedia_flow',
    default_args={
        "owner": "michael",
        "start_date": datetime(2023, 10, 1),
    },
    schedule_interval=None,
    catchup=False
)


# Task to extract data from wikipedia
extract_data_from_wikipedia = PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    provide_context=True,
    # op_kwargs={'url': "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},  # Pass the function
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag=dag
)

# Task to transform the extracted data
transform_wikipedia_data = PythonOperator(
    task_id='transform_wikipedia_data',
    provide_context=True,
    python_callable=transform_wikipedia_data,
    dag=dag
)

# Task to load the transformed data to Postgres
load_data_to_postgres = PythonOperator(
    task_id='load_data_to_postgres',
    provide_context=True,
    python_callable=load_data_to_postgres,
    dag=dag
)

#Task to write the transformed data to Azure Blob Storage
write_wikipedia_data = PythonOperator(
    task_id='write_wikipedia_data',
    provide_context=True,
    python_callable=write_wikipedia_data,
    dag=dag
)

# Define the task dependencies
extract_data_from_wikipedia >> transform_wikipedia_data >> [write_wikipedia_data, load_data_to_postgres]
