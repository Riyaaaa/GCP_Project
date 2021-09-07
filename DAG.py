import pandas as pd
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from google.cloud import storage
from google.cloud import bigquery

#Now I shall define the functions for the DAG
DAG_NAME = 'twitterDAG'

default_args = {
    "depends_on_past": False,
    "email":[],
    "email_on_failure": False,
    "email_on_retry" : False,
    "owner": "riya",
    "retries" : 3,
    "retry_delay": timedelta(minutes = 2),
    "start_date" : datetime(2021, 8, 31)
}

dag = DAG(
    dag_id = "twitterDAG",
    default_args = default_args,
    schedule_interval = '*/5 * * * *',
    max_active_runs = 1

)


def merge_chunks():
    storage_client = storage.Client()
    bucket_name = "consumed-twitter-data1"
    blobs = storage_client.list_blobs(bucket_name)
    merged = pd.DataFrame()
    for blob in blobs:
        file_name = blob.name
        print(file_name)
        blob.download_to_filename(f'/home/airflow/gcs/data/{file_name}')
        print('Combing data chunks')
        chunks = pd.read_csv(f'/home/airflow/gcs/data/{file_name}')
        merged = merged.append(chunks)
        
    merged.to_csv('/home/airflow/gcs/data/merged.csv', index = False)


def bucket_to_bq():
    client = bigquery.Client()
    table_id = "tensile-pier-322516.tweetsdata.tweet_info"
    job_config = bigquery.LoadJobConfig(
        schema = [
                bigquery.SchemaField("id", "NUMERIC"),
                bigquery.SchemaField("text", "STRING"),
                
        ],
        
        skip_leading_rows = 1,
        source_format = bigquery.SourceFormat.CSV,
        allow_quoted_newlines = True
    )
    uri = "gs://us-central1-newcomposer-ff97357e-bucket/data/merged.csv"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config = job_config
    )
    load_job.result()
    destination_table = client.get_table(table_id)
    print('Loaded {} rows'.format(destination_table.num_rows))


start = DummyOperator(task_id = "start", dag = dag)

task_1_merge_chunks = PythonOperator(
    task_id = "merge_chunks_1",
    python_callable = merge_chunks,
    dag = dag
)
task_2_bucket_to_bq = PythonOperator(
    task_id = "bucket_to_bq_2",
    python_callable = bucket_to_bq,
    dag = dag
)

end = DummyOperator(task_id = "end", dag = dag)

start >> task_1_merge_chunks >> task_2_bucket_to_bq >> end



