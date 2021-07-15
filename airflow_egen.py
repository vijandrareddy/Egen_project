import os

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from google.cloud.storage import Client
from io import BytesIO
import datetime
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

DATASET_NAME = "egen"
TABLE_NAME = "parking_citation"

#python code


dag = models.DAG(
    dag_id='airflow',
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['example'],
)

def create_dataset_if_not_exists():
    client = bigquery.Client()
    projectid = "alert-impulse-317221"
    dataset_id =  "{}.egen".format(projectid)
    try:
        client.get_dataset(dataset_id)  
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, timeout=30)  

def create_table_if_not_exists():
    client = bigquery.Client()
    table_id = "alert-impulse-317221.egen.parking_citations"
    
    try:
        client.get_table(table_id)
    except NotFound:
        schema =[
            bigquery.SchemaField('ticket_number', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('issue_date', 'TIMESTAMP',  mode ='NULLABLE'),
            bigquery.SchemaField('issue_time', 'TIMESTAMP',  mode ='NULLABLE'),
            bigquery.SchemaField('rp_state_plate', 'STRING',  mode ='NULLABLE'),
            bigquery.SchemaField('plate_expiry_date', 'FLOAT',  mode ='NULLABLE'),
            bigquery.SchemaField('make', 'STRING',  mode ='NULLABLE'),
            bigquery.SchemaField('body_style', 'STRING',  mode ='NULLABLE'),
            bigquery.SchemaField('color', 'STRING',  mode ='NULLABLE'),
            bigquery.SchemaField('location', 'STRING',  mode ='NULLABLE'),
            bigquery.SchemaField('route', 'STRING',  mode ='NULLABLE'),
            bigquery.SchemaField('agency', 'INTEGER',  mode ='NULLABLE'),
            bigquery.SchemaField('violation_code', 'STRING',  mode ='NULLABLE'),
            bigquery.SchemaField('violation_description', 'STRING',  mode ='NULLABLE'),
            bigquery.SchemaField('fine_amount', 'FLOAT',  mode ='NULLABLE'),
            bigquery.SchemaField('latitude', 'FLOAT',  mode ='NULLABLE'),
            bigquery.SchemaField('longitude', 'FLOAT',  mode ='NULLABLE'),
            bigquery.SchemaField('meter_id', 'STRING',  mode ='NULLABLE'),
            bigquery.SchemaField('marked_time', 'TIMESTAMP',  mode ='NULLABLE')
        ]
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  
    
    

def read_and_transform_data() :

    df_final = pd.DataFrame()
    storage_client = storage.Client()
    bucket_name = 'project-covid'
    bucket = storage_client.get_bucket(bucket_name)
    blobs_all = list(bucket.list_blobs())
    blobs_specific = list(bucket.list_blobs(prefix='raw_data'))
    blobs_specific.pop(0)
    
    for blobs in blobs_specific:  
        blob_name = str(blobs.name)
        blob = bucket.get_blob(blob_name)
        content = blob.download_as_string()
        df = pd.read_csv(BytesIO(content))
        print(df.head(5))
        df_final.append(df)
        
    if not df_final.empty:
        print("Inside loading")
        current_date_time = datetime.datetime.now()
        current_date_time_formatted = current_date_time.strftime("%m-%d-%Y-%H:%M:%S")
        bucket = storage_client.get_bucket("us-central1-egenproject-1a54d97d-bucket")   
        blob=bucket.blob("data/parking_data" + ".csv")
        blob.upload_from_string(data=df_final.to_csv(index=False),content_type="text/csv") 
        print(df_final)
   

def move_raw_files_to_historical_folder() :
    bucket_name="project-covid"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs_all = list(bucket.list_blobs())
    blobs_specific = list(bucket.list_blobs(prefix='raw_data'))
    blobs_specific.pop(0)
    
    
    for blobs in blobs_specific:  
        blob_name = str(blobs.name)
        source_bucket = storage_client.get_bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        new_blob_name = "update/" +  blob_name[blob_name.index("/")+1:]
        new_blob = source_bucket.copy_blob(source_blob, source_bucket, new_blob_name)
        source_blob.delete()
        print(f'File moved from {source_blob} to {new_blob_name}')
   
def delete_files_from_data_folder():
    bucket_name="us-central1-egenproject-1a54d97d-bucket"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs_all = list(bucket.list_blobs())
    blobs_specific = list(bucket.list_blobs(prefix='data'))
    blobs_specific.pop(0)
    
    for blobs in blobs_specific:  
        blob_name = str(blobs.name)
        source_blob = bucket.blob(blob_name)
        source_blob.delete()
        print(f'File deleted from {source_blob}')

# create_test_dataset = BigQueryCreateEmptyDatasetOperator(
#     task_id='create_airflow_test_dataset', dataset_id=DATASET_NAME, dag=dag
# )


t1 = PythonOperator(
    task_id='create_dataset_if_not_exists',
    python_callable= create_dataset_if_not_exists ,
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_table_if_not_exists',
    python_callable= create_table_if_not_exists ,
    dag=dag,
)

t3 = PythonOperator(
    task_id='read_and_transform_data',
    python_callable= read_and_transform_data ,
    dag=dag,
)

t4 = PythonOperator(
    task_id='move_raw_files_to_historical_folder',
    python_callable= move_raw_files_to_historical_folder ,
    dag=dag,
)



load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket='us-central1-egenproject-1a54d97d-bucket',
    source_objects=['data/parking_data.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    # schema_fields=[
    #     {'name': 'ticket_number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'issue_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    #     {'name': 'issue_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    #     {'name': 'rp_state_plate', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'plate_expiry_date', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #     {'name': 'make', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'body_style', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'color', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'route', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'agency', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'violation_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'violation_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'fine_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #     {'name': 'latitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #     {'name': 'longitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #     {'name': 'meter_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'marked_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},

    # ],
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
    skip_leading_rows = 1
)


t6 = PythonOperator(
    task_id='delete_data',
    python_callable=  delete_files_from_data_folder,
    dag=dag,
)

t1 >>t2 >> t3>> t4 >> load_csv >>t6