import os
from airflow import models
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
TABLE_NAME = "parking_citation_staging"


dag = models.DAG(
    dag_id='parking_citation_airflow',
    start_date=days_ago(2),
    
    schedule_interval='0 7 * * *',
    tags=['egen'],
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
    table_id = "alert-impulse-317221.egen.parking_citation"
    
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

    df_list=[]
    storage_client = storage.Client()
    bucket_name = 'project_egen'
    bucket = storage_client.get_bucket(bucket_name)
    blobs_all = list(bucket.list_blobs())
    blobs_specific = list(bucket.list_blobs(prefix='new_data'))
    blobs_specific.pop(0)
    
    for blobs in blobs_specific:  
        blob_name = str(blobs.name)
        blob = bucket.get_blob(blob_name)
        content = blob.download_as_string()              
        df = pd.read_csv(BytesIO(content))
        print(df.head(5))
        df=df.drop(["meter_id","marked_time"], axis = 1)
        df=df.dropna()
        df_list.append(df)
    df_final = pd.concat(df_list, axis=0, ignore_index=True)
    
    print(df_final)
        
    if not df_final.empty:
        print("Inside loading")
        current_date_time = datetime.datetime.now()
        current_date_time_formatted = current_date_time.strftime("%m-%d-%Y-%H:%M:%S")
        bucket = storage_client.get_bucket("us-central1-egenproject-1a54d97d-bucket")   
        blob=bucket.blob("data/parking_data" + ".csv")
        blob.upload_from_string(data=df_final.to_csv(index=False),content_type="text/csv") 
        print(df_final)

   

def move_raw_files_to_historical_folder() :
    bucket_name="project_egen"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs_all = list(bucket.list_blobs())
    blobs_specific = list(bucket.list_blobs(prefix='new_data'))
    blobs_specific.pop(0)
    
    
    for blobs in blobs_specific:  
        blob_name = str(blobs.name)
        source_bucket = storage_client.get_bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        new_blob_name = "historical_data/" +  blob_name[blob_name.index("/")+1:]
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



def create_staging_table_if_not_exists():
    client = bigquery.Client()
    table_id = "alert-impulse-317221.egen.parking_citation_staging"
    
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



def load_data_from_staging():
    client = bigquery.Client()
    query_job = client.query(
        
        """ 
        merge into `alert-impulse-317221.egen.parking_citation` as a
        using `alert-impulse-317221.egen.parking_citation_staging`  as b
        on a.ticket_number = b.ticket_number
        when not matched by target then 
        insert(ticket_number,issue_date,issue_time,rp_state_plate,plate_expiry_date,make,body_style,color,location,route,agency,violation_code,violation_description,fine_amount,latitude,longitude) 
        values(b.ticket_number,b.issue_date,b.issue_time,b.rp_state_plate,b.plate_expiry_date,b.make,b.body_style,b.color,b.location,b.route,b.agency,b.violation_code,b.violation_description,b.fine_amount,b.latitude,b.longitude)  ;


        """
    )
        
       

task_1 = PythonOperator(
    task_id='create_dataset_if_not_exists',
    python_callable= create_dataset_if_not_exists ,
    dag=dag,
)
task_2 = PythonOperator(
    task_id='create_staging_table_if_not_exists',
    python_callable= create_staging_table_if_not_exists ,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='create_table_if_not_exists',
    python_callable= create_table_if_not_exists ,
    dag=dag,
)

task_4 = PythonOperator(
    task_id='read_and_transform_data',
    python_callable= read_and_transform_data ,
    dag=dag,
)

task_5 = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket='us-central1-egenproject-1a54d97d-bucket',
    source_objects=['data/parking_data.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
    skip_leading_rows = 1
)
task_6 = PythonOperator(
    task_id='load_data_from_staging',
    python_callable= load_data_from_staging ,
    dag=dag,
)

task_7 = PythonOperator(
    task_id='move_raw_files_to_historical_folder',
    python_callable= move_raw_files_to_historical_folder ,
    dag=dag,
)

task_8 = PythonOperator(
    task_id='delete_data',
    python_callable=  delete_files_from_data_folder,
    dag=dag,
)


task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6 >> task_7 >> task_8
