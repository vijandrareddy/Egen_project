import base64
import pandas
from google.cloud.storage import Client  
import datetime

def hello_pubsub(event, context):

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    df = pandas.read_json(pubsub_message)
    print(df.head(5))
      

    current_date_time = datetime.datetime.now()
    current_date_time_formatted = current_date_time.strftime("%m-%d-%Y-%H%M%S")
    storage_client = Client()
    bucket = storage_client.get_bucket("project_egen")   
    blob=bucket.blob("new_data/parking_citation " + current_date_time_formatted + ".csv")
    blob.upload_from_string(data=df.to_csv(index=False),content_type="text/csv")    
