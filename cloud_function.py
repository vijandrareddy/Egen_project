import base64
import pandas
from google.cloud.storage import Client  
import datetime

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    df = pandas.read_json(pubsub_message)
    print(df.head(5))
    df_final = data_cleansing(df)
    print(df_final.head(5))
    upload_to_Bucket(df_final)
    

def  data_cleansing(df):
       
    df_new = df.transpose()
    
    df_new.reset_index(level=0,inplace=True)
    df_new =df_new.rename(columns={'index': 'US_states'}) 
    df1 =df_new [['US_states','confirmed', 'recovered', 'deaths','lat', 'long','updated']]
    df1 =df1 .replace(to_replace ="All",value ="All States")
    return df1

def upload_to_Bucket(df1):
    current_date_time = datetime.datetime.now()
    current_date_time_formatted = current_date_time.strftime("%m-%d-%Y-%H:%M:%S")
    storage_client = Client()
    bucket = storage_client.get_bucket("project-covid")   
    blob=bucket.blob("Covid_Data_" + current_date_time_formatted + ".csv")
    blob.upload_from_string(data=df1.to_csv(index=False),content_type="text/csv")    
    
