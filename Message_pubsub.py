import pandas
import http.client as http
import datetime as dt
from google.cloud import pubsub_v1
from concurrent import futures
from  google.cloud import storage
import time
import schedule

class publish:
    def __init__(self):
        self.project_id = "alert-impulse-317221"
        self.topic_id = "covid_data"
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path (self.project_id, self.topic_id)
        self.publish_futures = []
        
    def call_data_stream_api(self):
        conn = http.HTTPSConnection("covid-api.mmediagroup.fr")
        conn.request("GET", "/v1/cases?ab=us")
        response = conn.getresponse()
        response_data = response.read()
        print(response_data)
        return response_data

    def publish_message_to_topic(self, message):
    
        publish_future = self.publisher.publish(self.topic_path, message)
        
        publish_future.add_done_callback(self.get_callback(publish_future, message))
        self.publish_futures.append(publish_future)
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)

    def get_callback(self,publish_future, data):
        def callback(publish_future):
            try:
                print(publish_future.result(timeout=60))
            except futures.TimeoutError:
                print(f"Publishing {data} timed out.")

        return callback 

    def call_scheduler(self):
       
        message = self.call_data_stream_api()
        self.publish_message_to_topic(message)

if __name__ == "__main__":
    k=publish()
    
    #schedule will run every 5mins. 
    schedule.every(5).minutes.do(k.call_scheduler)

    while True:
        # Checks whether a scheduled task is pending to run or not
        schedule.run_pending()
        time.sleep(1)

    
    


   
    
    

    
