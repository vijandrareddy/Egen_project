import pandas
import http.client as http
import datetime as dt
from google.cloud.pubsub_v1 import PublisherClient
from concurrent import futures

def hello_pubsub(event, context):
  message = call_data_stream_api()
  publish_message_to_topic(message)
  
def call_data_stream_api():
  conn = http.HTTPSConnection("data.lacity.org")
  conn.request("GET", "/resource/wjz9-h9np.json")
  response = conn.getresponse()
  response_data = response.read()
  print(response_data)
  return response_data


def publish_message_to_topic(message):
  project_id = "alert-impulse-317221"
  topic_id = "covid_data"
  publisher = PublisherClient()
  publish_futures = []
  topic_path = publisher.topic_path (project_id, topic_id)
  publish_future = publisher.publish(topic_path, message)
  publish_future.add_done_callback(get_callback(publish_future, message))
  publish_futures.append(publish_future)
  futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

def get_callback(publish_future, data):
  def callback(publish_future):
    try:
      print(publish_future.result(timeout=60))
    except futures.TimeoutError:
      print(f"Publishing {data} timed out.")

  return callback 
