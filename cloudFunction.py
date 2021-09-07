import base64
import json
import pandas as pd
import logging 
from google.cloud import storage 
def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    json_data = json.loads(pubsub_message)
    
    df = pd.json_normalize(json_data)
    storage_client = storage.Client()
   
    bucket = storage_client.get_bucket('twitter-data-1059')
    blob = bucket.blob('twitter_data{}.csv'.format(context.timestamp))
    blob.upload_from_string(data = df.to_csv(index = False), content_type = 'text/csv')
