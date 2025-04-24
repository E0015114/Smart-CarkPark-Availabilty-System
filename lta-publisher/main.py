# app.py
import requests
import json
from google.cloud import pubsub_v1

PROJECT_ID = "yourprojectid"
TOPIC_ID = "yourtopic"
API_KEY = "yourapikey"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def fetch_and_publish(request):
    url = "http://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2"
    headers = {"AccountKey": API_KEY, "accept": "application/json"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("value", [])

        for record in data:
            message = json.dumps(record).encode("utf-8")
            publisher.publish(topic_path, message)

        return f"✅ Published {len(data)} records.", 200
    except Exception as e:
        return f"❌ Error occurred: {e}", 500
