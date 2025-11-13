import os
import random
import asyncio
import json
from datetime import datetime, timezone
from dotenv import load_dotenv

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPICallError

load_dotenv()

PROJECT_ID = "class-demo-478108"
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")
RATE_SECS = float(os.environ.get("RATE_SECS", 1.0))

print(f"Project ID: {PROJECT_ID} | Pub/Sub Topic: {PUBSUB_TOPIC} | Rate (secs): {RATE_SECS}")


# Pub/Sub Setup
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)

app = FastAPI(title="WebSocket Pub/Sub Publisher", version="1.0")

async def generate_sensor_event():
    sensor_ids = ['AI', 'A2', 'B1', 'B2', 'C1', 'C2']
    return {
        'sensor': random.choice(sensor_ids),
        'temperature': round(random.uniform(20.0, 30.0), 2),
        'humidity': round(random.uniform(30.0, 90.0), 2),
        'time': datetime.now(timezone.utc).strftime('%H:%M:%S')
    }

def publish_to_pubsub(event: dict):
    data = json.dumps(event).encode('utf-8')
    try:
        future = publisher.publish(topic_path, data,
                                   source='ws-sensor-generator',
                                   kind='sensor-event')
        msg_id = future.result()
        print(f"Published {event} with ID: {msg_id}")
    except GoogleAPICallError as e:
        print(f"Failed to publish message: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

@app.websocket("/ws/sensor")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("WebSocket connection accepted")
    try:
        while True:
            event = await generate_sensor_event()
            publish_to_pubsub(event)
            await websocket.send_json(event)
            await asyncio.sleep(RATE_SECS)
    except WebSocketDisconnect:
        print("WebSocket connection closed")
    except Exception as e:
        print(f"Error during WebSocket communication: {e}")
        await websocket.close()