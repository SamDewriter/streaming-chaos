import base64
import json
from fastapi import FastAPI, Request, Response

app = FastAPI(title="Pub/Sub Push Subscriber", version="1.0")

@app.post("/pubsub/push")
async def pubsub_push(request: Request):
    """Expects a Pub/Sub push message and logs the data payload."""
    body = await request.json()
    msg = body.get("message", {})

    # Decode the Pub/Sub message data
    data_b64 = msg.get("data", "")
    payload = {}
    if data_b64:
        payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))
    
    attrs = msg.get("attributes", {})
    msg_id = msg.get("messageId", "N/A")

    print(f"messageId: {msg_id} | attributes: {attrs} | payload: {payload}")

    if payload.get("temperature", 0) >= 32:
        print("ALERT: High temperature detected!")

    return Response(status_code=204)