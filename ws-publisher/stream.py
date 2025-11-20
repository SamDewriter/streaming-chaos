import os
import json
import asyncio
import signal
import logging
from typing import Any

from dotenv import load_dotenv
load_dotenv()

import websockets  # pip install websockets
from google.cloud import pubsub_v1  # pip install google-cloud-pubsub

WS_URL = "wss://socket.massive.com/stocks"

PROJECT_ID = os.getenv("PROJECT_ID")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC_2")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")  # if needed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("ws-to-pubsub")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)

# -------------------------
# Pub/Sub helper
# -------------------------
async def publish_to_pubsub(message: Any) -> None:
    """
    Publish a single message (dict or raw) to Pub/Sub.
    """
    if not isinstance(message, (dict, list)):
        payload = {"data": message}
    else:
        payload = message

    data_bytes = json.dumps(payload).encode("utf-8")

    # google-cloud-pubsub publish is sync-ish (returns a Future)
    future = publisher.publish(topic_path, data_bytes)

    # Optionally log result
    def _callback(f):
        try:
            msg_id = f.result()
            logger.debug(f"Published message ID: {msg_id}")
        except Exception as e:
            logger.error(f"Failed to publish message: {e}", exc_info=True)

    future.add_done_callback(_callback)


# -------------------------
# WebSocket consumer
# -------------------------
async def handle_ws_messages() -> None:
    """
    Connect to WebSocket and forward messages to Pub/Sub.
    """
    logger.info(f"Connecting to WebSocket: {WS_URL}")

    # You can tune ping_interval / ping_timeout as needed
    async with websockets.connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=20,
    ) as websocket:
        logger.info("WebSocket connection established")

        # ---- AUTH/ SUBSCRIBE (adjust for real API) ----
        # Example: Polygon-style auth (placeholder)
        auth_msg = {
            "action": "auth",
            "params": POLYGON_API_KEY,
        }
        await websocket.send(json.dumps(auth_msg))
        logger.info("Sent auth message")

        # Example: subscribe to some stock tickers (placeholder)
        subscribe_msg = {
            "action": "subscribe",
            "params": "T.AAPL,T.MSFT,T.GOOG",
        }
        await websocket.send(json.dumps(subscribe_msg))
        logger.info("Sent subscribe message")

        # ---- MESSAGE LOOP ----
        async for raw_message in websocket:
            logger.debug(f"Received raw message: {raw_message!r}")

            # Try to parse JSON; if it fails, send raw string
            try:
                parsed = json.loads(raw_message)
            except json.JSONDecodeError:
                parsed = raw_message

            # Many stock streams send arrays of events in one frame
            if isinstance(parsed, list):
                for item in parsed:
                    await publish_to_pubsub(item)
            else:
                await publish_to_pubsub(parsed)

# -------------------------
# Run loop with graceful shutdown
# -------------------------
class GracefulExit(SystemExit):
    pass


def _signal_handler(*_):
    raise GracefulExit()


async def main():
    # Handle Ctrl+C / SIGTERM nicely
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    while True:
        try:
            await handle_ws_messages()
        except GracefulExit:
            logger.info("Received shutdown signal. Exiting...")
            break
        except Exception as e:
            logger.error(f"WebSocket error: {e}. Reconnecting in 5 seconds...", exc_info=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    logger.info(f"Using project={PROJECT_ID}, topic={PUBSUB_TOPIC}")
    asyncio.run(main())