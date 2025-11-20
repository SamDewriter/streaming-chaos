import os
from google.cloud import pubsub_v1

from dotenv import load_dotenv
load_dotenv()


PROJECT_ID = os.getenv("PROJECT_ID", "your-gcp-project-id")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID", "stocks-stream-sub")

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def callback(message: pubsub_v1.subscriber.message.Message):
    print("🔔 Received message:")
    print(message.data.decode("utf-8"))
    print("-" * 40)

    # Acknowledge message so it’s not re-delivered
    message.ack()

def main():
    print(f"Listening on subscription: {subscription_path}")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    try:
        streaming_pull_future.result()  # Keep the subscriber alive
    except KeyboardInterrupt:
        streaming_pull_future.cancel()  # Stop gracefully

if __name__ == "__main__":
    main()
