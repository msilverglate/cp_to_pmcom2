import os
import json
import base64
from azure.storage.queue import QueueClient

# CREATE QUEUE BEFORE USING
# az storage queue create \
#   --name cp-smartsheet-queue \
#   --account-name costpoint1 \
#   --account-key <STORAGE_ACCOUNT_KEY>


# 1️⃣ Read connection string from environment
STORAGE_CONN_STR = os.getenv("STORAGE_CONN_STR")
if not STORAGE_CONN_STR:
    raise RuntimeError("Set STORAGE_CONN_STR environment variable first!")
QUEUE_NAME = "cp-smartsheet-queue"

# Message payload
payload = {
    "sheet_id": 864938054602628,
    "blob_name": "Project Data 1.xlsx"
}

# Convert to JSON, then Base64
json_str = json.dumps(payload)
encoded_message = base64.b64encode(json_str.encode("utf-8")).decode("utf-8")

# Send to queue
queue_client = QueueClient.from_connection_string(STORAGE_CONN_STR, QUEUE_NAME)
# queue_client.create_queue()  # ignore if exists
queue_client.send_message(encoded_message)

print("Base64-encoded message sent successfully!")