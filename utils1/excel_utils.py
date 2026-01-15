# excel_utils.py
import os
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient

BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER_NAME", "blob1")
STORAGE_CONN_STR = os.environ["AzureWebJobsStorage"]


def read_excel_from_blob(blob_name, logger):
    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER)
    if not container_client.exists():
        raise RuntimeError(f"Container '{BLOB_CONTAINER}' does not exist!")

    blob_client = container_client.get_blob_client(blob_name)
    if not blob_client.exists():
        raise RuntimeError(f"Blob '{blob_name}' does not exist in container '{BLOB_CONTAINER}'!")

    # Get blob timestamp
    props = blob_client.get_blob_properties()
    cp_update_ts = props.last_modified.strftime("%Y-%m-%d %H:%M:%S UTC")

    blob_data = blob_client.download_blob().readall()

    df = pd.read_excel(io.BytesIO(blob_data))
    # Inject timestamp column into DataFrame
    df["Costpoint Update Date"] = cp_update_ts

    logger.info(f"✅ Loaded {len(df)} rows from blob {blob_name} in container {BLOB_CONTAINER}"
                )
    return df