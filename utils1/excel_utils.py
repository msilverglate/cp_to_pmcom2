# excel_utils.py V2

import os
import io


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

# ---------------------------
# WRITE TO BLOB
# ---------------------------
from io import BytesIO
import pandas as pd
from azure.storage.blob import BlobServiceClient

def write_df_to_blob_excel(df, blob_name, logger):
    logger.info("Preparing to write Excel to blob: %s", blob_name)

    # Ensure df is a DataFrame
    if df is None:
        logger.warning("DataFrame is None — creating empty placeholder")
        df = pd.DataFrame({"Message": ["No data available"]})

    elif isinstance(df, list):
        logger.warning("Input is a list — converting to DataFrame")
        df = pd.DataFrame(df)

    # If empty, still write a visible sheet
    if df.empty:
        logger.warning("DataFrame is empty — writing placeholder sheet")
        df = pd.DataFrame({"Message": ["No data available"]})

    # Write to Excel in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")

    output.seek(0)

    # Upload to blob
    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
    blob_client = blob_service_client.get_blob_client(
        container=BLOB_CONTAINER,
        blob=blob_name
    )

    blob_client.upload_blob(output.getvalue(), overwrite=True)

    logger.info("Excel file written to blob: %s", blob_name)