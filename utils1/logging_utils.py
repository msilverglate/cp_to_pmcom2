# logging_utils.py v2.0 includes debug 
import io, os
import logging
from datetime import datetime
from azure.storage.blob import BlobServiceClient

BLOB_CONTAINER = os.environ.get("BLOB_BLOB_CONTAINER", "blob1")
STORAGE_CONN_STR = os.environ["AzureWebJobsStorage"]

def setup_blob_logger(prefix="log", debug=False):
    """
    Returns a logger and an upload function.
    Logs go to console AND in-memory buffer, then uploaded to Azure blob at the end.
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"{prefix}_{timestamp}.txt"
    buffer = io.StringIO()

    # Set log level based on debug flag
    log_level = logging.DEBUG if debug else logging.INFO

    # Use a unique logger name per run
    logger = logging.getLogger(f"{prefix}_{timestamp}")
    logger.setLevel(log_level)
    logger.propagate = False  # prevents duplicate logs

    # Control Azure SDK verbosity
    logging.getLogger("azure").setLevel(
        logging.DEBUG if debug else logging.WARNING
    )

    # Clear previous handlers (important!)
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # 1️⃣ Console handler
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # 2️⃣ Buffer handler (writes to StringIO for blob upload)
    bh = logging.StreamHandler(buffer)
    bh.setLevel(log_level)
    bh.setFormatter(formatter)
    logger.addHandler(bh)

    # Upload function
    def upload():
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
        blob_client = blob_service_client.get_blob_client(BLOB_CONTAINER, blob_name)
        blob_client.upload_blob(buffer.getvalue(), overwrite=True)
        logger.info(f"✅ Uploaded log to blob: {BLOB_CONTAINER}/{blob_name}")

    return logger, upload