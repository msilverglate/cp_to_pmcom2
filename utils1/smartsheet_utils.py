# =====================
# SMARTSHEET UTILS
# =====================
import numpy as np
import time

def clear_smartsheet(sheet, smartsheet_client, logger):
    row_ids = [row.id for row in sheet.rows]
    total_rows = len(row_ids)
    logger.info(f"Starting to clear {total_rows} rows from Smartsheet...")

    CHUNK_SIZE = 400
    deleted_count = 0

    for i in range(0, total_rows, CHUNK_SIZE):
        chunk = row_ids[i:i + CHUNK_SIZE]
        smartsheet_client.Sheets.delete_rows(sheet.id, chunk)
        deleted_count += len(chunk)
        logger.info(f"Deleted {len(chunk)} rows in this chunk. Total deleted so far: {deleted_count}/{total_rows}")

        # Add a short delay for testing
        # logger.info("Pausing briefly to observe deletion...")
        # time.sleep(10)  # <-- adjust seconds as needed

    logger.info(f"Completed clearing rows. Total deleted: {deleted_count}")


def reduce_columns(df, allowed_columns):
    df1 = df[sorted(allowed_columns)].copy()
    for col in ["PJ UDEF Date 1", "End Date", "Project Start Date"]:
        if col in df1.columns:
            df1[col] = df1[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df1.replace({np.nan: ""}, inplace=True)
    return df1