import requests
import re
import pandas as pd
from datetime import datetime, timedelta
import json
import os
import io
import argparse
import azure.functions as func
import numpy as np
import smartsheet
import logging
from azure.storage.blob import BlobServiceClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

bootstrap_logger = logging.getLogger("bootstrap")

# =====================
# CONFIG
# =====================

BLOB_CONTAINER = os.environ.get("BLOB_BLOB_CONTAINER", "blob1")
BLOB_NAME_A1 = os.environ.get("BLOB_NAME_A1", "Project Data 1.xlsx")
STORAGE_CONN_STR = os.environ["AzureWebJobsStorage"]

BASE_URL = "https://api.projectmanager.com/api/data"
API_KEY = os.environ.get("API_KEY")
if not API_KEY:
    raise RuntimeError("Set API_KEY in environment first!")

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}


def setup_blob_logger(blob_container, storage_conn_str, prefix="log"):
    """
    Returns a logger and an upload function.
    Logs go to console AND in-memory buffer, then uploaded to Azure blob at the end.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"{prefix}_{timestamp}.txt"
    buffer = io.StringIO()

    # Use a unique logger name per run
    logger = logging.getLogger(f"{prefix}_{timestamp}")
    logger.setLevel(logging.INFO)

    # Clear previous handlers (important!)
    if logger.hasHandlers():
        logger.handlers.clear()

    # 1️⃣ Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(ch)

    # 2️⃣ Buffer handler (writes to StringIO for blob upload)
    bh = logging.StreamHandler(buffer)
    bh.setLevel(logging.INFO)
    bh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(bh)

    # Upload function
    def upload():
        blob_service_client = BlobServiceClient.from_connection_string(storage_conn_str)
        blob_client = blob_service_client.get_blob_client(blob_container, blob_name)
        blob_client.upload_blob(buffer.getvalue(), overwrite=True)
        logger.info(f"✅ Uploaded log to blob: {blob_container}/{blob_name}")

    return logger, upload


def read_excel_from_blob(BLOB_CONTAINER, blob_name, logger):
    """
    Downloads an Excel file from Azure Blob Storage and returns a pandas DataFrame.
    Checks for container and blob existence before reading.
    """
    if not STORAGE_CONN_STR:
        raise RuntimeError("Set AZURE_STORAGE_CONNECTION_STRING environment variable first!")

    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER)

    if not container_client.exists():
        raise RuntimeError(f"Container '{BLOB_CONTAINER}' does not exist!")

    blob_client = container_client.get_blob_client(blob_name)
    if not blob_client.exists():
        raise RuntimeError(f"Blob '{blob_name}' does not exist in container '{BLOB_CONTAINER}'!")

    # Download blob into memory and read with pandas
    blob_data = blob_client.download_blob().readall()
    df = pd.read_excel(io.BytesIO(blob_data))
    logger.info(f"✅ Loaded {len(df)} rows from blob {blob_name} in container {BLOB_CONTAINER}")
    return df


# === DATA DICTIONARY ===

DEFAULT_DATA_DICTIONARY = '''

{
  "revisedEndDt": {
    "cp_source": "PJ UDEF Date 1",
    "field_type": "ProjNative",
    "pm_field": "targetDate",
    "update": "Always",
    "transform": "YYYY-MM-DD"
  },
  "navID": {
    "cp_source": "Notes",
    "field_type": "ProjCustom",
    "pm_field": "NAV ID",
    "update": "ifBlank",
    "transform": null
  },
  "caseCode": {
    "cp_source": "Notes",
    "field_type": "TaskCustom",
    "pm_field": "CASE CODE",
    "update": "ifBlank",
    "transform": "regex_left_of_dot"
  },
  "taskPM": {
    "cp_source": "Project Manager Name",
    "field_type": "TaskCustom",
    "pm_field": "Project Manager",
    "update": "ifBlank",
    "transform": null
  },
  "taskCG": {
    "cp_source": "Project ID",
    "field_type": "TaskCustom",
    "pm_field": "Charge Code",
    "update": "ifBlank",
    "transform": "regex_left_of_last_dot"
  },
  "caseCodeProj": {
    "cp_source": "Notes",
    "field_type": "ProjCustom",
    "pm_field": "Case Code",
    "update": "ifBlank",
    "transform": "regex_left_of_last_dot"
  }
}

'''


# =====================
# TRANSFORMS
# =====================
def regex_left_of_dot(text):
    if not text:
        return text
    m = re.match(r"([^.]+)", text)
    return m.group(1) if m else text


def regex_left_of_last_dot(value):
    if not isinstance(value, str):
        return value
    match = re.match(r"^(.*)\.[^.]+$", value)
    return match.group(1) if match else value


def transform_value(rule, value):
    """
    Transform a CP value according to data dictionary rule.
    Safely handles NaT, None, blank strings, and unexpected types.
    """

    # --- Universal empty checks ---
    if value is None or pd.isna(value) or str(value).strip() == "":
        return None

    # --------------------
    # Date Transform Rule
    # --------------------
    if rule == "YYYY-MM-DD":
        try:
            # Pandas Timestamp or Python datetime
            if isinstance(value, (pd.Timestamp, datetime)):
                return value.strftime("%Y-%m-%d")

            # Other formats → try converting
            return pd.to_datetime(value).strftime("%Y-%m-%d")

        except Exception:
            return None  # Invalid date → treat as blank

    # ---------------------------
    # Regex Left of First Dot
    # ---------------------------
    if rule == "regex_left_of_dot":
        try:
            return regex_left_of_dot(value)
        except Exception:
            return value

    # ---------------------------
    # Regex Left of LAST Dot
    # ---------------------------
    if rule == "regex_left_of_last_dot":
        try:
            return regex_left_of_last_dot(value)
        except Exception:
            return value

    # --------------------
    # Number Transform (Future Expand)
    # --------------------
    if rule == "number":
        try:
            return float(value)
        except Exception:
            return None

    # --------------------
    # No rule → return raw
    # --------------------
    return value


# =====================
# LOAD DATA DICTIONARY
# =====================
def load_data_dictionary(logger):
    # Azure blob container and blob names
    blob_dict_name = "CC_PM_Update_DataDict.xlsx"

    try:
        # Attempt to load from Azure blob
        df = read_excel_from_blob(BLOB_CONTAINER, blob_dict_name, logger)

        # Clean column names
        df.columns = [c.strip() for c in df.columns]

        # Keep only active rows
        if "Active" in df.columns:
            df = df[df["Active"].astype(str).str.upper() == "Y"]

        # Trim string columns
        for c in df.columns:
            if df[c].dtype == "object":
                df[c] = df[c].map(lambda x: x.strip() if isinstance(x, str) else x)

        df["Transform"] = df["Transform"].apply(lambda v: None if pd.isna(v) else v)

        data_dict = {}
        for _, row in df.iterrows():
            col = str(row["Col"]).strip()
            data_dict[col] = {
                "cp_source": row.get("CP Source"),
                "field_type": row.get("Field Type"),
                "pm_field": row.get("PM Field"),
                "update": row.get("Update?"),
                "transform": row.get("Transform"),
            }

        logger.info("=== DATA DICTIONARY LOADED FROM BLOB ===")
        logger.info(json.dumps(data_dict, indent=2))
        return data_dict

    except Exception as ex:
        logger.warning("Failed to load data dictionary from Azure blob")
        logger.warning("Reason: %s", ex)
        logger.info("=== USING DEFAULT EMBEDDED DATA DICTIONARY ===")
        try:
            data_dict = json.loads(DEFAULT_DATA_DICTIONARY)
            logger.info(json.dumps(data_dict, indent=2))
            return data_dict
        except Exception as json_ex:
            logger.error("Failed to load DEFAULT_DATA_DICTIONARY")
            logger.error(json_ex)
            return {}


def get_project_status(response_json):
    """
    Extracts the project status name ("Open", "Closed", etc.)
    from the GET /projects response.
    """

    # Safety checks
    if not response_json or "data" not in response_json:
        return None

    data = response_json.get("data", [])
    if not data:
        return None

    project = data[0]

    # Status field is always under project["status"]["name"]
    status = project.get("status", {})
    return status.get("name")


# =====================
# READ CP FILE WITH FILTERING
# =====================
def filterCPProjectsToUpdate(data_dict, filters=None, debug=False, logger=None):
    # Load Excel from blob and filter down CP dataset
    df = read_excel_from_blob(BLOB_CONTAINER, BLOB_NAME_A1, logger)

    df["PJ UDEF Date 1"] = pd.to_datetime(df["PJ UDEF Date 1"], errors="coerce")
    threshold_date = datetime.now() - timedelta(days=30)

    excluded_ids = ["OP-0050475"]
    filtered_df = df[
        (df["Opportunity ID"].notna()) &
        (df["Level Number"] == 5) &
        (~df["Opportunity ID"].isin(excluded_ids)) &
        (
                df["PJ UDEF Date 1"].isna() |
                (df["PJ UDEF Date 1"].astype(str).str.strip() == "") |
                (df["PJ UDEF Date 1"] > threshold_date)
        )
        ]

    # ----------------------------------------
    # Apply command-line filters
    # ----------------------------------------
    if filters:
        for filter_expr in filters:
            # Parse filter expression: column=pattern
            column_name, raw_pattern = filter_expr.split("=", 1)
            column_name = column_name.strip()
            raw_pattern = raw_pattern.strip()

            # Convert SQL-style wildcard (%) to regex (.*)
            regex_pattern = raw_pattern.replace("%", ".*")

            # Skip invalid columns
            if column_name not in filtered_df.columns:
                logger.info(
                    "[FILTER WARNING] Column '%s' not in dataframe, skipping",
                    column_name
                )
                continue

            # Compile regex (case-insensitive)
            compiled_regex = re.compile(regex_pattern, re.IGNORECASE)

            # Apply filter row-by-row
            def matches_filter(cell_value):
                return bool(compiled_regex.search(str(cell_value)))

            filtered_df = filtered_df[
                filtered_df[column_name].apply(matches_filter)
            ]

            if debug:
                logger.info(
                    "[FILTER DEBUG] Applied filter: %s LIKE %s, remaining rows: %d",
                    column_name,
                    regex_pattern,
                    len(filtered_df)
                )

    # ----------------------------------------
    # Build project update payloads
    # ----------------------------------------
    projects_to_update = []

    for _, row in filtered_df.iterrows():
        project_data = {}

        # Derive shortCode from Opportunity ID
        opportunity_id = str(row.get("Opportunity ID", ""))
        project_data["shortCode"] = opportunity_id[-7:]

        # Preserve original row for traceability
        project_data["source_row"] = row

        # Map and transform fields using data dictionary
        for output_field, metadata in data_dict.items():
            source_column = metadata["cp_source"]
            transform_name = metadata["transform"]

            raw_value = row.get(source_column)
            transformed_value = transform_value(transform_name, raw_value)

            project_data[output_field] = transformed_value

        projects_to_update.append(project_data)

    logger.info("Filtered rows: %d", len(projects_to_update))
    return projects_to_update


# =====================
# LOAD FIELD IDS
# =====================
def load_project_field_ids():
    url = f"{BASE_URL}/projects/fields"
    resp = requests.get(url, headers=headers)
    fields = resp.json().get("data", [])
    return {f["name"].strip().lower(): f["id"] for f in fields}


def load_task_field_ids(project_id):
    url = f"{BASE_URL}/projects/{project_id}/tasks/fields"
    resp = requests.get(url, headers=headers)
    fields = resp.json().get("data", [])
    return {f["name"].strip().lower(): f["id"] for f in fields}


def load_project_tasks(project_id, logger):
    url = f"{BASE_URL}/tasks?%24filter=projectId%20eq%20{project_id}"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        logger.warning(f"Failed to load tasks for project {project_id}")
        return []
    return resp.json().get("data", [])


def get_task_field_value(task_id, field_id, logger):
    url = f"{BASE_URL}/tasks/{task_id}/fields/{field_id}/values"
    resp = requests.get(url, headers=headers)
    try:
        data = resp.json().get("data")
        if isinstance(data, list) and data:
            return data[0].get("value")
        elif isinstance(data, dict):
            return data.get("value")
    except Exception as e:
        logger.error("Error retrieving task field value for task %s field %s: %s", task_id, field_id, e)
    return None


# =====================
# UPDATE PROJECT & TASKS WITH DEBUG LIMITS
# =====================
def update_pmcom_matching_projects(projects, data_dict, not_allowed_statuses, debug=False, logger=None):
    """
    Optimized version:
    - Single GET to fetch all project tasks
    - Local filtering of fieldValues
    - Skip updates when field already has value (ifBlank rule)
    - Skip projects with disallowed statuses
    """

    project_field_ids = load_project_field_ids()

    # Debug mode: limit projects
    if debug:
        projects = projects[:2]
        logger.info(f"=== DEBUG MODE: Limiting to {len(projects)} project(s) ===")
    for i, proj in enumerate(projects, start=1):

        short_code = proj["shortCode"]

        # ───────────────────────────────────────────────
        # 1. GET project by shortCode
        # ───────────────────────────────────────────────
        url = f"{BASE_URL}/projects?%24top=1&%24filter=shortCode%20eq%20'{short_code}'"
        resp = requests.get(url, headers=headers)

        if debug:
            logger.info(f"[DEBUG] GET {url} -> Status: {resp.status_code}")
            try:
                logger.info(json.dumps(resp.json(), indent=2))
            except Exception:
                logger.info(resp.text)

        resp_json = resp.json()
        data = resp_json.get("data", [])

        if not data:
            logger.warning(f"No PM.com project found for shortCode {short_code}")
            continue

        project = data[0]
        project_id = project["id"]
        project_name = project["name"]

        # ───────────────────────────────────────────────
        # 2. Check project status
        # ───────────────────────────────────────────────
        status_name = get_project_status(resp_json)
        normalized_status = (status_name or "").strip()

        logger.info(f"Status for {short_code}: {normalized_status}")

        if normalized_status in not_allowed_statuses:
            logger.warning(
                f"Skipping {short_code}: status '{normalized_status}' "
                f"in not allowed list {not_allowed_statuses}"
            )
            continue

        logger.info(f"=== Project {i}/{len(projects)}: {project_name} ===")

        # ───────────────────────────────────────────────
        # 3. GET ALL TASKS IN ONE CALL
        #    Eliminates 20–300 GET calls ✔
        # ───────────────────────────────────────────────
        tasks = load_project_tasks(project_id, logger)

        logger.info(f"Loaded {len(tasks)} tasks for this project")

        if debug:
            tasks = tasks[:10]
            logger.info(f"*** DEBUG MODE: Limiting to {len(tasks)} tasks ***")

        # Preload task field definitions once per project
        task_field_ids = load_task_field_ids(project_id)

        # Convert list of tasks → dict by ID for fast lookup
        task_dict = {t["id"]: t for t in tasks}

        # Build lookup for task custom field values PER TASK (local)
        # Example: task_field_map[task_id]['nav id'] = "ABC-123"
        task_field_map = {}

        for t in tasks:
            tf = {}
            for fv in t.get("fieldValues", []):
                fname = fv["name"].lower()
                tf[fname] = fv.get("value")
            task_field_map[t["id"]] = tf

        # ───────────────────────────────────────────────
        # 4. PROCESS ALL CP→PM FIELDS
        # ───────────────────────────────────────────────
        for key, meta in data_dict.items():
            value = proj[key]
            field_type = meta["field_type"]
            pm_field = meta["pm_field"].lower()
            rule = meta["update"]

            if value is None:
                continue

            # PROJECT NATIVE FIELD
            if field_type == "ProjNative":
                logger.info(f"Updating project native field {pm_field}: {value}")

                put_url = f"{BASE_URL}/projects/{project_id}"
                payload = {pm_field: value}
                r = requests.put(put_url, headers=headers, json=payload)

                if debug:
                    logger.info(f"[DEBUG] PUT {put_url} -> {r.status_code}")

            # PROJECT CUSTOM FIELD
            elif field_type == "ProjCustom":
                field_id = project_field_ids.get(pm_field)
                if not field_id:
                    logger.warning(f"[WARN] Project field '{pm_field}' not found")
                    continue

                # Only GET once if rule == ifBlank
                if rule == "ifBlank":
                    get_url = f"{BASE_URL}/projects/{project_id}/fields/{field_id}"
                    r = requests.get(get_url, headers=headers)
                    existing = r.json().get("data", {}).get("value")

                    if existing not in (None, "", " "):
                        if debug:
                            logger.info(f"[SKIP] Project custom field {pm_field} already has value: {existing}")
                        continue

                logger.info(f"Updating project custom field {pm_field}: {value}")

                put_url = f"{BASE_URL}/projects/{project_id}/fields/{field_id}"
                r = requests.put(put_url, headers=headers, json={"value": value})

                if debug:
                    logger.info(f"[DEBUG] PUT {put_url} -> {r.status_code}")

            # TASK CUSTOM FIELD (FAST MODE, NO PER-TASK GET)
            elif field_type == "TaskCustom":
                field_id = task_field_ids.get(pm_field)
                if not field_id:
                    logger.warning(f"[WARN] Task field '{pm_field}' not found")
                    continue

                logger.info(f"Updating task custom field {pm_field} for {len(tasks)} tasks")

                for task_id in task_dict.keys():

                    existing = task_field_map[task_id].get(pm_field)

                    # Skip per rules
                    if rule == "ifBlank" and existing not in (None, "", " "):
                        continue

                    # Skip if value already matches (avoid unnecessary calls)
                    if existing == value:
                        continue

                    put_url = f"{BASE_URL}/tasks/{task_id}/fields/{field_id}/values"
                    r = requests.put(put_url, headers=headers, json={"value": value})
                    logger.info(f"  ✓ Task {task_id} | {pm_field} = {value}")

                    if debug:
                        logger.info(f"[DEBUG] PUT task {task_id} -> {r.status_code}")

                logger.info(f"✓ Completed updates for task field {pm_field}")

        logger.info(f"=== Finished project {short_code} ===\n")


def run_cp_to_pmcom(filters=None, not_allowed_statuses=None, debug=False):
    logger, upload_log = setup_blob_logger(BLOB_CONTAINER, STORAGE_CONN_STR, prefix="pm_update_log")

    try:
        logger.info("=== CP → PMCOM Update Started ===")
        logger.info(f"Start time: {datetime.now()}")

        if not not_allowed_statuses:
            not_allowed_statuses = ["Closed"]

        logger.info(f"Filters: {filters}, Not Allowed Statuses: {not_allowed_statuses}, Debug: {debug}")

        # Load data dictionary
        data_dict = load_data_dictionary(logger)

        # Read CP Excel
        projects = filterCPProjectsToUpdate(data_dict, filters=filters, debug=debug, logger=logger)

        # Update PMCOM
        update_pmcom_matching_projects(
            projects,
            data_dict,
            not_allowed_statuses=not_allowed_statuses,
            debug=debug,
            logger=logger
        )

        logger.info(f"Finished CP → PMCOM update. Total projects processed: {len(projects)}")

    except Exception as e:
        logger.exception(f"❌ CP → PMCOM update failed: {e}")

    finally:
        logger.info(f"End time: {datetime.now()}")
        upload_log()


# =====================
# AZURE FUNCTION APP
# =====================
app = func.FunctionApp()


@app.function_name(name="CostpointToPMcom")
@app.route(route="CostpointToPMcom", methods=["POST", "GET"])  # HTTP trigger
def CostpointToPMcom(req: func.HttpRequest):
    # -------------------------
    # GET → describe function
    # -------------------------
    if req.method == "GET":
        df = read_excel_from_blob(BLOB_CONTAINER, BLOB_NAME_A1, logger = bootstrap_logger)
        cp_columns = list(df.columns)

        return func.HttpResponse(
            json.dumps({
                "description": "Update PM.com projects from CP Excel feed",
                "available_filters": cp_columns,
                "filter_syntax": "FieldName=Value or FieldName=%partial%",
                "examples": {
                    "filters": [
                        "Project Manager Name=%Lendo%",
                        "Opportunity ID=0140045"
                    ],
                    "not_allowed_statuses": [
                        "Closed"
                    ]
                },
                "defaults": {
                    "not_allowed_statuses": [
                        "Closed"
                    ],
                    "debug": False
                }
            }, indent=2),
            mimetype="application/json",
            status_code=200
        )

    # optional: read query params or JSON payload
    if req.method == "POST":
        data = req.get_json()
        filters = data.get("filters")
        not_allowed_statuses = data.get("not_allowed_statuses")
        debug = data.get("debug", False)

        run_cp_to_pmcom(
            filters=filters,
            not_allowed_statuses=not_allowed_statuses,
            debug=debug
        )

    return func.HttpResponse(
        "CP to PMCOM processing triggered successfully.",
        status_code=200
    )


# =====================
# SMARTSHEET IMPORT
# =====================

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

    logger.info(f"Completed clearing rows. Total deleted: {deleted_count}")


def reduce_columns(df, allowed_columns):
    df1 = df[sorted(allowed_columns)].copy()
    for col in ["PJ UDEF Date 1", "End Date", "Project Start Date"]:
        if col in df1.columns:
            df1[col] = df1[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df1.replace({np.nan: ""}, inplace=True)
    return df1


def run_cp_to_smartsheet(sheet_id: int, blob_name: str, debug=False):
    logger, upload_log = setup_blob_logger(BLOB_CONTAINER, STORAGE_CONN_STR,
                                           prefix=f"smartsheet_update_log_{blob_name}")

    logger.info("CP → Smartsheet function triggered")
    try:
        SMARTSHEET_API_KEY = os.environ.get("SMARTSHEET_API_KEY")
        if not SMARTSHEET_API_KEY:
            logger.error("SMARTSHEET_API_KEY is missing")
            raise ValueError("SMARTSHEET_API_KEY is missing")

        logger.info(f"SMARTSHEET_API_KEY loaded successfully")
        smartsheet_client = smartsheet.Smartsheet(SMARTSHEET_API_KEY)

        logger.info(f"=== CP → Smartsheet Sync Started ({blob_name}) ===")
        logger.info(f"Start time: {datetime.now()}")

        df = read_excel_from_blob(BLOB_CONTAINER, blob_name, logger)

        sheet = smartsheet_client.Sheets.get_sheet(sheet_id)
        logger.info(f"Loaded Smartsheet '{sheet.name}' with {len(sheet.rows)} existing rows")

        clear_smartsheet(sheet, smartsheet_client, logger)

        smartsheet_columns = [c.title for c in sheet.columns]
        common_columns = list(set(smartsheet_columns).intersection(df.columns))
        df1 = reduce_columns(df, common_columns)
        logger.info(f"Matched columns ({len(common_columns)}): {common_columns}")

        # Prepare and upload rows
        rows = []
        ROW_LIMIT = 20000
        for idx, row in df1.iterrows():
            if idx >= ROW_LIMIT:
                break
            new_row = smartsheet.models.Row()
            new_row.to_bottom = True
            for col in sheet.columns:
                if col.title in df1.columns:
                    new_row.cells.append({"column_id": col.id, "value": row[col.title]})
            rows.append(new_row)

            if (idx + 1) % 100 == 0:
                logger.info(f"Prepared {idx + 1} rows")

        if rows:
            logger.info(f"Writing {len(rows)} rows to Smartsheet")
            smartsheet_client.Sheets.add_rows(sheet_id, rows)

        logger.info(f"=== CP → Smartsheet Sync Completed ({blob_name}) ===")

    except Exception as e:
        logger.exception(f"❌ Smartsheet sync failed: {e}")
        raise

    finally:
        logger.info(f"End time: {datetime.now()}")
        upload_log()


# HTTP trigger CostpointToSmartsheet A1 function
@app.function_name(name="CostpointToSmartsheet")
@app.route(route="CostpointToSmartsheet", methods=["POST"])
def CostpointToSmartsheet(req: func.HttpRequest):

    try:
        run_cp_to_smartsheet(
            sheet_id=864938054602628,  # A1 Smartsheet
            blob_name="Project Data 1.xlsx"  # A1 CP source
        )
        return func.HttpResponse("Costpoint to Smartsheet completed successfully", status_code=200)
    except Exception as e:
        bootstrap_logger.error("Unhandled exception")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


# HTTP CostpointToSmartsheet A4 function
@app.function_name(name="CostpointToSmartsheetA4")
@app.route(route="CostpointToSmartsheetA4", methods=["POST"])
def CostpointToSmartsheetA4(req: func.HttpRequest):

    try:
        run_cp_to_smartsheet(
            sheet_id=2469989006135172,  # A4 Smartsheet
            blob_name="Project Data 1CA.xlsx"  # A4 CP source
        )
        return func.HttpResponse("A4 Smartsheet sync completed", status_code=200)
    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)


if __name__ == "__main__":

    # =====================
    # LOAD CP EXCEL COLUMNS FOR HELP
    # =====================
    df = read_excel_from_blob(BLOB_CONTAINER, BLOB_NAME_A1, logger=bootstrap_logger)
    bootstrap_logger.info(f"✅ Loaded {len(df)} rows from blob {BLOB_NAME_A1} in container {BLOB_CONTAINER}")

    cp_columns = list(df.columns)

    parser = argparse.ArgumentParser(
        description=f"Update PM.com projects and Smartsheet from CP Excel feed.\n\n"
                    f"Available fields for filtering:\n  {', '.join(cp_columns)}"
    )

    parser.add_argument("--newlog", action="store_true")
    parser.add_argument("--filter", action="append")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--not-allowed-status", action="append")

    args = parser.parse_args()

    # If neither VBA nor CLI requested logging mode → default to newlog
    if not args.newlog:
        args.newlog = True

    # =====================
    # RUN PMCOM UPDATE
    # =====================
    try:
        run_cp_to_pmcom()
    except Exception as e:
        bootstrap_logger.error(f"❌ Smartsheet update failed: {e}")

    # =====================
    # RUN SMARTSHEET UPDATE A1
    # =====================
    try:
        run_cp_to_smartsheet(sheet_id=864938054602628, blob_name="Project Data 1.xlsx")  # A1 data
    except Exception as e:
        bootstrap_logger.error(f"❌ Smartsheet update failed: {e}")

    # =====================
    # RUN SMARTSHEET UPDATE A4
    # =====================
    try:
        run_cp_to_smartsheet(sheet_id=2469989006135172, blob_name="Project Data 1CA.xlsx")  # A3 data
    except Exception as e:
        bootstrap_logger.error(f"❌ Smartsheet update failed: {e}")