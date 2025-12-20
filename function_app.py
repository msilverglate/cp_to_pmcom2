
import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import sys
from datetime import datetime
import os
import io
import argparse
import azure.functions as func



#----------Azure Blob Connection --------

from azure.storage.blob import BlobServiceClient


def read_excel_from_blob(container_name, blob_name):
    """
    Downloads an Excel file from Azure Blob Storage and returns a pandas DataFrame.
    Checks for container and blob existence before reading.
    """
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not connect_str:
        raise RuntimeError("Set AZURE_STORAGE_CONNECTION_STRING environment variable first!")

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    if not container_client.exists():
        raise RuntimeError(f"Container '{container_name}' does not exist!")

    blob_client = container_client.get_blob_client(blob_name)
    if not blob_client.exists():
        raise RuntimeError(f"Blob '{blob_name}' does not exist in container '{container_name}'!")

    print(f"✅ Connected to blob: {container_name}/{blob_name}")

    # Download blob into memory and read with pandas
    blob_data = blob_client.download_blob().readall()
    df = pd.read_excel(io.BytesIO(blob_data))
    return df


# -----------------------------
# Tee class for capturing logs
# -----------------------------
class BlobTee:
    """Redirects prints to console and in-memory buffer."""
    def __init__(self, container_name, blob_name):
        self.container_name = container_name
        self.blob_name = blob_name
        self.buffer = io.StringIO()
        self._stdout = sys.__stdout__  # <-- original stdout

    def write(self, obj):
        self._stdout.write(obj)       # write to console
        self.buffer.write(obj)        # capture in memory

    def flush(self):
        self._stdout.flush()
        self.buffer.flush()

    def upload_to_blob(self):
        connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if not connect_str:
            raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING not set!")

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client(self.container_name)
        if not container_client.exists():
            container_client.create_container()

        blob_client = container_client.get_blob_client(self.blob_name)
        blob_client.upload_blob(self.buffer.getvalue(), overwrite=True)
        self._stdout.write(f"\n✅ Uploaded log to blob: {self.container_name}/{self.blob_name}\n")

# =====================
# CONFIG
# =====================
API_KEY = os.environ.get("API_KEY")
if not API_KEY:
    raise RuntimeError("Set API_KEY_PM in environment first!")
BASE_URL = "https://api.projectmanager.com/api/data"
STORAGE_ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_KEY")

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

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

import re

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
def load_data_dictionary():
    # Azure blob container and blob names
    container_name = "blob1"
    blob_name = "CC_PM_Update_DataDict.xlsx"

    try:
        # Attempt to load from Azure blob
        df = read_excel_from_blob(container_name, blob_name)

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

        print("\n=== DATA DICTIONARY LOADED FROM BLOB ===")
        print(json.dumps(data_dict, indent=2))
        return data_dict

    except Exception as ex:
        print("\n[WARN] Failed to load data dictionary from Azure blob")
        print("Reason:", ex)
        print("\n=== USING DEFAULT EMBEDDED DATA DICTIONARY ===")
        try:
            data_dict = json.loads(DEFAULT_DATA_DICTIONARY)
            print(json.dumps(data_dict, indent=2))
            return data_dict
        except Exception as json_ex:
            print("[FATAL] Failed to load DEFAULT_DATA_DICTIONARY")
            print(json_ex)
            return {}


def get_available_filter_fields():
    data_dict = load_data_dictionary()
    df = read_excel_from_blob("blob1", "Project Data 1.xlsx")

    cp_columns = list(df.columns)
    # all_fields = sorted(set(list(data_dict.keys()) + cp_columns))

    return cp_columns

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
def readCP_File(data_dict, filters=None, debug=False):
    # Azure blob info
    container_name = "blob1"
    blob_name = "Project Data 1.xlsx"

    # Load Excel from blob
    df = read_excel_from_blob(container_name, blob_name)

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

    # Command-line filters
    if filters:
        for f in filters:
            field, pattern = f.split("=", 1)
            field = field.strip()
            pattern = pattern.strip().replace("%", ".*")  # wildcard -> regex
            if field not in filtered_df.columns:
                print(f"[FILTER WARNING] Column '{field}' not in dataframe, skipping")
                continue
            regex = re.compile(pattern, re.IGNORECASE)
            filtered_df = filtered_df[filtered_df[field].astype(str).apply(lambda x: bool(regex.search(x)))]
            if debug:
                print(f"[FILTER DEBUG] Applied filter: {field} LIKE {pattern}, remaining rows: {len(filtered_df)}")

    projects_to_update = []
    for _, row in filtered_df.iterrows():
        project_data = {"shortCode": str(row["Opportunity ID"])[-7:], "source_row": row}
        for key, meta in data_dict.items():
            raw_val = row.get(meta["cp_source"])
            project_data[key] = transform_value(meta["transform"], raw_val)
        projects_to_update.append(project_data)

    print("Filtered rows:", len(projects_to_update))
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

def load_project_tasks(project_id):
    url = f"{BASE_URL}/tasks?%24filter=projectId%20eq%20{project_id}"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"Failed to load tasks for project {project_id}")
        return []
    return resp.json().get("data", [])

def get_task_field_value(task_id, field_id):
    url = f"{BASE_URL}/tasks/{task_id}/fields/{field_id}/values"
    resp = requests.get(url, headers=headers)
    try:
        data = resp.json().get("data")
        if isinstance(data, list) and data:
            return data[0].get("value")
        elif isinstance(data, dict):
            return data.get("value")
    except Exception as e:
        print(f"Error retrieving task field value for task {task_id}, field {field_id}: {e}")
    return None

# =====================
# UPDATE PROJECT & TASKS WITH DEBUG LIMITS
# =====================
def update_pmcom_matching_projects(projects, data_dict, allowed_statuses, debug=False):
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
        print(f"\n=== DEBUG MODE: Limiting to {len(projects)} project(s) ===")

    for i, proj in enumerate(projects, start=1):

        short_code = proj["shortCode"]

        # ───────────────────────────────────────────────
        # 1. GET project by shortCode
        # ───────────────────────────────────────────────
        url = f"{BASE_URL}/projects?%24top=1&%24filter=shortCode%20eq%20'{short_code}'"
        resp = requests.get(url, headers=headers)

        if debug:
            print(f"[DEBUG] GET {url} -> Status: {resp.status_code}")
            try:
                print("[DEBUG] Response:", json.dumps(resp.json(), indent=2))
            except Exception:
                print("[DEBUG] Response not JSON:", resp.text)

        resp_json = resp.json()
        data = resp_json.get("data", [])

        if not data:
            print(f"[WARN] No PM.com project found for shortCode {short_code}")
            continue

        project = data[0]
        project_id = project["id"]
        project_name = project["name"]

        # ───────────────────────────────────────────────
        # 2. Check project status
        # ───────────────────────────────────────────────
        status_name = get_project_status(resp_json)
        normalized_status = (status_name or "").strip()

        print(f"Status for {short_code}: {normalized_status}")

        if normalized_status not in allowed_statuses:
            print(f"Skipping {short_code}: status '{normalized_status}' not in allowed list {allowed_statuses}")
            continue

        print(f"\n=== Project {i}/{len(projects)}: {project_name} ===")

        # ───────────────────────────────────────────────
        # 3. GET ALL TASKS IN ONE CALL
        #    Eliminates 20–300 GET calls ✔
        # ───────────────────────────────────────────────
        tasks = load_project_tasks(project_id)

        print(f"Loaded {len(tasks)} tasks for this project")

        if debug:
            tasks = tasks[:10]
            print(f"*** DEBUG MODE: Limiting to {len(tasks)} tasks ***")

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
                print(f"Updating project native field {pm_field}: {value}")

                put_url = f"{BASE_URL}/projects/{project_id}"
                payload = {pm_field: value}
                r = requests.put(put_url, headers=headers, json=payload)

                if debug:
                    print(f"[DEBUG] PUT {put_url} -> {r.status_code}")

            # PROJECT CUSTOM FIELD
            elif field_type == "ProjCustom":
                field_id = project_field_ids.get(pm_field)
                if not field_id:
                    print(f"[WARN] Project field '{pm_field}' not found")
                    continue

                # Only GET once if rule == ifBlank
                if rule == "ifBlank":
                    get_url = f"{BASE_URL}/projects/{project_id}/fields/{field_id}"
                    r = requests.get(get_url, headers=headers)
                    existing = r.json().get("data", {}).get("value")

                    if existing not in (None, "", " "):
                        if debug:
                            print(f"[SKIP] Project custom field {pm_field} already has value: {existing}")
                        continue

                print(f"Updating project custom field {pm_field}: {value}")

                put_url = f"{BASE_URL}/projects/{project_id}/fields/{field_id}"
                r = requests.put(put_url, headers=headers, json={"value": value})

                if debug:
                    print(f"[DEBUG] PUT {put_url} -> {r.status_code}")

            # TASK CUSTOM FIELD (FAST MODE, NO PER-TASK GET)
            elif field_type == "TaskCustom":
                field_id = task_field_ids.get(pm_field)
                if not field_id:
                    print(f"[WARN] Task field '{pm_field}' not found")
                    continue

                print(f"Updating task custom field {pm_field} for {len(tasks)} tasks")

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
                    print(f"  ✓ Task {task_id} | {pm_field} = {value}")

                    if debug:
                        print(f"[DEBUG] PUT task {task_id} -> {r.status_code}")

                print(f"✓ Completed updates for task field {pm_field}")

        print(f"=== Finished project {short_code} ===\n")


def run_cp_to_pmcom(filters=None, allowed_statuses=None, debug=False):


    # # =====================
    # # LOAD CP EXCEL COLUMNS FOR HELP
    # # =====================
    # # excel_file = r"C:\Users\mike.silverglate\OneDrive - Red River Technology LLC\Documents\2025 04 Apr\Project Data 1.xlsx"
    # df = read_excel_from_blob("blob1", "Project Data 1.xlsx")
    # print(f"Loaded CP file from blob with {len(df.columns)} columns")
    # # df = pd.read_excel(excel_file)
    # cp_columns = list(df.columns)

    # # Merge with data dictionary keys
    # all_fields = sorted(set(list(data_dict.keys()) + cp_columns))
    # # all_fields_text = ", ".join(all_fields)

    # =====================
    # LOGGING SETUP
    # =====================
    container = "blob1"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"pm_update_log_{timestamp}.txt"

    sys.stdout = BlobTee(container, blob_name)

    print("This log will go to both console and blob!")
    print("Processing project data...")

    
    # -------- Start timestamp --------
    print("\n===================")
    print(f"Script run started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("===================\n")

    data_dict = load_data_dictionary()
    print("Allowed Statuses", allowed_statuses)
    print("Filters", filters)
    print("Debug", debug)

    # =====================
    # DEFAULT BEHAVIOR (NO CONFIG / IDE RUN)
    # =====================
    if not allowed_statuses:
        allowed_statuses = [
            "Open",
            "Planning",
            "Bucket of Hours"
        ]

    if not filters:
        filters = None

    # =====================
    # PROCESS
    # =====================
    projects = readCP_File(data_dict, filters=filters, debug=debug)

    update_pmcom_matching_projects(
        projects,
        data_dict,
        allowed_statuses=allowed_statuses,
        debug=debug
    )

    # =====================
    # END OF SCRIPT — close log
    # =====================
    print("\n===================")
    print(f"Script run ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("===================\n")

    sys.stdout.upload_to_blob()

# =====================
# AZURE FUNCTION APP
# =====================
app = func.FunctionApp()

@app.function_name(name="cp_to_pmcom_main2")
@app.route(route="cp_to_pmcom_main2", methods=["POST", "GET"])  # HTTP trigger

def cp_to_pmcom_main2(req: func.HttpRequest):

    # -------------------------
    # GET → describe function
    # -------------------------
    if req.method == "GET":
        fields = get_available_filter_fields()

        return func.HttpResponse(
            json.dumps({
                "description": "Update PM.com projects from CP Excel feed",
                "available_filters": fields,
                "filter_syntax": "FieldName=Value or FieldName=%partial%",
                "examples": {
                    "filters": [
                        "Project Manager Name=%Lendo%",
                        "Opportunity ID=0140045"
                    ],
                    "allowed_statuses": [
                        "Open",
                        "Planning",
                        "Bucket of Hours"
                    ]
                },
                "defaults": {
                    "allowed_statuses": [
                        "Open",
                        "Planning",
                        "Bucket of Hours"
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
        allowed_statuses = data.get("allowed_statuses")
        debug = data.get("debug", False)

    run_cp_to_pmcom(
        filters=filters,
        allowed_statuses=allowed_statuses,
        debug=debug
    )

    return func.HttpResponse(
        "CP to PMCOM processing triggered successfully.",
        status_code=200
    )

    
if __name__ == "__main__":

    data_dict = load_data_dictionary()

    # =====================
    # LOAD CP EXCEL COLUMNS FOR HELP
    # =====================
    excel_file = r"C:\Users\mike.silverglate\OneDrive - Red River Technology LLC\Documents\2025 04 Apr\Project Data 1.xlsx"
    df = pd.read_excel(excel_file)
    cp_columns = list(df.columns)

    all_fields = sorted(set(list(data_dict.keys()) + cp_columns))
    all_fields_text = ", ".join(all_fields)

    # =====================
    # ARGPARSE WITH UPDATED HELP
    # =====================
    parser = argparse.ArgumentParser(
        description=f"Update PM.com projects from CP Excel feed.\n\n"
                    f"Available fields for filtering:\n  {all_fields_text}\n\n"
                    f"Examples:\n"
                    f'  --filter "Project Manager Name=%%Lendo%%"\n'
                    f'  --filter "Opportunity ID=0140045"'
    )

    parser.add_argument("--newlog", action="store_true")
    parser.add_argument("--filter", action="append")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--allowed-status", action="append")

    args = parser.parse_args()

    # If neither VBA nor CLI requested logging mode → default to newlog
    if not args.newlog:
        args.newlog = True

    run_cp_to_pmcom(
        filters=args.filter,
        allowed_statuses=args.allowed_status,
        debug=args.debug
    )

