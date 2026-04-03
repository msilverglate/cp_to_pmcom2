## Version 2.7 Includes As Sold Cost and As Sold Revenue

import requests
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import azure.functions as func
import smartsheet
import logging
import uuid
import base64
from utils1.logging_utils import setup_blob_logger
from utils1.cp_project_task_data_util import (load_project_tasks,
                                              load_task_field_ids, load_project_field_ids,
                                              pick_pmcom_project, get_project_status)
from utils1.excel_utils import read_excel_from_blob
from utils1.api_call_utils import robust_put, robust_get, robust_post, robust_delete
from azure.storage.queue import QueueClient
from dateutil.parser import parse
from dateutil.tz import tzutc

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

bootstrap_logger = logging.getLogger("bootstrap")

# ----------------------------
# CONFIG
# ----------------------------
BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER_NAME", "blob1")
BLOB_NAME_A1 = os.environ.get("BLOB_NAME_A1", "Project Data 1.xlsx")
BLOB_NAME_A2 = os.environ.get("BLOB_NAME_A2", "PTO CP to PMCOM.xlsx")
BLOB_NAME_A4 = os.environ.get("BLOB_NAME_A4", "Project Data 1CA.xlsx")
STORAGE_CONN_STR = os.environ["AzureWebJobsStorage"]
PTO_PROJ_SHORTCODE = os.environ.get("PTO_PROJ_SHORTCODE","TimeOff")

BASE_URL = "https://api.projectmanager.com/api/data"
API_KEY = os.environ.get("PM_API_KEY")
if not API_KEY:
    raise RuntimeError("Set API_KEY in environment first!")

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# ----------------------------
# DATA DICTIONARY
# ----------------------------

DEFAULT_DATA_DICTIONARY = '''

{
    "cpTimeStamp": {
    "cp_source": "Costpoint Update Date",
    "field_type": "ProjCustom",
    "pm_field": "CP Update Timestamp",
    "update": "Always",
    "transform": null
  },
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
  },
    "cpEngActHrsProj": {
    "cp_source": "CP ENG ACT HRS",
    "field_type": "ProjCustom",
    "pm_field": "CP ENG ACT HRS",
    "update": "Always",
    "transform": null
  },
    "cpPm1ActHrsProj": {
    "cp_source": "CP PM1 ACT HRS",
    "field_type": "ProjCustom",
    "pm_field": "CP PM1 ACT HRS",
    "update": "Always",
    "transform": null
  },
    "cpTrvActHrsProj": {
    "cp_source": "CP TRV ACT HRS",
    "field_type": "ProjCustom",
    "pm_field": "CP TRV ACT HRS",
    "update": "Always",
    "transform": null
  },
    "cpDnbActHrsProj": {
    "cp_source": "CP DNB ACT HRS",
    "field_type": "ProjCustom",
    "pm_field": "CP DNB ACT HRS",
    "update": "Always",
    "transform": null
  },
  
    "cpAsSoldCost": {
    "cp_source": "Cost Funded",
    "field_type": "ProjCustom",
    "pm_field": "As Sold Cost",
    "update": "Always",
    "transform": null
    },
    
    "cpAsSoldRev": {
    "cp_source": "Total Funded",
    "field_type": "ProjCustom",
    "pm_field": "As Sold Rev",
    "update": "Always",
    "transform": null
    }
}

'''



# ----------------------------
# TRANSFORMS
# ----------------------------
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
    if value is None or pd.isna(value) or str(value).strip() == "":
        return None
    if rule == "YYYY-MM-DD":
        try:
            if isinstance(value, (pd.Timestamp, datetime)):
                return value.strftime("%Y-%m-%d")
            return pd.to_datetime(value).strftime("%Y-%m-%d")
        except Exception:
            return None
    if rule == "regex_left_of_dot":
        try:
            return regex_left_of_dot(value)
        except Exception:
            return value
    if rule == "regex_left_of_last_dot":
        try:
            return regex_left_of_last_dot(value)
        except Exception:
            return value
    if rule == "number":
        try:
            return float(value)
        except Exception:
            return None
    return value


# ----------------------------
# LOAD DATA DICTIONARY
# ----------------------------
def load_data_dictionary(logger):
    blob_dict_name = "CC_PM_Update_DataDict.xlsx"
    try:
        df = read_excel_from_blob(blob_dict_name, logger)
        df.columns = [c.strip() for c in df.columns]
        if "Active" in df.columns:
            df = df[df["Active"].astype(str).str.upper() == "Y"]
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
        return data_dict
    except Exception as ex:
        logger.warning("Failed to load data dictionary from Azure blob: %s", ex)
        data_dict = json.loads(DEFAULT_DATA_DICTIONARY)
        return data_dict




# ----------------------------
# APPLY LEVEL 6 HOURS
# ----------------------------

def load_level6_hours_from_excel(blob_name, logger):
    """
    Reads Level 6 hours from an Excel blob and aggregates them to Level 5.

    Returns:
        {
            level5_project_id: {
                "ENG": hours,
                "PM1": hours,
                "DNB": hours,
                "TRV": hours,
                ...
            }
        }
    """
    pm_suffixes = {"ENG", "PM1", "DNB", "TRV", "ODC", "SUB"}
    agg = {}

    df_lvl6 = read_excel_from_blob(blob_name, logger)

    for _, row in df_lvl6.iterrows():
        project_id = str(row.get("Project ID", "")).strip()
        hours = row.get("Entered Hours", 0.0)

        if not project_id:
            continue

        try:
            hours = float(hours)
        except (TypeError, ValueError):
            hours = 0.0

        tokens = project_id.split(".")
        if len(tokens) < 2:
            continue

        suffix = tokens[-1]
        if suffix not in pm_suffixes:
            continue

        level5_pid = ".".join(tokens[:-1])

        agg.setdefault(level5_pid, {})
        agg[level5_pid][suffix] = agg[level5_pid].get(suffix, 0.0) + hours

    logger.info(f"Loaded Level 6 hours for {len(agg)} Level 5 projects")
    return agg


def apply_level6_hours_to_pm_fields(
    df,
    level6_blob_name,
    logger,
    debug=False
):
    """
    Populates PM columns on Level 5 projects using a separate Level 6 Excel blob.
    """

    pm_fields = {
        "ENG": "CP ENG ACT HRS",
        "PM1": "CP PM1 ACT HRS",
        "DNB": "CP DNB ACT HRS",
        "TRV": "CP TRV ACT HRS",
        "ODC": "CP ODC ACT HRS",
        "SUB": "CP SUB ACT HRS",
    }

    # Ensure columns exist
    for col in pm_fields.values():
        if col not in df.columns:
            df[col] = 0.0

    # Load Level 6 data from Excel blob
    level6_hours = load_level6_hours_from_excel(level6_blob_name, logger)

    # Apply to Level 5 rows only
    for idx, row in df[df["Level Number"] == 5].iterrows():
        pid = row["Project ID"]
        project_hours = level6_hours.get(pid, {})

        for suffix, col_name in pm_fields.items():
            value = project_hours.get(suffix, 0.0)
            df.at[idx, col_name] = value

            if debug and value:
                logger.info(
                    f"[DEBUG] Level 5 {pid} <- {suffix} = {value}"
                )

    return df


# ----------------------------
# FILTER CP PROJECTS
# ----------------------------
def filterCPProjectsToUpdate(data_dict, filters=None, debug=False, logger=None):
    df = read_excel_from_blob(BLOB_NAME_A1, logger)
    df = apply_level6_hours_to_pm_fields(df, BLOB_NAME_A4, logger)
    df["PJ UDEF Date 1"] = pd.to_datetime(df["PJ UDEF Date 1"], errors="coerce")
    threshold_date = datetime.now() - timedelta(days=30)
    excluded_ids = ["OP-0050475"]
    filtered_df = df[
        (df["Opportunity ID"].notna()) &
        (df["Level Number"] == 5) &
        (~df["Opportunity ID"].isin(excluded_ids)) &
        ((df["PJ UDEF Date 1"].isna()) | (df["PJ UDEF Date 1"].astype(str).str.strip() == "") | (
                    df["PJ UDEF Date 1"] > threshold_date))
        ]
    if filters:
        for filter_expr in filters:
            column_name, raw_pattern = filter_expr.split("=", 1)
            column_name = column_name.strip()
            raw_pattern = raw_pattern.strip()
            regex_pattern = raw_pattern.replace("%", ".*")
            if column_name not in filtered_df.columns:
                logger.info("[FILTER WARNING] Column '%s' not in dataframe, skipping", column_name)
                continue
            compiled_regex = re.compile(regex_pattern, re.IGNORECASE)
            filtered_df = filtered_df[filtered_df[column_name].apply(lambda v: bool(compiled_regex.search(str(v))))]
            if debug:
                logger.info("[FILTER DEBUG] Applied filter: %s LIKE %s, remaining rows: %d", column_name, regex_pattern,
                            len(filtered_df))
    projects_to_update = []
    for _, row in filtered_df.iterrows():
        project_data = {}
        opportunity_id = str(row.get("Opportunity ID", ""))
        project_data["shortCode"] = opportunity_id[-7:]
        project_data["source_row"] = row
        project_data["Costpoint Update Date"] = row.get("Costpoint Update Date")
        for output_field, metadata in data_dict.items():
            source_column = metadata["cp_source"]
            transform_name = metadata["transform"]
            raw_value = row.get(source_column)
            transformed_value = transform_value(transform_name, raw_value)
            project_data[output_field] = transformed_value
        projects_to_update.append(project_data)
    logger.info("Filtered rows: %d", len(projects_to_update))
    return projects_to_update


# ----------------------------
# UPDATE PMCOM MATCHING PROJECTS
# ----------------------------

def update_pmcom_matching_projects(projects, data_dict, not_allowed_statuses, debug=False, logger=None):
    project_field_ids = load_project_field_ids(logger)
    if debug:
        projects = projects[:2]
        logger.info(f"=== DEBUG MODE: Limiting to {len(projects)} project(s) ===")

    for i, proj in enumerate(projects, start=1):
        short_code = proj["shortCode"]
        cp_project_id = proj["source_row"].get("Project ID")
        url = f"{BASE_URL}/projects?%24top=10&%24filter=shortCode eq '{short_code}'"
        resp_json = robust_get(url, headers, logger)
        data = resp_json.get("data", [])

        project = pick_pmcom_project(data, cp_project_id, short_code, logger)
        if not project:
            logger.warning("No PM.com project found for shortCode %s", short_code)
            continue

        project_id = project["id"]
        project_name = project["name"]

        # Update counters (per project)
        proj_native_updates = 0
        proj_custom_updates = 0
        task_updates = 0

        status_name = get_project_status(resp_json)
        normalized_status = (status_name or "").strip()
        logger.info(f"Status for {short_code}: {normalized_status}")
        if normalized_status in not_allowed_statuses:
            logger.warning(f"Skipping {short_code}: status '{normalized_status}' in not allowed list")
            continue

        logger.info(f"=== Project {i}/{len(projects)}: {project_name} ===")

        # timestamp logic same as before
        sheet_ts_raw = proj["Costpoint Update Date"]
        sheet_ts_dt = parse(sheet_ts_raw) if sheet_ts_raw else None
        if sheet_ts_dt and sheet_ts_dt.tzinfo is None:
            sheet_ts_dt = sheet_ts_dt.replace(tzinfo=tzutc())
        pm_ts_str = next((f["value"] for f in project.get("fieldValues", []) if f.get("name") == "CP Update Timestamp"),
                         None)
        pm_ts_dt = parse(pm_ts_str) if pm_ts_str else None
        cp_data_is_new = not pm_ts_dt or sheet_ts_dt > pm_ts_dt

        tasks = load_project_tasks(project_id, logger)
        if debug:
            tasks = tasks[:10]
        task_field_ids = load_task_field_ids(project_id, logger)
        task_dict = {t["id"]: t for t in tasks}
        task_field_map = {}
        for t in tasks:
            tf = {}
            for fv in t.get("fieldValues", []):
                tf[fv["name"].lower()] = fv.get("value")
            task_field_map[t["id"]] = tf

        # PROCESS FIELDS
        for key, meta in data_dict.items():
            value = str(proj[key]) if proj.get(key) is not None else None
            field_type = meta["field_type"]
            pm_field = meta["pm_field"].lower()
            rule = meta["update"]
            if value is None:
                continue

            # PROJ NATIVE FIELD
            if field_type == "ProjNative":
                if not cp_data_is_new:
                    continue
                put_url = f"{BASE_URL}/projects/{project_id}"
                robust_put(put_url, headers, {pm_field: value}, logger)
                proj_native_updates += 1


            # PROJ CUSTOM FIELD
            elif field_type == "ProjCustom":
                if not cp_data_is_new:
                    continue
                field_id = project_field_ids.get(pm_field)
                if not field_id:
                    continue
                if rule == "ifBlank":
                    get_url = f"{BASE_URL}/projects/{project_id}/fields/{field_id}"
                    existing_val = robust_get(get_url, headers, logger).get("data", {}).get("value")
                    if existing_val not in (None, "", " "):
                        continue
                put_url = f"{BASE_URL}/projects/{project_id}/fields/{field_id}"
                robust_put(put_url, headers, {"value": value}, logger)
                proj_custom_updates += 1


            # TASK CUSTOM FIELD
            elif field_type == "TaskCustom":
                field_id = task_field_ids.get(pm_field)
                if not field_id:
                    continue
                for task_id in task_dict.keys():
                    existing = task_field_map[task_id].get(pm_field)
                    if rule == "ifBlank" and existing not in (None, "", " "):
                        continue
                    if existing == value:
                        continue
                    put_url = f"{BASE_URL}/tasks/{task_id}/fields/{field_id}/values"
                    robust_put(put_url, headers, {"value": value}, logger)
                    task_updates += 1

        # Minimal per-project update summary
        if proj_native_updates or proj_custom_updates or task_updates:
            logger.info(
                f"✔ Updates applied for {short_code} | "
                f"Project Native: {proj_native_updates}, "
                f"Project Custom: {proj_custom_updates}, "
                f"Tasks: {task_updates}"
            )

        logger.info(f"=== Finished project {short_code} ===\n")

# ----------------------------
# RUN CP TO PMCOM
# ----------------------------
def run_cp_to_pmcom(filters=None, not_allowed_statuses=None, debug=False):
    logger, upload_log = setup_blob_logger(prefix="pm_update_log")
    try:
        if not not_allowed_statuses:
            not_allowed_statuses = ["Closed"]
        data_dict = load_data_dictionary(logger)
        projects = filterCPProjectsToUpdate(data_dict, filters=filters, debug=debug, logger=logger)
        update_pmcom_matching_projects(projects, data_dict, not_allowed_statuses, debug, logger)
    finally:
        upload_log()


# =====================
# AZURE FUNCTION APP
# =====================
app = func.FunctionApp()

# ============================
# UPDATED PMCOM HTTP FUNCTION
# (drop-in replacement ONLY)
# ============================

PMCOM_QUEUE_NAME = "cp-pmcom-queue"


@app.function_name(name="CostpointToPMcom")
@app.route(route="CostpointToPMcom", methods=["POST", "GET"])
def CostpointToPMcom(req: func.HttpRequest):
    # -------------------------
    # GET → describe function
    # -------------------------
    if req.method == "GET":
        df = read_excel_from_blob(BLOB_NAME_A1, logger=bootstrap_logger)
        cp_columns = list(df.columns)

        return func.HttpResponse(
            json.dumps({
                "description": "Queue CP → PM.com update job",
                "available_filters": cp_columns,
                "filter_syntax": "FieldName=Value or FieldName=%partial%",
                "defaults": {
                    "not_allowed_statuses": ["Closed"],
                    "debug": False
                }
            }, indent=2),
            mimetype="application/json",
            status_code=200
        )

    # -------------------------
    # POST → enqueue PMCOM job
    # -------------------------
    data = req.get_json()
    payload = {
        "filters": data.get("filters"),
        "not_allowed_statuses": data.get("not_allowed_statuses"),
        "debug": data.get("debug", False)
    }

    encoded_message = base64.b64encode(
        json.dumps(payload).encode("utf-8")
    ).decode("utf-8")

    queue_client = QueueClient.from_connection_string(
        STORAGE_CONN_STR,
        PMCOM_QUEUE_NAME
    )
    queue_client.send_message(encoded_message)

    bootstrap_logger.info(
        f"PMCOM job queued to {PMCOM_QUEUE_NAME}: {payload}"
    )

    return func.HttpResponse(
        "CP → PM.com job queued",
        status_code=202
    )


# ============================
# NEW PMCOM QUEUE FUNCTION
# (drop-in addition ONLY)
# ============================

@app.function_name(name="CostpointToPMcomQueue")
@app.queue_trigger(
    arg_name="msg",
    queue_name="cp-pmcom-queue",
    connection="AzureWebJobsStorage"
)
def CostpointToPMcomQueue(msg: func.QueueMessage):
    """
    Queue-triggered CP → PM.com processor.
    Message JSON:
      {
        "filters": [...],
        "not_allowed_statuses": [...],
        "debug": false
      }
    """
    try:
        payload = json.loads(msg.get_body().decode("utf-8"))

        bootstrap_logger.info(
            f"PMCOM queue message received: {payload}"
        )

        run_cp_to_pmcom(
            filters=payload.get("filters"),
            not_allowed_statuses=payload.get("not_allowed_statuses"),
            debug=payload.get("debug", False)
        )

    except Exception as e:
        bootstrap_logger.exception(
            f"❌ PMCOM queue processing failed: {e}"
        )
        raise  # poison-queue on failure

# =====================
# PTO IMPORT
# =====================

def format_date(dt):
    return pd.to_datetime(dt).strftime("%Y-%m-%d")

def get_project_id(short_code, logger):
    url = f"{BASE_URL}/projects?%24top=10&%24filter=shortCode eq '{short_code}'"
    resp_json = robust_get(url, headers, logger)
    project = resp_json.get("data", [])
    if not project:
        return(logger.warning("No PM.com project found for shortCode %s", short_code))
    project = project[0]
    project_id = project["id"]
    project_name = project["name"]
    logger.info(f"PTO Project Name: {project_name} | PTO Project ID: {project_id}")
    # breakpoint()
    return(project_id, project_name)

def delete_tasks(tasks, project_id, logger):
    for task in tasks:
        task_project_id = task.get("projectId")
        # Safety check
        if task_project_id != project_id:
            logger.warning(
                f"Skipping task {task.get('id')} - belongs to different project: {task_project_id}"
            )
            continue
        task_id = task.get("id")
        delete_url = f"{BASE_URL}/tasks/{task_id}"
        logger.debug(f"Deleting task {task_id} from project {project_id}")
        resp = robust_delete(delete_url, headers=headers, logger=logger)


def create_tasks_and_update_df(df, project_id, headers, logger):
    df["task_id"] = None
    url = f"https://api.projectmanager.com/api/data/projects/{project_id}/tasks"

    for idx, row in df.iterrows():
        start_date = format_date(row["SCHEDULE_DT"])
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        today = datetime.today().date()
        percent_complete = 0 if start_dt > today else 100

        payload = {
            "name": "PTO",
            "plannedStartDate": start_date,
            "plannedFinishDate": start_date,
            "plannedEffort": int(row["LEAVE_HRS"] * 60),
            # "approvalStatus": row["REQUEST_STATUS"],
            "percentComplete": percent_complete
        }

        r = robust_post(url, payload=payload, headers=headers, logger=logger)
        r.raise_for_status()
        response = r.json()
        # Put task_id back into df so we can assign resource based on lookup
        task_id = response["data"]["id"]
        df.at[idx, "task_id"] = task_id

    logger.info(f"\n{df.head(10)}")
    return df

# TODO Put this in a file anyone can edit and upload to blob
NAME_TRANSLATIONS = {
    "samuel palatucci": "sam palatucci",
    "daniel bender": "dan bender",
    "christopher dixon": "chris dixon",
    "christopher russell": "chris russell",
    "michael silverglate": "mike silverglate",
    "peter pavlovich": "pete pavlovich"
    # add more as needed
}

def translate_name(name, logger=None):
    normalized = " ".join(name.strip().lower().split())
    translated = NAME_TRANSLATIONS.get(normalized, normalized)
    if logger and normalized != translated:
        logger.info(f"Name translated: '{normalized}' → '{translated}'")

    return translated

def get_resource_lookup(logger):
    url = f"{BASE_URL}/resources"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    data = r.json()
    # Adjust depending on response shape (data vs direct list)
    resources = data.get("data", data)
    lookup = {}
    for r in resources:
        name = r.get("name")
        if name:
            lookup[name.strip()] = r.get("id")

    return lookup

def build_normalized_lookup(resource_lookup, logger):
    lookup = {}
    for name, rid in resource_lookup.items():
        normalized_name = name.strip().lower()
        lookup[normalized_name] = rid
        logger.debug(f"Resource mapping → Name: {name} | Normalized: {normalized_name} | ID: {rid}")

    return lookup

def assign_from_df(df, headers, logger):

    for _, row in df.iterrows():
        resource_id = row.get("resource_id")
        if not resource_id:
            logger.warning(f"Skipping row — no resource_id for {row.get('EMPLOYEE_NAME')}")
            continue
        payload = [{"id": resource_id}]
        url = f"https://api.projectmanager.com/api/data/tasks/{row['task_id']}/assignees"
        logger.debug(f"Assigning {resource_id} to task {row['task_id']}")

        try:
            robust_put(url, headers=headers, payload=payload, logger=logger)
        except Exception as e:
            logger.error(f"❌ Failed assigning {resource_id} to task {row['task_id']}: {e}")


def run_cp_to_pmcom_PTO(debug=False):
    logger, upload_log = setup_blob_logger(
        prefix=f"cp_to_pmcom_PTO_update_log_", debug=debug
    )

    try:
        project_id, project_name = get_project_id(PTO_PROJ_SHORTCODE, logger)

        logger.info("Getting existing PTO tasks...")
        existing_tasks = load_project_tasks(project_id, logger)

        logger.info("Deleting old PTO tasks...")
        delete_tasks(existing_tasks, project_id, logger)

        logger.info("Get data from CP PTO Report")
        df = read_excel_from_blob(BLOB_NAME_A2, logger=logger)

        logger.info("Building new PTO tasks and assigning resources")
        df["task_id"] = None
        df = create_tasks_and_update_df(df, project_id, headers, logger)

        resource_lookup = get_resource_lookup(logger)
        normalized_lookup = build_normalized_lookup(resource_lookup, logger)

        df["resource_id"] = df["EMPLOYEE_NAME"].apply(
            lambda x: normalized_lookup.get(
                NAME_TRANSLATIONS.get(
                    " ".join(x.strip().lower().split()),
                    " ".join(x.strip().lower().split())
                )
            )
        )

        assign_from_df(df, headers, logger)

        logger.info("PTO refresh complete.")

        return {"status": "success"}

    except Exception as e:
        logger.exception("PTO refresh failed")
        return {"status": "failure", "error": str(e)}

    finally:
        upload_log()

@app.function_name(name="CostpointToPMcomPTO")
@app.route(route="CostpointToPMcomPTO", methods=["POST", "GET"])  # HTTP trigger
def CostpointToPMcomPTO(req: func.HttpRequest):

    import json

    # -------------------------
    # GET → describe function
    # -------------------------
    if req.method == "GET":
        return func.HttpResponse(
            json.dumps({
                "description": "Update PM.com PTO project from CP Excel feed",
                "usage": {
                    "POST body or query param": {
                        "debug": "true | false"
                    }
                }
            }, indent=2),
            mimetype="application/json",
            status_code=200
        )

    if req.method == "POST":

        debug = False

        debug_param = req.params.get("debug")

        try:
            data = req.get_json()
        except ValueError:
            data = {}

        if not debug_param and data:
            debug_param = data.get("debug")

        if isinstance(debug_param, str):
            debug = debug_param.lower() == "true"
        elif isinstance(debug_param, bool):
            debug = debug_param

        try:
            result = run_cp_to_pmcom_PTO(debug=debug)

            return func.HttpResponse(
                f"CP to PMCOM PTO processing completed. Debug={debug}, Result={result}",
                status_code=200
            )

        except Exception as e:
            return func.HttpResponse(
                f"CP to PMCOM PTO processing failed: {str(e)}",
                status_code=500
            )

    # Unsupported method
    return func.HttpResponse(
        "Method not allowed",
        status_code=405
    )

# =====================
# SMARTSHEET IMPORT
# =====================

# =====================
# SMARTSHEET UTILS
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
    logger, upload_log = setup_blob_logger(prefix=f"smartsheet_update_log_{blob_name}")

    invocation_id = str(uuid.uuid4())
    instance = os.environ.get("WEBSITE_INSTANCE_ID", "local")
    logger.info(f"PMCOM START | instance={instance} | invocation={invocation_id}")

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

        # Unpack DataFrame and timestamp
        df = read_excel_from_blob(blob_name, logger)

        # Log the Costpoint extract timestamp for this sheet
        if "Costpoint Update Date" in df.columns:
            sheet_ts = df["Costpoint Update Date"].iloc[0]  # all rows have same timestamp
            logger.info(f"Costpoint sheet timestamp: {sheet_ts}")
        else:
            logger.warning("No Costpoint timestamp column found in DataFrame")
        sheet = smartsheet_client.Sheets.get_sheet(sheet_id)
        logger.info(f"Loaded Smartsheet '{sheet.name}' with {len(sheet.rows)} existing rows")

        clear_smartsheet(sheet, smartsheet_client, logger)

        smartsheet_columns = [c.title for c in sheet.columns]
        logger.info(f"SmartSheet columns {smartsheet_columns}")
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


# ---------------------------
# Smartsheet Update Queue-triggered function
# ---------------------------
@app.function_name(name="CostpointToSmartsheetQueue")
@app.queue_trigger(
    arg_name="msg",
    queue_name="cp-smartsheet-queue",
    connection="AzureWebJobsStorage"
)
def CostpointToSmartsheetQueue(msg: func.QueueMessage):
    """
    Queue-triggered function to run the CP → Smartsheet update asynchronously.
    Expects messages JSON with 'sheet_id' and 'blob_name'.
    """
    try:
        # runtime handles Base64, just decode bytes to string
        payload = json.loads(msg.get_body().decode("utf-8"))
        sheet_id = payload.get("sheet_id")
        blob_name = payload.get("blob_name")

        bootstrap_logger.info(f"Queue message received: sheet_id={sheet_id}, blob_name={blob_name}")

        # Call main function
        run_cp_to_smartsheet(sheet_id=sheet_id, blob_name=blob_name)

    except Exception as e:
        bootstrap_logger.exception(f"Error processing queue message: {e}")
        raise  # ensures message goes to poison queue if it fails


# ---------------------------
# Smartsheet Update HTTP-triggered function to enqueue messages
# ---------------------------
QUEUE_NAME = "cp-smartsheet-queue"

@app.function_name(name="CostpointToSmartsheet")
@app.route(route="CostpointToSmartsheet", methods=["POST"])
def CostpointToSmartsheet(req: func.HttpRequest):
    """
    HTTP POST endpoint to enqueue a Smartsheet job.
    """
    try:
        # Message payload TODO: Send blob_name from trigger for dynamic update
        payload = {
            "sheet_id": 864938054602628,
            "blob_name": BLOB_NAME_A1
        }

        # Convert to JSON, then Base64
        json_str = json.dumps(payload)
        encoded_message = base64.b64encode(json_str.encode("utf-8")).decode("utf-8")

        # CREATE QUEUE BEFORE USING
        # az storage queue create \
        #   --name cp-smartsheet-queue \
        #   --account-name costpoint1 \
        #   --account-key <STORAGE_ACCOUNT_KEY>

        # Send to queue
        queue_client = QueueClient.from_connection_string(STORAGE_CONN_STR, QUEUE_NAME)
        queue_client.send_message(encoded_message)

        bootstrap_logger.info("Base64-encoded message sent successfully!")
        bootstrap_logger.info(f"Message sent successfully to queue {QUEUE_NAME}: {payload}")

        return func.HttpResponse("Smartsheet job queued", status_code=202)

    except Exception as e:
        bootstrap_logger.exception("❌ Failed to enqueue Smartsheet job")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


# HTTP CostpointToSmartsheet A4 function
@app.function_name(name="CostpointToSmartsheetA4")
@app.route(route="CostpointToSmartsheetA4", methods=["POST"])
def CostpointToSmartsheetA4(req: func.HttpRequest):
    try:
        run_cp_to_smartsheet(
            sheet_id=2469989006135172,  # A4 Smartsheet
            blob_name=BLOB_NAME_A4  # A4 CP source
        )
        return func.HttpResponse("A4 Smartsheet sync completed", status_code=200)
    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)


if __name__ == "__main__":

    # =====================
    # LOCAL CONFIG (edit here)
    # =====================
    DEBUG = False
    UPDATE_PMCOMONLY = True
    FILTERS = []  # e.g. ["PROJ_MGR_NAME=Russell"]
    NOT_ALLOWED_STATUSES = ["CLOSED"]  # e.g. ["CLOSED", "ON_HOLD"]

    # =====================
    # LOAD CP EXCEL COLUMNS FOR HELP / VALIDATION
    # =====================
    df = read_excel_from_blob(BLOB_NAME_A1, logger=bootstrap_logger)

    bootstrap_logger.info(
        f"✅ Loaded {len(df)} rows from blob {BLOB_NAME_A1} "
    )

    cp_columns = list(df.columns)
    bootstrap_logger.info(
        f"Available CP fields for filtering: {', '.join(cp_columns)}"
    )

    # =====================
    # RUN PM.COM UPDATE
    # =====================
    try:
        run_cp_to_pmcom(
            filters=FILTERS,
            debug=DEBUG,
            not_allowed_statuses=NOT_ALLOWED_STATUSES,
        )
    except Exception as e:
        bootstrap_logger.error(f"❌ PM.com update failed: {e}", exc_info=True)

    try:
        run_cp_to_pmcom_PTO(
            debug=DEBUG
        )
    except Exception as e:
        bootstrap_logger.error(f"❌ PM.com PTO update failed: {e}", exc_info=True)

    # =====================
    # RUN SMARTSHEET UPDATE A1
    # =====================
    if not UPDATE_PMCOMONLY:
        try:
            run_cp_to_smartsheet(
                sheet_id=864938054602628,
                blob_name=BLOB_NAME_A1,
                debug=DEBUG,
            )
        except Exception as e:
            bootstrap_logger.error(f"❌ Smartsheet A1 update failed: {e}", exc_info=True)

        # =====================
        # RUN SMARTSHEET UPDATE A4
        # =====================
        try:
            run_cp_to_smartsheet(
                sheet_id=2469989006135172,
                blob_name=BLOB_NAME_A4,
                debug=DEBUG,
            )
        except Exception as e:
            bootstrap_logger.error(f"❌ Smartsheet A4 update failed: {e}", exc_info=True)