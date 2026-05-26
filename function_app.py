## Version 2.8 Adds rollup of cost to date and granular switches for running locally, remove SS Updates, further abstract utils

import pandas as pd
import json, os, logging, uuid, base64
import azure.functions as func
from azure.storage.queue import QueueClient


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
# BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER_NAME", "blob1")
BLOB_NAME_A1 = os.environ.get("BLOB_NAME_A1", "Project Data 1.xlsx")
BLOB_NAME_A2 = os.environ.get("BLOB_NAME_A2", "PTO CP to PMCOM.xlsx")
BLOB_NAME_A4 = os.environ.get("BLOB_NAME_A4", "Project Data 1CA.xlsx")
STORAGE_CONN_STR = os.environ["AzureWebJobsStorage"]
PTO_PROJ_SHORTCODE = os.environ.get("PTO_PROJ_SHORTCODE", "TimeOff")

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
# FILTER CP PROJECTS
# ----------------------------
import re
from datetime import datetime, timedelta
from utils1.excel_utils import read_excel_from_blob, write_df_to_blob_excel

def filterCPProjectsToUpdate(data_dict, filters=None, debug=False, logger=None):
    df = read_excel_from_blob(BLOB_NAME_A1, logger)
    df = apply_level6_hours_to_pm_fields(df, BLOB_NAME_A4, logger)
    df= rollup_level6_to_level5(df,"Project ID", "Actual ITD Costs", logger)
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
            logger.debug("[FILTER DEBUG] Applied filter: %s LIKE %s, remaining rows: %d", column_name, regex_pattern,
                            len(filtered_df))
    logger.info(f"filtered_df rows: {len(filtered_df)}")
    logger.info(f"filtered_df columns: {filtered_df.columns.tolist()}")
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
    # Export snapshot for audit / debugging
    try:
        write_df_to_blob_excel(projects_to_update, blob_name="project_updates.xlsx",logger=logger)
    except Exception as e:
        logger.warning(f"Blob export failed: {e}")
    logger.info("Filtered rows: %d", len(projects_to_update))

    return projects_to_update


# ----------------------------
# UPDATE PMCOM MATCHING PROJECTS
# ----------------------------

from utils1.api_call_utils import robust_put, robust_get, robust_post, robust_delete
from dateutil.parser import parse
from dateutil.tz import tzutc
from utils1.cp_project_task_data_util import (load_project_tasks,
                                              load_task_field_ids, load_project_field_ids,
                                              pick_pmcom_project, get_project_status)

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
                # if not cp_data_is_new:
                #     continue
                put_url = f"{BASE_URL}/projects/{project_id}"
                robust_put(put_url, headers, {pm_field: value}, logger)
                proj_native_updates += 1


            # PROJ CUSTOM FIELD
            elif field_type == "ProjCustom":
                # if not cp_data_is_new:
                #     continue
                field_id = project_field_ids.get(pm_field)
                if not field_id:
                    logger.warning(
                        "No CP project custom field mapping found for '%s' (shortCode=%s)", pm_field, short_code)
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

from utils1.mapping_utils import (transform_value, load_data_dictionary,apply_level6_hours_to_pm_fields,
                                rollup_level6_to_level5)
from utils1.logging_utils import setup_blob_logger

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
# PMCOM HTTP FUNCTION
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
from utils1.pto_utils import (NAME_TRANSLATIONS, build_normalized_lookup, get_resource_lookup,
                                get_project_id, format_date)

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


if __name__ == "__main__":

    # =====================
    # LOCAL CONFIG (edit here)
    # =====================
    DEBUG = False

    # 🔧 FEATURE SWITCHES
    RUN_PMCOM = False
    RUN_PMCOM_PTO = True


    FILTERS = []  # e.g. ["PROJ_MGR_NAME=Russell"]
    NOT_ALLOWED_STATUSES = ["Closed"]

    # =====================
    # LOAD CP EXCEL COLUMNS FOR HELP / VALIDATION
    # =====================
    df = read_excel_from_blob(BLOB_NAME_A1, logger=bootstrap_logger)

    bootstrap_logger.info(
        f"✅ Loaded {len(df)} rows from blob {BLOB_NAME_A1}"
    )

    cp_columns = list(df.columns)
    bootstrap_logger.info(
        f"Available CP fields for filtering: {', '.join(cp_columns)}"
    )

    # =====================
    # RUN PMCOM UPDATE
    # =====================
    if RUN_PMCOM:
        try:
            run_cp_to_pmcom(
                filters=FILTERS,
                debug=DEBUG,
                not_allowed_statuses=NOT_ALLOWED_STATUSES,
            )
        except Exception as e:
            bootstrap_logger.error(f"❌ PM.com update failed: {e}", exc_info=True)

    # =====================
    # RUN PMCOM PTO UPDATE
    # =====================
    if RUN_PMCOM_PTO:
        try:
            run_cp_to_pmcom_PTO(debug=DEBUG)
        except Exception as e:
            bootstrap_logger.error(f"❌ PM.com PTO update failed: {e}", exc_info=True)
