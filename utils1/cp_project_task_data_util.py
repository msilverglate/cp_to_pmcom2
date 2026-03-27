## Version 1.0 broke out data retrieval from PMCOM

import os
from utils1.api_call_utils import robust_get


# ----------------------------
# CONFIG
# ----------------------------
BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER_NAME", "blob1")
BLOB_NAME_A1 = os.environ.get("BLOB_NAME_A1", "Project Data 1.xlsx")
BLOB_NAME_A2 = os.environ.get("BLOB_NAME_A2", "PTO CP to PMCOM.xlsx")
BLOB_NAME_A4 = os.environ.get("BLOB_NAME_A4", "Project Data 1CA.xlsx")
STORAGE_CONN_STR = os.environ["AzureWebJobsStorage"]
PTO_PROJ_SHORTCODE = os.environ.get("PTO_PROJ_SHORTCODE","CopyPTO")

BASE_URL = "https://api.projectmanager.com/api/data"
API_KEY = os.environ.get("PM_API_KEY")
if not API_KEY:
    raise RuntimeError("Set API_KEY in environment first!")

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}


def get_project_status(response_json):
    if not response_json or "data" not in response_json:
        return None
    data = response_json.get("data", [])
    if not data:
        return None
    project = data[0]
    status = project.get("status", {})
    return status.get("name")


def load_project_field_ids(logger):
    url = f"{BASE_URL}/projects/fields"
    resp = robust_get(url, headers, logger)
    fields = resp.get("data", [])
    return {f["name"].strip().lower(): f["id"] for f in fields}


def load_task_field_ids(project_id, logger):
    url = f"{BASE_URL}/projects/{project_id}/tasks/fields"
    resp = robust_get(url, headers, logger)
    fields = resp.get("data", [])
    return {f["name"].strip().lower(): f["id"] for f in fields}


def load_project_tasks(project_id, logger):
    url = f"{BASE_URL}/tasks?%24filter=projectId%20eq%20{project_id}"
    resp = robust_get(url, headers, logger)
    tasks = resp.get("data", [])

    for t in tasks:
        name = (t.get("name") or "").strip()
        start_date = t.get("plannedStartDate")
        logger.debug(f"Task: {name} | Start Date: {start_date}")
    # breakpoint()
    return resp.get("data", [])


def get_task_field_value(task_id, field_id, logger):
    url = f"{BASE_URL}/tasks/{task_id}/fields/{field_id}/values"
    resp = robust_get(url, headers, logger)
    data = resp.get("data")
    if isinstance(data, list) and data:
        return data[0].get("value")
    elif isinstance(data, dict):
        return data.get("value")
    return None

def pick_pmcom_project(data: list, cp_project_id: str, short_code: str, logger):
    """Pick correct PM.com project when multiple rows returned for the same shortCode."""
    if not data:
        return None
    if len(data) == 1:
        return data[0]

    logger.warning("Multiple PM.com projects returned for shortCode %s (%d rows)", short_code, len(data))
    project = next(
        (row for row in data if "chargeCode" in row and cp_project_id in row["chargeCode"].get("name", "")),
        data[0]
    )

    if any("chargeCode" in row and cp_project_id in row["chargeCode"].get("name", "") for row in data):
        logger.info("Matched PM.com project %s to CP Project ID %s via chargeCode", project["id"], cp_project_id)
    else:
        logger.warning("No PM.com project matched chargeCode for CP Project ID %s. Keeping first project: %s", cp_project_id, project.get("id"))

    return project

