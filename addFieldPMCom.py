import os
import requests
import json
import sys

# =====================
# CONFIGURATION BLOCK
# =====================

# ---------------------
# API key
# ---------------------
API_KEY = os.environ.get("PM_API_KEY")  # Make sure this is set in your env

# ---------------------
# Operation options
# ---------------------
FIELD_TYPE = "project"       # "project" or "task"
OPERATION = "create"      # "create" or "delete"
DEBUG = False              # True = dry-run, False = actually create/delete
SHORT_CODE = "0108115"    # For single project task operation (ignored for project fields)
APPLY_TO_ALL = False      # True = apply to all projects (task fields only)

# ---------------------
# Field definition
# ---------------------
NEW_FIELD = {
    "name": "CP Update Timestamp",
    "type": "string",       # string, number, date, bool, currency, dropdown-single, dropdown-multi
    "description": "Created via script",
    "required": False
}

# =====================
# CONSTANTS
# =====================
BASE_URL = "https://api.projectmanager.com/api/data"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# =====================
# FUNCTIONS
# =====================

def list_project_fields(debug=False):
    """Return workspace-wide project fields"""
    url = f"{BASE_URL}/projects/fields"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"[ERROR] Failed to list project fields: {resp.status_code}")
        return []

    fields = resp.json().get("data", [])
    if debug:
        print("\n[DEBUG] Existing workspace-wide project fields:")
        print(json.dumps(fields, indent=2))
    return fields


def list_task_fields(project_id, debug=False):
    url = f"{BASE_URL}/projects/{project_id}/tasks/fields"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"[ERROR] Failed to list task fields for project {project_id}: {resp.status_code}")
        return []

    fields = resp.json().get("data", [])
    if debug:
        print(f"\n[DEBUG] Existing task fields for project {project_id}:")
        print(json.dumps(fields, indent=2))
    return fields


def get_project(project_short_code):
    url = f"{BASE_URL}/projects?%24top=1&%24filter=shortCode eq '{project_short_code}'"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"[ERROR] Project lookup failed: {resp.status_code}")
        return None
    data = resp.json().get("data", [])
    if not data:
        print(f"[ERROR] No project found for shortCode '{project_short_code}'")
        return None
    return {
        "id": data[0]["id"],
        "name": data[0].get("name"),
        "shortCode": data[0].get("shortCode")
    }


def list_all_projects():
    url = f"{BASE_URL}/projects?%24top=5000"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to fetch projects: {resp.status_code}")
    return resp.json().get("data", [])


def create_project_field(field_def, debug=False):
    fields = list_project_fields(debug=debug)

    if any(f["name"].strip().lower() == field_def["name"].strip().lower() for f in fields):
        print(f"[SKIP] Workspace-wide project field '{field_def['name']}' already exists")
        return

    url = f"{BASE_URL}/projects/fields"
    payload = {
        "name": field_def["name"],
        "type": field_def["type"],
        "description": field_def.get("description", ""),
        "required": field_def.get("required", False)
    }
    if field_def["type"] in ("dropdown-single", "dropdown-multi"):
        payload["options"] = field_def.get("options", [])

    print("\n[INFO] Payload (workspace-wide project field):")
    print(json.dumps(payload, indent=2))
    if debug:
        print("[DEBUG] Dry-run only; no field created")
        return

    resp = requests.post(url, headers=HEADERS, json=payload)
    if resp.status_code in (200, 201, 204):
        print(f"[SUCCESS] Workspace-wide project field '{field_def['name']}' created")
    else:
        print(f"[ERROR] Field creation failed (workspace-wide)")
        print("Status:", resp.status_code)
        print("Body:", resp.text)


def delete_project_field(field_name, debug=False):
    fields = list_project_fields(debug=debug)
    match = next((f for f in fields if f["name"].strip().lower() == field_name.strip().lower()), None)
    if not match:
        print(f"[INFO] Workspace-wide project field '{field_name}' does not exist")
        return

    field_id = match["id"]
    print(f"[INFO] Deleting workspace-wide project field '{field_name}' (ID: {field_id})")
    if debug:
        print("[DEBUG] Dry-run only; no field deleted")
        return

    url = f"{BASE_URL}/projects/fields/{field_id}"
    resp = requests.delete(url, headers=HEADERS)
    if resp.status_code in (200, 204):
        print(f"[SUCCESS] Field '{field_name}' deleted")
    else:
        print(f"[ERROR] Failed to delete field '{field_name}'")
        print("Status:", resp.status_code)
        print("Body:", resp.text)


def create_task_field(project_id, field_def, debug=False):
    fields = list_task_fields(project_id, debug=debug)
    if any(f["name"].strip().lower() == field_def["name"].strip().lower() for f in fields):
        print(f"[SKIP] Task field '{field_def['name']}' already exists on project {project_id}")
        return

    url = f"{BASE_URL}/projects/{project_id}/tasks/fields"
    payload = {
        "name": field_def["name"],
        "type": field_def["type"],
        "description": field_def.get("description", ""),
        "required": field_def.get("required", False)
    }
    if field_def["type"] in ("dropdown-single", "dropdown-multi"):
        payload["options"] = field_def.get("options", [])

    print("\n[INFO] Payload (task field):")
    print(json.dumps(payload, indent=2))
    if debug:
        print("[DEBUG] Dry-run only; no field created")
        return

    resp = requests.post(url, headers=HEADERS, json=payload)
    if resp.status_code in (200, 201, 204):
        print(f"[SUCCESS] Task field '{field_def['name']}' created for project {project_id}")
    else:
        print(f"[ERROR] Task field creation failed for project {project_id}")
        print("Status:", resp.status_code)
        print("Body:", resp.text)


def delete_task_field(project_id, field_name, debug=False):
    fields = list_task_fields(project_id, debug=debug)
    match = next((f for f in fields if f["name"].strip().lower() == field_name.strip().lower()), None)
    if not match:
        print(f"[INFO] Task field '{field_name}' does not exist on project {project_id}")
        return

    field_id = match["id"]
    print(f"[INFO] Deleting task field '{field_name}' for project {project_id} (ID: {field_id})")
    if debug:
        print("[DEBUG] Dry-run only; no field deleted")
        return

    url = f"{BASE_URL}/projects/{project_id}/tasks/fields/{field_id}"
    resp = requests.delete(url, headers=HEADERS)
    if resp.status_code in (200, 204):
        print(f"[SUCCESS] Task field '{field_name}' deleted for project {project_id}")
    else:
        print(f"[ERROR] Failed to delete task field '{field_name}' for project {project_id}")
        print("Status:", resp.status_code)
        print("Body:", resp.text)


# =====================
# MAIN EXECUTION
# =====================

if not API_KEY:
    raise RuntimeError("PM_API_KEY environment variable is not set")

if FIELD_TYPE == "project":
    if OPERATION == "delete":
        delete_project_field(NEW_FIELD["name"], debug=DEBUG)
    else:
        create_project_field(NEW_FIELD, debug=DEBUG)
    sys.exit(0)

# Task field logic
if FIELD_TYPE == "task":
    if APPLY_TO_ALL:
        projects = list_all_projects()
        print(f"\n⚠️ You are about to operate on {len(projects)} projects")
        confirmation = input("Type EXACTLY 'APPLY TO ALL PROJECTS' to continue: ")
        if confirmation != "APPLY TO ALL PROJECTS":
            print("[ABORTED] Confirmation failed")
            sys.exit(1)

        for i, p in enumerate(projects, start=1):
            print(f"\n=== Project {i}/{len(projects)} :: {p.get('shortCode')} ===")
            if OPERATION == "delete":
                delete_task_field(p.get("id"), NEW_FIELD["name"], debug=DEBUG)
            else:
                create_task_field(p.get("id"), NEW_FIELD, debug=DEBUG)

    else:  # single project
        project = get_project(SHORT_CODE)
        if not project:
            sys.exit(1)
        print("\n[INFO] Project resolved:")
        print(f"  Name      : {project['name']}")
        print(f"  ShortCode : {project['shortCode']}")
        print(f"  ID        : {project['id']}")

        if OPERATION == "delete":
            delete_task_field(project["id"], NEW_FIELD["name"], debug=DEBUG)
        else:
            create_task_field(project["id"], NEW_FIELD, debug=DEBUG)
