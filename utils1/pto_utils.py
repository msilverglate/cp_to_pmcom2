# PTO UTIL V1

from utils1.api_call_utils import robust_get
import os
import pandas as pd
import requests

BASE_URL = "https://api.projectmanager.com/api/data"
API_KEY = os.environ.get("PM_API_KEY")
if not API_KEY:
    raise RuntimeError("Set API_KEY in environment first!")

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}


def format_date(dt):
    return pd.to_datetime(dt).strftime("%Y-%m-%d")


def get_project_id(short_code, logger):
    url = f"{BASE_URL}/projects?%24top=10&%24filter=shortCode eq '{short_code}'"
    resp_json = robust_get(url, headers, logger)
    project = resp_json.get("data", [])
    if not project:
        return (logger.warning("No PM.com project found for shortCode %s", short_code))
    project = project[0]
    project_id = project["id"]
    project_name = project["name"]
    logger.info(f"PTO Project Name: {project_name} | PTO Project ID: {project_id}")
    # breakpoint()
    return (project_id, project_name)


# TODO Put this in a file anyone can edit and upload to blob
NAME_TRANSLATIONS = {
    "samuel palatucci": "sam palatucci",
    "daniel bender": "dan bender",
    "christopher dixon": "chris dixon",
    "christopher russell": "(rs) chris russell",
    "michael silverglate": "mike silverglate",
    "peter pavlovich": "pete pavlovich",
    "rostislav veniaminov": "slava veniaminov"
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
