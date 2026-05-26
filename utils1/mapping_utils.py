## Mapping Util Version 1

import re
import pandas as pd
from datetime import datetime, timedelta
import json
from utils1.excel_utils import read_excel_from_blob, write_df_to_blob_excel

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
    },

    "cpActualCost": {
    "cp_source": "Actual ITD Costs",
    "field_type": "ProjCustom",
    "pm_field": "CP Cost to Date",
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


def rollup_level6_to_level5(df, project_col, cost_col, logger):
    """
    Rolls up Level 6 costs into Level 5 projects by updating the Level 5 rows.

    Args:
        df (pd.DataFrame): Source dataframe
        project_col (str): Column containing project IDs
        cost_col (str): Column containing cost values

    Returns:
        pd.DataFrame: Updated dataframe with Level 5 costs rolled up
    """

    df = df.copy()

    # --- Identify Level 5 vs Level 6 ---
    df["level"] = df[project_col].str.count(r"\.") + 1

    # Level 5 = 6 segments (P001.R033545.001.00.PSF)
    # Level 6 = 7 segments (P001.R033545.001.00.PSF.ENG)
    level5 = df[df["level"] == 5].copy()
    level6 = df[df["level"] == 6].copy()
    logger.info(f"Level 5 count: {len(level5)}")
    # --- Derive parent ID for Level 6 ---
    level6["parent_id"] = level6[project_col].str.rsplit(".", n=1).str[0]

    # --- Aggregate Level 6 costs ---
    agg_costs = (
        level6.groupby("parent_id")[cost_col]
        .sum()
        .reset_index()
        .rename(columns={cost_col: "rolled_cost"})
    )

    # --- Merge into Level 5 ---
    level5 = level5.merge(
        agg_costs,
        left_on=project_col,
        right_on="parent_id",
        how="left"
    )

    # --- Update Level 5 cost ---
    level5[cost_col] = level5["rolled_cost"].fillna(0)

    # Cleanup
    level5.drop(columns=["parent_id", "rolled_cost"], inplace=True)
    logger.info(f"Loaded Level 6 costs to {len(level5)} Level 5 projects")

    return level5
