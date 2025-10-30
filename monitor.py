import os
import re
import yaml
import datetime
import smtplib
import json
from typing import List, Dict
from email.mime.text import MIMEText

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from openai import OpenAI


# -----------------------------------------------------------------------------
# CONFIG LOADER (supports ${ENV_VAR} syntax in config.yaml)
# -----------------------------------------------------------------------------
def load_config(path="config.yaml"):
    """Load YAML config, resolve ${ENV_VAR} placeholders from environment."""
    with open(path, "r") as f:
        raw_text = f.read()

    def replace_env(match):
        var_name = match.group(1)
        return os.getenv(var_name, match.group(0))

    resolved_text = re.sub(r"\$\{([^}]+)\}", replace_env, raw_text)
    return yaml.safe_load(resolved_text)


def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


# -----------------------------------------------------------------------------
# GOOGLE SHEETS UTILITIES
# -----------------------------------------------------------------------------
def get_sheets_service(cfg: dict):
    """
    Build an authenticated Google Sheets API client using a service account.
    """
    sa_path = cfg["google_sheets"]["service_account_json"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    service = build("sheets", "v4", credentials=creds)
    return service


def read_monitors(service, cfg: dict) -> List[Dict]:
    """
    Read the Monitors tab and return all rows where Active == YES.
    Also attach _row_number so we know which row to update later.
    """
    sheet_id = cfg["google_sheets"]["sheet_id"]
    monitors_tab = cfg["google_sheets"]["monitors_tab"]

    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{monitors_tab}!A1:O1000"
    ).execute()

    rows = result.get("values", [])
    if not rows or len(rows) < 2:
        return []

    headers = rows[0]
    data_rows = rows[1:]

    active_rows = []
    for idx, row in enumerate(data_rows, start=2):
        row_dict = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        row_dict["_row_nu]()_
