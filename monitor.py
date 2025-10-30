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
# UTIL / CONFIG
# -----------------------------------------------------------------------------
def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def load_config(path: str = "config.yaml") -> dict:
    """Load YAML config, resolve ${ENV_VAR} placeholders from environment."""
    print("[INFO] Loading config.yaml ...")
    with open(path, "r") as f:
        raw_text = f.read()

    def replace_env(match):
        var_name = match.group(1)
        return os.getenv(var_name, match.group(0))

    resolved_text = re.sub(r"\$\{([^}]+)\}", replace_env, raw_text)
    cfg = yaml.safe_load(resolved_text)
    print("[INFO] Config loaded successfully.")
    return cfg


# -----------------------------------------------------------------------------
# GOOGLE SHEETS HELPERS
# -----------------------------------------------------------------------------
def get_sheets_service(cfg: dict):
    """
    Build an authenticated Google Sheets API client using a service account.
    """
    print("[INFO] Initializing Google Sheets client ...")
    sa_path = cfg["google_sheets"]["service_account_json"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    service = build("sheets", "v4", credentials=creds)
    print("[INFO] Google Sheets client ready.")
    return service


def read_monitors(service, cfg: dict) -> List[Dict]:
    """
    Read Monitors tab, return only rows where Active == YES.
    Attach _row_number for later updates.
    """
    sheet_id = cfg["google_sheets"]["sheet_id"]
    monitors_tab = cfg["google_sheets"]["monitors_tab"]

    print(f"[INFO] Reading monitors from '{monitors_tab}' in spreadsheet {sheet_id} ...")
    resp = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{monitors_tab}!A1:O1000"
    ).execute()

    rows = resp.get("values", [])
    if not rows or len(rows) < 2:
        print("[WARN] No data rows found in Monitors (only headers or empty).")
        return []

    headers = rows[0]
    data_rows = rows[1:]

    active_rows = []
    for idx, row in enumerate(data_rows, start=2):
        row_dict = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        # attach physical row index in sheet (2 = first data row)
        row_dict["_row_number"] = idx
        if row_dict.get("Active", "").strip().upper() == "YES":
            active_rows.append(row_dict)

    print(f"[INFO] Found {len(active_rows)} active row(s) with Active=YES.")
    return active_rows


def update_monitor_row(service, monitor_row: Dict, best_deal: Dict, cfg: dict):
    """
    Write back into Monitors tab:
    M: LastBestPricePerPerson
    N: LastBestLink
    O: LastCheckedUTC
    """
    sheet_id = cfg["google_sheets"]["sheet_id"]
    monitors_tab = cfg["google_sheets"]["monitors_tab"]
    sheet_row_number = monitor_row["_row_number"]

    cell_range = f"{monitors_tab}!M{sheet_row_number}:O{sheet_row_number}"
    values = [[
        best_deal.get("price_per_person_usd", ""),
        best_deal.get("booking_url", ""),
        now_utc_iso()
    ]]

    print(f"[INFO] Updating Monitors row {sheet_row_number} (range {cell_range}) with:")
    print(f"       price_per_person={best_deal.get('price_per_person_usd','')}, link={best_deal.get('booking_url','')}")
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=cell_range,
        valueInputOpti
