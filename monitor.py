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
    print("[INFO] Loading config.yaml ...")
    with open(path, "r") as f:
        raw_text = f.read()

    def replace_env(match):
        var_name = match.group(1)
        return os.getenv(var_name, match.group(0))

    resolved_text = re.sub(r"\$\{([^}]+)\}", replace_env, raw_text)
    cfg = yaml.safe_load(resolved_text)
    print("[INFO] Config loaded.")
    return cfg


def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


# -----------------------------------------------------------------------------
# GOOGLE SHEETS UTILITIES
# -----------------------------------------------------------------------------
def get_sheets_service(cfg: dict):
    """
    Build an authenticated Google Sheets API client using a service account.
    """
    print("[INFO] Initializing Google Sheets service ...")
    sa_path = cfg["google_sheets"]["service_account_json"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    service = build("sheets", "v4", credentials=creds)
    print("[INFO] Google Sheets service initialized.")
    return service


def read_monitors(service, cfg: dict) -> List[Dict]:
    """
    Read the Monitors tab and return all rows where Active == YES.
    Also attach _row_number so we know which row to update later.
    """
    sheet_id = cfg["google_sheets"]["sheet_id"]
    monitors_tab = cfg["google_sheets"]["monitors_tab"]

    print(f"[INFO] Reading monitors from sheet '{monitors_tab}' in spreadsheet {sheet_id} ...")
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{monitors_tab}!A1:O1000"
    ).execute()

    rows = result.get("values", [])
    if not rows or len(rows) < 2:
        print("[WARN] No rows (or only headers) found in Monitors tab.")
        return []

    headers = rows[0]
    data_rows = rows[1:]

    active_rows = []
    for idx, row in enumerate(data_rows, start=2):
        row_dict = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        row_dict["_row_number"] = idx
        if row_dict.get("Active", "").strip().upper() == "YES":
            active_rows.append(row_dict)

    print(f"[INFO] Found {len(active_rows)} active monitor row(s).")
    return active_rows


# -----------------------------------------------------------------------------
# LOGGING TO SHEET (Run_Log, Alerts_Log, updating Monitors row)
# -----------------------------------------------------------------------------
def append_runlog(service, cfg: dict, status: str, active_count: int, notes: str):
    """
    Append a single row to the Run_Log tab so we can see that the script ran.
    Columns:
    TimestampUTC | Status | ActiveMonitorCount | Notes
    """
    sheet_id = cfg["google_sheets"]["sheet_id"]
    runlog_tab = cfg["google_sheets"]["runlog_tab"]

    values = [[
        now_utc_iso(),
        status,
        str(active_count),
        notes
    ]]

    print(f"[INFO] Appending run log entry to tab '{runlog_tab}': status={status}, active={active_count}, notes={notes}")
    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{runlog_tab}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


def append_alert_to_sheet(service, deal: Dict, monitor_row: Dict, cfg: dict):
    """
    Append an alert row to Alerts_Log tab when we actually find a qualifying deal.
    Columns:
    TimestampUTC, OriginUsed, DestinationFound, DepartDate, ReturnDate,
    Seats, PricePerPersonUSD, TotalTripUSD, BookingLink, MonitorRowNotes
    """
    sheet_id = cfg["google_sheets"]["sheet_id"]
    alerts_tab = cfg["google_sheets"]["alerts_tab"]

    values = [[
        now_utc_iso(),
        deal.get("origin", ""),
        deal.get("destination", ""),
        deal.get("depart", ""),
        deal.get("return", ""),
        monitor_row.get("Seats", ""),
        deal.get("price_per_person_usd", ""),
        deal.get("total_for_group_usd", ""),
        deal.get("booking_url", ""),
        monitor_row.get("Notes", ""),
    ]]

    print(f"[ALERT] Logging alert in '{alerts_tab}' for {deal.get('origin')}→{deal.get('destination')} at ${deal.get('price_per_person_usd')} pp")
    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{alerts_tab}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


def update_monitor_row(service, monitor_row: Dict, best_deal: Dict, cfg: dict):
    """
    Write back into Monitors tab:
    - LastBestPricePerPerson  (col M)
    - LastBestLink            (col N)
    - LastCheckedUTC          (col O)
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

    print(f"[INFO] Updating Monitors row {sheet_row_number} with best price {best_deal.get('price_per_person_usd','')} and timestamp.")
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=cell_range,
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


# -----------------------------------------------------------------------------
# GPT-5 CALL AND RESULT PARSING
# -----------------------------------------------------------------------------
def build_prompt_for_search(monitor_row: Dict, cfg: dict) -> str:
    """
    Build the web-search prompt for GPT-5 Thinking.
    """
    scan_defaults = cfg["scan_defaults"]

    origin_home = monitor_row.get("OriginHome", "").strip() or scan_defaults["home_fallback"]

    origin_airports = monitor_row.get("OriginAirports", "").strip()
    if not origin_airports:
        origin_airports = ",".join(scan_defaults["default_origin_airports"])

    destination = monitor_row.get("Destination", "").strip()
    depart_start = monitor_row.get("DepartStart", "")
    depart_end = monitor_row.get("DepartEnd", "")
    return_start = monitor_row.get("ReturnStart", "")
    return_end = monitor_row.get("ReturnEnd", "")

    seats = monitor_row.get("Seats", "").strip() or str(scan_defaults["default_seats"])
    max_price = monitor_row.get("MaxPricePerPersonUSD", "").strip() or str(scan_defaults["default_threshold"])

    open_destination = (destination == "")

    prompt = f"""
You are a flight deal scout. Search live flight prices on major public travel sites.

Goal:
Find the CHEAPEST flight options that satisfy:
- Origin airports: {origin_airports} (airports ~100 miles from {origin_home} are allowed)
- Destination: {"OPEN / ANYWHERE cheap" if open_destination else destination}
- Departure date between {depart_start} and {depart_end}
- Return date between {return_start} and {return_end} (if blank, treat as one-way)
- Number of travelers: {seats} seats
- Connections are OK
- Alert if price per person <= ${max_price} USD.

Return JSON with key 'deals' (max length 3), each having keys:
["origin","destination","depart","return","airline",
 "price_per_person_usd","total_for_group_usd","seats_available","booking_url"]
"""
    return prompt


def call_gpt_web(prompt: str, cfg: dict) -> str:
    """
    Calls GPT-5 Thinking with browsing enabled.
    Uses OPENAI_API_KEY from env first, then falls back to config.yaml.
    """
    api_key = os.getenv("OPENAI_API_KEY") or cfg["openai"].get("api_key")
    if not api_key:
        raise RuntimeError("OpenAI API key not found. Set OPENAI_API_KEY env var or config.yaml openai.api_key")

    print("[INFO] Calling GPT-5 Thinking for flight search ...")
    client = OpenAI(api_key=api_key)

    completion = client.responses.create(
        model=cfg["openai"]["model"],
        input=[{"role": "user", "content": prompt}],
        extra_headers=cfg["openai"].get("extra_headers", {})
    )

    text_chunks = []
    for item in completion.output:
        if item.get("content"):
            for c in item["content"]:
                if c["type"] == "output_text":
                    text_chunks.append(c["text"])

    print("[INFO] GPT-5 response received.")
    return "\n".join(text_chunks)


def extract_deals_from_gpt(raw_text: str) -> List[Dict]:
    """
    Pull a 'deals' array out of GPT response text.
    """
    match = re.search(r'("deals"\s*:\s*\[.*?\])', raw_text, flags=re.D_*]()
