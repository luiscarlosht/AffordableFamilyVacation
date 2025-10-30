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


# =============================================================================
# Utility / Config
# =============================================================================

def now_utc_iso() -> str:
    """Return UTC timestamp in ISO format without microseconds."""
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def load_config(path: str = "config.yaml") -> dict:
    """
    Load YAML config, and resolve ${ENV_VAR} placeholders using environment variables.
    Example in config.yaml:
      api_key: "${OPENAI_API_KEY}"
    """
    print("[INFO] Loading config.yaml ...")
    with open(path, "r") as f:
        raw_text = f.read()

    def replace_env(m):
        var_name = m.group(1)
        return os.getenv(var_name, m.group(0))

    resolved_text = re.sub(r"\$\{([^}]+)\}", replace_env, raw_text)
    cfg = yaml.safe_load(resolved_text)
    print("[INFO] Config loaded successfully.")
    return cfg


# =============================================================================
# Google Sheets Helpers
# =============================================================================

def get_sheets_service(cfg: dict):
    """
    Build an authenticated Google Sheets API client using a service account.
    We expect:
      cfg["google_sheets"]["service_account_json"]
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
    Read Monitors tab, return only rows where Active == 'YES'.
    Attach _row_number for each (the actual row index in the sheet).
    """
    sheet_id = cfg["google_sheets"]["sheet_id"]
    monitors_tab = cfg["google_sheets"]["monitors_tab"]

    print(f"[INFO] Reading monitors from tab '{monitors_tab}' in spreadsheet {sheet_id} ...")
    resp = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{monitors_tab}!A1:O1000"
    ).execute()

    rows = resp.get("values", [])
    if not rows or len(rows) < 2:
        print("[WARN] No data rows found in Monitors (either empty or headers only).")
        return []

    headers = rows[0]
    data_rows = rows[1:]

    active_rows: List[Dict] = []
    for idx, row in enumerate(data_rows, start=2):
        row_dict = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        row_dict["_row_number"] = idx  # sheet row (2,3,4,...)
        if row_dict.get("Active", "").strip().upper() == "YES":
            active_rows.append(row_dict)

    print(f"[INFO] Found {len(active_rows)} active row(s) in Monitors.")
    return active_rows


def update_monitor_row(service, monitor_row: Dict, best_deal: Dict, cfg: dict):
    """
    Write back into Monitors tab for a single row:
      M: LastBestPricePerPerson
      N: LastBestLink
      O: LastCheckedUTC
    We always update timestamp even if no deal.
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

    print(
        f"[INFO] Updating Monitors row {sheet_row_number} "
        f"(range {cell_range}) with price={best_deal.get('price_per_person_usd','')}, "
        f"link={best_deal.get('booking_url','')}, timestamp."
    )

    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=cell_range,
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


def append_alert_to_sheet(service, deal: Dict, monitor_row: Dict, cfg: dict):
    """
    Append an alert row to Alerts_Log tab when we get a qualifying deal.

    Alerts_Log headers must be:
    TimestampUTC | OriginUsed | DestinationFound | DepartDate | ReturnDate
    | Seats | PricePerPersonUSD | TotalTripUSD | BookingLink | MonitorRowNotes
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

    print(
        f"[ALERT] Appending alert row to '{alerts_tab}' "
        f"for {deal.get('origin')}→{deal.get('destination')} "
        f"@ ${deal.get('price_per_person_usd')} pp"
    )

    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{alerts_tab}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


def append_runlog(service, cfg: dict, status: str, active_count: int, notes: str):
    """
    Append a heartbeat row to Run_Log tab so you can confirm the script ran.

    Run_Log headers must be:
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

    print(
        f"[INFO] Appending Run_Log row to '{runlog_tab}': "
        f"status={status}, active={active_count}, notes={notes}"
    )

    try:
        service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=f"{runlog_tab}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        print("[INFO] Run_Log append OK.")
    except Exception as e:
        print(f"[ERROR] Failed to append Run_Log: {e}")


# =============================================================================
# GPT Flight Search
# =============================================================================

def build_prompt_for_search(monitor_row: Dict, cfg: dict) -> str:
    """
    Build instructions for GPT-5 Thinking to go search cheapest flights.
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
- Connections/layovers are OK
- Alert if price per person <= ${max_price} USD.

Return JSON with key 'deals' (max length 3), each entry using keys:
["origin","destination","depart","return","airline",
 "price_per_person_usd","total_for_group_usd","seats_available","booking_url"]
"""
    return prompt


def call_gpt_web(prompt: str, cfg: dict) -> str:
    """
    Calls GPT-5 Thinking using OpenAI Responses API.
    Uses OPENAI_API_KEY from environment first, else config.yaml['openai']['api_key'].
    """
    api_key = os.getenv("OPENAI_API_KEY") or cfg["openai"].get("api_key")
    if not api_key:
        raise RuntimeError("OpenAI API key not found. Set OPENAI_API_KEY or config.yaml openai.api_key")

    print("[INFO] Calling GPT-5 Thinking for flight search ...")
    client = OpenAI(api_key=api_key)

    completion = client.responses.create(
        model=cfg["openai"]["model"],
        input=[{"role": "user", "content": prompt}],
        extra_headers=cfg["openai"].get("extra_headers", {})
    )

    # stitch together assistant text
    text_chunks = []
    for item in completion.output:
        if item.get("content"):
            for c in item["content"]:
                if c.get("type") == "output_text":
                    text_chunks.append(c["text"])

    full_text = "\n".join(text_chunks)
    print("[INFO] GPT-5 response received.")
    print(f"[DEBUG] GPT raw first 500 chars:\n{full_text[:500]}")
    return full_text


def extract_deals_from_gpt(raw_text: str) -> List[Dict]:
    """
    Pull the array assigned to "deals" out of GPT response text.
    We'll look for something like:
      "deals": [ {...}, {...} ]
    """
    match = re.search(r'("deals"\s*:\s*\[.*?\])', raw_text, flags=re.DOTALL)
    if not match:
        print("[WARN] No 'deals' array found in GPT response.")
        return []

    json_snippet = "{ " + match.group(1) + " }"
    print(f"[DEBUG] Parsing deals JSON snippet (truncated 400 chars):\n{json_snippet[:400]}")

    try:
        parsed = json.loads(json_snippet)
        deals = parsed.get("deals", [])
        print(f"[INFO] Parsed {len(deals)} deal(s) from GPT output.")
        return deals
    except Exception as e:
        print(f"[ERROR] Failed to parse GPT JSON: {e}")
        return []


# =============================================================================
# Email Notifications
# =============================================================================

def send_email_alert(deal: Dict, monitor_row: Dict, cfg: dict):
    """
    Send an email alert for any deal that meets threshold.
    Requires cfg['email'] section with SMTP creds.
    """
    email_cfg = cfg["email"]

    subject = (
        f"✈ Flight Deal: {deal.get('origin')}→{deal.get('destination')} "
        f"${deal.get('price_per_person_usd')} pp"
    )

    body = f"""
Deal found!

Route: {deal.get('origin')} → {deal.get('destination')}
Depart: {deal.get('depart')}
Return: {deal.get('return')}
Airline: {deal.get('airline')}
Seats requested: {monitor_row.get('Seats')}
Seats available: {deal.get('seats_available')}

Price/person: ${deal.get('price_per_person_usd')}
Total: ${deal.get('total_for_group_usd')}

Booking link:
{deal.get('booking_url')}

Monitor notes:
{monitor_row.get('Notes')}

Time (UTC): {now_utc_iso()}
""".strip()

    print(f"[ALERT] Sending email: {subject}")
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = email_cfg["from"]
    msg["To"] = ", ".join(email_cfg["to"])

    with smtplib.SMTP(email_cfg["smtp_host"], email_cfg["smtp_port"]) as server:
        server.starttls()
        server.login(email_cfg["smtp_user"], email_cfg["smtp_pass"])
        server.sendmail(email_cfg["from"], email_cfg["to"], msg.as_string())

    print("[ALERT] Email sent successfully.")


# =============================================================================
# Single-Monitor Handler
# =============================================================================

def handle_single_monitor(service, cfg: dict, mrow: Dict) -> str:
    """
    Process one active monitor row:
    - build GPT prompt
    - query GPT
    - parse top deals
    - update Monitors row with best seen
    - if deal is cheap enough, send alert + log in Alerts_Log
    Return a short status string like 'alert_sent', 'checked_no_alert', etc.
    """
    rownum = mrow["_row_number"]
    dest = mrow.get("Destination", "(OPEN)")
    orig = mrow.get("OriginAirports", "")
    print(f"[INFO] Processing monitor row {rownum}: {orig} → {dest}")

    prompt = build_prompt_for_search(mrow, cfg)

    # 1. Call GPT
    try:
        raw_text = call_gpt_web(prompt, cfg)
    except Exception as e:
        print(f"[ERROR] GPT call failed for row {rownum}: {e}")
        # still mark timestamp so you see attempt
        dummy_deal = {
            "price_per_person_usd": mrow.get("LastBestPricePerPerson", ""),
            "booking_url": mrow.get("LastBestLink", "")
        }
        update_monitor_row(service, mrow, dummy_deal, cfg)
        return f"gpt_error:{repr(e)}"

    # 2. Parse deals from GPT output
    deals = extract_deals_from_gpt(raw_text)

    # 3. Determine alert threshold
    try:
        threshold = float(
            mrow.get("MaxPricePerPersonUSD", "").strip()
            or cfg["scan_defaults"]["default_threshold"]
        )
    except Exception:
        threshold = float(cfg["scan_defaults"]["default_threshold"])

    if not deals:
        print(f"[INFO] Row {rownum}: no deals parsed.")
        dummy_deal = {
            "price_per_person_usd": mrow.get("LastBestPricePerPerson", ""),
            "booking_url": mrow.get("LastBestLink", "")
        }
        update_monitor_row(service, mrow, dummy_deal, cfg)
        return "no_deals_found"

    # 4. Sort by cheapest price_per_person_usd
    def price_val(d):
        try:
            return float(d.get("price_per_person_usd", "999999"))
        except Exception:
            return 999999.0

    deals.sort(key=price_val)
    best = deals[0]

    print(
        f"[INFO] Row {rownum}: cheapest {best.get('origin')}→{best.get('destination')} "
        f"at ${best.get('price_per_person_usd')} per person"
    )

    # 5. Update Monitors row with best deal + timestamp
    update_monitor_row(service, mrow, best, cfg)

    # 6. Check if we send alert
    try:
        best_price = float(best.get("price_per_person_usd", "999999"))
    except Exception:
        best_price = 999999.0

    if best_price <= threshold:
        print(f"[INFO] Row {rownum}: deal meets threshold ({best_price} <= {threshold}). Alerting.")
        send_email_alert(best, mrow, cfg)
        append_alert_to_sheet(service, best, mrow, cfg)
        return "alert_sent"

    print(f"[INFO] Row {rownum}: deal does NOT meet threshold ({best_price} > {threshold}).")
    return "checked_no_alert"


# =============================================================================
# Main Orchestration
# =============================================================================

def monitor_once():
    """
    - Load config
    - Connect to Sheets
    - Read active monitors
    - Process each
    - ALWAYS append a row to Run_Log (or log why we couldn't)
    """
    print("========== monitor start ==========")

    cfg = load_config("config.yaml")
    service = get_sheets_service(cfg)

    monitors = read_monitors(service, cfg)

    if not monitors:
        print("[INFO] No active monitors found in sheet.")
        append_runlog(
            service,
            cfg,
            status="no_active_monitors",
            active_count=0,
            notes="nothing_to_scan"
        )
        print("========== monitor end (no ac
