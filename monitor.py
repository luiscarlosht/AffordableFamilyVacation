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


# =========================
# Utility / Config helpers
# =========================

def now_utc_iso() -> str:
    """Return UTC timestamp in ISO format without microseconds."""
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def load_config(path: str = "config.yaml") -> dict:
    """
    Load YAML config, resolve ${ENV_VAR} placeholders dynamically.

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
    print("[INFO] Config loaded.")
    return cfg


# =========================
# Google Sheets helpers
# =========================

def get_sheets_service(cfg: dict):
    """
    Build an authenticated Google Sheets client using service account.
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
    Read Monitors tab, return rows where Active == 'YES'.
    Attach _row_number for each row.
    """
    sheet_id = cfg["google_sheets"]["sheet_id"]
    monitors_tab = cfg["google_sheets"]["monitors_tab"]

    print(f"[INFO] Reading monitors from tab '{monitors_tab}' ...")
    resp = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{monitors_tab}!A1:O1000"
    ).execute()

    rows = resp.get("values", [])
    if not rows or len(rows) < 2:
        print("[WARN] Monitors tab has no data rows.")
        return []

    headers = rows[0]
    data_rows = rows[1:]

    active_rows: List[Dict] = []
    for idx, row in enumerate(data_rows, start=2):
        row_dict = {}
        for i in range(len(headers)):
            row_dict[headers[i]] = row[i] if i < len(row) else ""
        row_dict["_row_number"] = idx  # actual sheet row index
        if row_dict.get("Active", "").strip().upper() == "YES":
            active_rows.append(row_dict)

    print(f"[INFO] Found {len(active_rows)} active monitor row(s).")
    return active_rows


def update_monitor_row(service, monitor_row: Dict, best_deal: Dict, cfg: dict):
    """
    Write back into Monitors tab for this row:
      M: LastBestPricePerPerson
      N: LastBestLink
      O: LastCheckedUTC
    """
    sheet_id = cfg["google_sheets"]["sheet_id"]
    monitors_tab = cfg["google_sheets"]["monitors_tab"]
    row_num = monitor_row["_row_number"]

    cell_range = f"{monitors_tab}!M{row_num}:O{row_num}"
    values = [[
        best_deal.get("price_per_person_usd", ""),
        best_deal.get("booking_url", ""),
        now_utc_iso()
    ]]

    print("[INFO] Updating Monitors row", row_num,
          "price=", best_deal.get("price_per_person_usd", ""),
          "link=", best_deal.get("booking_url", ""))
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=cell_range,
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


def append_alert_to_sheet(service, deal: Dict, monitor_row: Dict, cfg: dict):
    """
    Append an alert row to Alerts_Log tab.

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

    print("[ALERT] Logging alert in Alerts_Log for deal",
          deal.get("origin", ""), "->", deal.get("destination", ""),
          "price=", deal.get("price_per_person_usd", ""))

    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{alerts_tab}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


def append_runlog(service, cfg: dict, status: str, active_count: int, notes: str):
    """
    Append heartbeat row to Run_Log tab.

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

    print("[INFO] Appending Run_Log entry:",
          "status=", status,
          "active_count=", active_count,
          "notes=", notes)

    try:
        service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=f"{runlog_tab}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        print("[INFO] Run_Log append OK.")
    except Exception as e:
        print("[ERROR] Failed to append Run_Log:", e)


# =========================
# GPT / Flight search
# =========================

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
    Calls GPT-5 Thinking using the Responses API.
    Reads OPENAI_API_KEY from env first.
    """
    api_key = os.getenv("OPENAI_API_KEY") or cfg["openai"].get("api_key")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY and no fallback in config.")

    print("[INFO] Calling GPT-5 Thinking for flight search ...")
    client = OpenAI(api_key=api_key)

    completion = client.responses.create(
        model=cfg["openai"]["model"],
        input=[{"role": "user", "content": prompt}],
        extra_headers=cfg["openai"].get("extra_headers", {})
    )

    # Collect assistant text
    text_chunks = []
    for item in completion.output:
        if item.get("content"):
            for c in item["content"]:
                if c.get("type") == "output_text":
                    text_chunks.append(c["text"])

    full_text = "\n".join(text_chunks)
    print("[INFO] GPT-5 returned response.")
    print("[DEBUG] First 400 chars of GPT output:\n", full_text[:400])
    return full_text


def extract_deals_from_gpt(raw_text: str) -> List[Dict]:
    """
    Extract "deals": [...] from the GPT response.
    We'll regex the deals array, then json.loads it.
    """
    match = re.search(r'("deals"\s*:\s*\[.*?\])', raw_text, flags=re.DOTALL)
    if not match:
        print("[WARN] No 'deals' array found in GPT response.")
        return []

    json_snippet = "{ " + match.group(1) + " }"
    print("[DEBUG] Attempting to parse deals JSON snippet (truncated to 400 chars):")
    print(json_snippet[:400])

    try:
        parsed = json.loads(json_snippet)
        deals = parsed.get("deals", [])
        print(f"[INFO] Parsed {len(deals)} deal(s) from GPT output.")
        return deals
    except Exception as e:
        print("[ERROR] Failed to parse GPT JSON:", e)
        return []


# =========================
# Email alerts
# =========================

def send_email_alert(deal: Dict, monitor_row: Dict, cfg: dict):
    """
    Send an email alert when a deal meets threshold.
    """
    email_cfg = cfg["email"]

    subject = (
        "✈ Flight Deal: "
        + str(deal.get("origin", "")) + "→" + str(deal.get("destination", ""))
        + " $" + str(deal.get("price_per_person_usd", "")) + " pp"
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

    print("[ALERT] Sending email:", subject)
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = email_cfg["from"]
    msg["To"] = ", ".join(email_cfg["to"])

    with smtplib.SMTP(email_cfg["smtp_host"], email_cfg["smtp_port"]) as server:
        server.starttls()
        server.login(email_cfg["smtp_user"], email_cfg["smtp_pass"])
        server.sendmail(email_cfg["from"], email_cfg["to"], msg.as_string())

    print("[ALERT] Email sent OK.")


# =========================
# Per-monitor flow
# =========================

def handle_single_monitor(service, cfg: dict, mrow: Dict) -> str:
    """
    Process one active monitor row:
    - Build GPT prompt
    - Ask GPT for cheapest flight options
    - Parse deals
    - Update Monitors with best seen deal + timestamp
    - If good enough price, email + write to Alerts_Log
    Return small status string for Run_Log.
    """
    row_num = mrow["_row_number"]
    dest = mrow.get("Destination", "(OPEN)")
    orig = mrow.get("OriginAirports", "")
    print(f"[INFO] Processing monitor row {row_num}: {orig} -> {dest}")

    prompt = build_prompt_for_search(mrow, cfg)

    # 1. Call GPT
    try:
        raw_text = call_gpt_web(prompt, cfg)
    except Exception as e:
        print("[ERROR] GPT call failed for row", row_num, "err=", e)
        # still stamp timestamp in Monitors so we know we tried
        dummy_deal = {
            "price_per_person_usd": mrow.get("LastBestPricePerPerson", ""),
            "booking_url": mrow.get("LastBestLink", "")
        }
        update_monitor_row(service, mrow, dummy_deal, cfg)
        return "gpt_error"

    # 2. Parse deals
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
        print("[INFO] Row", row_num, ": no deals parsed.")
        dummy_deal = {
            "price_per_person_usd": mrow.get("LastBestPricePerPerson", ""),
            "booking_url": mrow.get("LastBestLink", "")
        }
        update_monitor_row(service, mrow, dummy_deal, cfg)
        return "no_deals_found"

    # 4. Sort by cheapest per-person price
    def price_val(d):
        try:
            return float(d.get("price_per_person_usd", "999999"))
        except Exception:
            return 999999.0

    deals.sort(key=price_val)
    best = deals[0]

    print("[INFO] Row", row_num, "cheapest:",
          best.get("origin", ""), "->", best.get("destination", ""),
          "price=", best.get("price_per_person_usd", ""))

    # 5. Stamp best deal + timestamp into Monitors
    update_monitor_row(service, mrow, best, cfg)

    # 6. See if we alert
    try:
        best_price = float(best.get("price_per_person_usd", "999999"))
    except Exception:
        best_price = 999999.0

    if best_price <= threshold:
        print("[INFO] Row", row_num, ": meets threshold.",
              "best_price=", best_price, "threshold=", threshold)
        send_email_alert(best, mrow, cfg)
        append_alert_to_sheet(service, best, mrow, cfg)
        return "alert_sent"

    print("[INFO] Row", row_num, ": above threshold.",
          "best_price=", best_price, "threshold=", threshold)
    return "checked_no_alert"


# =========================
# Main orchestration
# =========================

def monitor_once():
    """
    - Load config
    - Connect to Google Sheets
    - Read active monitors
    - Process each one
    - ALWAYS append to Run_Log so we can verify it ran
    """
    print("[INFO] ===== monitor start =====")

    cfg = load_config("config.yaml")
    service = get_sheets_service(cfg)

    monitors = read_monitors(service, cfg)

    if not monitors:
        print("[INFO] No active monitors in sheet.")
        append_runlog(
            service,
            cfg,
            status="no_active_monitors",
            active_count=0,
            notes="nothing_to_scan"
        )
        print("[INFO] ===== monitor end (no active monitors) =====")
        return

    statuses = []
    for mrow in monitors:
        st = handle_single_monitor(service, cfg, mrow)
        statuses.append(st)

    summary_notes = "; ".join(statuses)
    print("[INFO] Finished all active monitors. Summary:", summary_notes)

    append_runlog(
        service,
        cfg,
        status="completed",
        active_count=len(monitors),
        notes=summary_notes
    )

    print("[INFO] ===== monitor end (completed) =====")


if __name__ == "__main__":
    monitor_once()
