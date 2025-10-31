import os
import yaml
import json
import re
import smtplib
import datetime
from typing import List, Dict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from openai import OpenAI
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


# =========================
# Time helper
# =========================

def now_utc_iso() -> str:
    """Return UTC timestamp in ISO without microseconds."""
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


# =========================
# Config loader
# =========================

def load_config() -> dict:
    """
    Load config.yaml.
    """
    print("[INFO] Loading config.yaml ...")
    with open("config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    print("[INFO] Config loaded.")
    return cfg


# =========================
# Google Sheets helpers
# =========================

def get_sheets_service(cfg: dict):
    """
    Auth to Google Sheets via service account.
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
    Return active monitor rows (Active == YES), each with _row_num.
    """
    print(f"[INFO] Reading monitors from tab '{cfg['google_sheets']['monitors_tab']}' ...")
    sheet_id = cfg["google_sheets"]["sheet_id"]
    rng = f"{cfg['google_sheets']['monitors_tab']}!A1:O1000"
    resp = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=rng,
    ).execute()

    rows = resp.get("values", [])
    if len(rows) <= 1:
        print("[WARN] Monitors tab has no data rows.")
        return []

    headers = rows[0]
    out = []
    for idx, row in enumerate(rows[1:], start=2):
        row_map = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        row_map["_row_num"] = idx
        if row_map.get("Active", "").strip().upper() == "YES":
            out.append(row_map)

    print(f"[INFO] Found {len(out)} active monitor row(s).")
    return out


def update_monitor_row(service, cfg: dict, row_num: int, last_price: str, last_source: str):
    """
    Write back into Monitors row:
      Col M: LastBestPricePerPerson
      Col N: LastBestSource
      Col O: LastCheckedUTC
    """
    sheet_id = cfg["google_sheets"]["sheet_id"]
    tab = cfg["google_sheets"]["monitors_tab"]
    ts = now_utc_iso()

    print(f"[INFO] Updating Monitors row {row_num} price={last_price} source_site={last_source}")
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{tab}!M{row_num}:O{row_num}",
        valueInputOption="USER_ENTERED",
        body={"values": [[last_price, last_source, ts]]},
    ).execute()


def append_run_log(service, cfg: dict, status: str, active_count: int, notes: str):
    """
    Append heartbeat row to Run_Log:
      TimestampUTC | Status | ActiveMonitorCount | Notes
    """
    run_tab = cfg["google_sheets"]["runlog_tab"]
    sheet_id = cfg["google_sheets"]["sheet_id"]
    ts = now_utc_iso()
    row = [ts, status, str(active_count), notes]

    print(f"[INFO] Appending Run_Log entry: status={status} active_count={active_count} notes={notes}")
    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{run_tab}!A:D",
        valueInputOption="USER_ENTERED",
        body={"values": [row]},
    ).execute()
    print("[INFO] Run_Log append OK.")


def append_alert_to_sheet(service, cfg: dict, deal: Dict, monitor_row: Dict):
    """
    Log a qualifying alert deal into Alerts_Log.

    Alerts_Log headers must be EXACTLY:
    A: TimestampUTC
    B: OriginUsed
    C: DestinationFound
    D: DepartDate
    E: ReturnDate
    F: Seats
    G: PricePerPersonUSD
    H: TotalTripUSD
    I: SourceSite
    J: QueryHint
    K: Airline
    L: SeatsAvailable
    M: MonitorNotes
    """
    sheet_id = cfg["google_sheets"]["sheet_id"]
    tab = cfg["google_sheets"]["alerts_tab"]

    values = [[
        now_utc_iso(),
        deal.get("origin", ""),
        deal.get("destination", ""),
        deal.get("depart", ""),
        deal.get("return", ""),
        monitor_row.get("Seats", ""),
        deal.get("price_per_person_usd", ""),
        deal.get("total_usd", ""),
        deal.get("source_site", ""),
        deal.get("query_hint", ""),
        deal.get("airline", ""),
        deal.get("seats_available", ""),
        monitor_row.get("Notes", ""),
    ]]

    print(
        "[ALERT] Logging alert row in Alerts_Log:",
        deal.get("origin", ""), "->", deal.get("destination", ""),
        "price=", deal.get("price_per_person_usd", ""),
        "source=", deal.get("source_site", "")
    )

    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


# =========================
# OpenAI call + parsing
# =========================

def call_gpt_web(prompt: str, cfg: dict) -> str:
    """
    Call the OpenAI model specified in config.yaml.
    We ask it to browse, and REQUIRE source_site and query_hint.
    """
    api_key = os.getenv("OPENAI_API_KEY") or cfg["openai"].get("api_key")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY and no fallback in config.")

    print("[INFO] Calling OpenAI model for flight search ...")
    client = OpenAI(api_key=api_key)

    completion = client.responses.create(
        model=cfg["openai"]["model"],
        input=[
            {
                "role": "system",
                "content": (
                    "You are a live flight deal agent.\n"
                    "- You MUST browse public airfare on airline sites, Google Flights, Kayak, Expedia, etc.\n"
                    "- Only return JSON.\n"
                    "- Format:\n"
                    "{\n"
                    "  \"deals\": [\n"
                    "    {\n"
                    "      \"origin\": \"DFW\",\n"
                    "      \"destination\": \"PHX\",\n"
                    "      \"depart\": \"2025-12-20\",\n"
                    "      \"return\": \"2025-12-26\",\n"
                    "      \"airline\": \"American Airlines\",\n"
                    "      \"price_per_person_usd\": 145,\n"
                    "      \"total_usd\": 435,\n"
                    "      \"seats_available\": 3,\n"
                    "      \"source_site\": \"Google Flights\",\n"
                    "      \"query_hint\": \"DFW to PHX Dec 20–26 3 travelers American Airlines\",\n"
                    "      \"booking_link\": \"https://www.google.com/flights?...\"\n"
                    "    }\n"
                    "  ]\n"
                    "}\n"
                    "- 'source_site' MUST say where you saw the fare (Google Flights, Expedia, southwest.com, etc.).\n"
                    "- 'query_hint' MUST be something I can paste into that site to re-find the same fare.\n"
                    "- If no real booking link exists, set booking_link to \"\".\n"
                    "- No commentary outside JSON."
                ),
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        extra_headers=cfg["openai"].get("extra_headers", {}),
    )

    # Try simple property first
    if hasattr(completion, "output_text") and completion.output_text:
        raw_text = completion.output_text
        print("[DEBUG] Used completion.output_text")
    else:
        # Fallback for older SDK structures
        chunks = []
        if hasattr(completion, "output"):
            for item in completion.output:
                if hasattr(item, "content"):
                    for c in item.content:
                        c_type = getattr(c, "type", None)
                        c_text = getattr(c, "text", None)
                        if isinstance(c, dict):
                            c_type = c.get("type", c_type)
                            c_text = c.get("text", c_text)
                        if c_type == "output_text" and c_text:
                            chunks.append(c_text)
        raw_text = "\n".join(chunks)
        print("[DEBUG] Used manual chunk extraction")

    print("[INFO] Model response received.")
    print("[DEBUG] First 400 chars of model output:\n", raw_text[:400])
    return raw_text


def safe_extract_deals_json(raw_text: str) -> str:
    """
    Attempt to extract the full top-level JSON object by balancing braces.
    We assume the response starts with '{' and is valid JSON until the last '}'.
    If we can't find a balanced object, return "".
    """
    # Find first '{'
    start = raw_text.find("{")
    if start == -1:
        return ""

    depth = 0
    for i in range(start, len(raw_text)):
        ch = raw_text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                # slice inclusive of this brace
                return raw_text[start:i+1]

    # If we never got back to depth 0, it's incomplete
    return ""


def fallback_single_deal(raw_text: str) -> List[Dict]:
    """
    Fallback parser:
    Try to pull ONLY the first deal inside "deals": [ ... ] if full JSON fails.
    We'll use a best-effort regex to grab the first { ... } inside deals.
    """
    m = re.search(
        r'"deals"\s*:\s*\[\s*(\{.*?\})',
        raw_text,
        flags=re.DOTALL
    )
    if not m:
        return []

    first_obj_txt = m.group(1)
    try:
        deal_obj = json.loads(first_obj_txt)
        return [deal_obj]
    except Exception as e:
        print("[ERROR] fallback_single_deal JSON parse failed:", e)
        return []


def extract_deals_from_gpt(raw_text: str) -> List[Dict]:
    """
    1. Try to grab a fully balanced top-level JSON object from the model output.
    2. json.loads it and return ["deals"].
    3. If that fails, fallback to extracting just the first deal object.
    """
    balanced_json = safe_extract_deals_json(raw_text)

    if balanced_json:
        try:
            data = json.loads(balanced_json)
            deals = data.get("deals", [])
            print(f"[INFO] Parsed {len(deals)} deal(s) from balanced JSON.")
            return deals
        except Exception as e:
            print("[ERROR] JSON parse failed on balanced_json:", e)
            print("[DEBUG] balanced_json starts with:\n", balanced_json[:500])

    # Fallback
    print("[WARN] Falling back to single-deal extraction.")
    deals = fallback_single_deal(raw_text)
    if deals:
        print(f"[INFO] Fallback extracted {len(deals)} deal(s).")
    else:
        print("[WARN] Fallback also found 0 deals.")
    return deals


# =========================
# Email alert
# =========================

def send_email_alert(cfg: dict, deal: Dict, seats_requested: str):
    """
    Send a rich HTML email that includes source_site and query_hint
    so you can go reproduce the deal yourself.
    """
    email_cfg = cfg["email"]

    subject = (
        f"Flight Alert: {deal.get('origin')} → {deal.get('destination')} "
        f"${deal.get('price_per_person_usd')} per person"
    )

    body_html = f"""
    <h3>Flight Deal Found</h3>
    <p>
    <b>Route:</b> {deal.get('origin')} → {deal.get('destination')}<br>
    <b>Depart:</b> {deal.get('depart')}<br>
    <b>Return:</b> {deal.get('return')}<br>
    <b>Airline:</b> {deal.get('airline')}<br>
    <b>Seats requested:</b> {seats_requested}<br>
    <b>Seats available (model est):</b> {deal.get('seats_available')}<br>
    <b>Price/person:</b> ${deal.get('price_per_person_usd')}<br>
    <b>Total est:</b> ${deal.get('total_usd')}<br>
    <b>Source site:</b> {deal.get('source_site')}<br>
    <b>Search yourself using this phrase:</b><br>
    <code>{deal.get('query_hint')}</code><br>
    <b>Booking link (may be placeholder):</b><br>
    <a href="{deal.get('booking_link', '')}">{deal.get('booking_link', '')}</a>
    </p>
    <p>Time (UTC): {now_utc_iso()}</p>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_cfg["from"]
    msg["To"] = ", ".join(email_cfg["to"])
    msg.attach(MIMEText(body_html, "html"))

    with smtplib.SMTP(email_cfg["smtp_host"], email_cfg["smtp_port"]) as server:
        server.starttls()
        server.login(email_cfg["smtp_user"], email_cfg["smtp_pass"])
        server.send_message(msg)

    print("[ALERT] Email sent OK.")


# =========================
# Core per-row logic
# =========================

def handle_row(service, cfg: dict, row: Dict) -> str:
    """
    Process one monitor row:
    - Ask the model for flight deals
    - Parse deals
    - Update Monitors sheet with price + source
    - If under threshold => email + Alerts_Log
    - Return a status string for Run_Log
    """
    rownum = row["_row_num"]
    origin = row.get("OriginAirports", "")
    dest = row.get("Destination", "") or "anywhere"
    seats = row.get("Seats", "1")
    depart_start = row.get("DepartStart", "")
    depart_end = row.get("DepartEnd", "")
    return_start = row.get("ReturnStart", "")
    return_end = row.get("ReturnEnd", "")
    max_pp_raw = row.get("MaxPricePerPersonUSD", "")
    try:
        max_pp_val = float(max_pp_raw) if max_pp_raw else 9999.0
    except:
        max_pp_val = 9999.0

    print(f"[INFO] Processing monitor row {rownum}: {origin} -> {dest}")

    # User prompt that we feed the model
    prompt = (
        f"Find round-trip economy flights for {seats} travelers "
        f"from these origin airports: {origin}. "
        f"Destination: {dest}. "
        f"Depart between {depart_start} and {depart_end}. "
        f"Return between {return_start} and {return_end}. "
        f"All prices in USD. "
        f"Only include deals with at least {seats} seats if possible. "
        f"Remember: every deal needs source_site and query_hint."
    )

    # 1. Query OpenAI
    try:
        raw = call_gpt_web(prompt, cfg)
    except Exception as e:
        print(f"[ERROR] OpenAI request failed for row {rownum}: {e}")
        update_monitor_row(service, cfg, rownum, "", "gpt_call_failed")
        return "gpt_error"

    # 2. Parse deals with robust extractor
    deals = extract_deals_from_gpt(raw)
    if not deals:
        print(f"[INFO] Row {rownum}: no deals parsed.")
        update_monitor_row(service, cfg, rownum, "", "no_deals_found")
        return "no_deals_found"

    # 3. choose cheapest by price_per_person_usd
    def price_val(d):
        try:
            return float(d.get("price_per_person_usd", 999999))
        except:
            return 999999.0

    deals.sort(key=price_val)
    best = deals[0]

    best_price = price_val(best)
    best_source = best.get("source_site", "")
    print(f"[INFO] Row {rownum}: best {best.get('origin')} -> {best.get('destination')} @ {best_price} per person from {best_source}")

    # 4. Update Monitors sheet with best observed deal info
    update_monitor_row(service, cfg, rownum, str(best_price), best_source)

    # 5. Price threshold logic
    if best_price <= max_pp_val:
        print(f"[INFO] Row {rownum}: meets threshold ({best_price} <= {max_pp_val}), alerting.")
        send_email_alert(cfg, best, seats_requested=seats)
        append_alert_to_sheet(service, cfg, best, row)
        return "alert_sent"

    print(f"[INFO] Row {rownum}: above threshold ({best_price} > {max_pp_val}). No alert.")
    return "checked_no_alert"


# =========================
# Main orchestration
# =========================

def monitor_once():
    print("[INFO] ===== monitor start =====")
    cfg = load_config()
    service = get_sheets_service(cfg)
    monitors = read_monitors(service, cfg)

    if not monitors:
        print("[INFO] No active monitors in sheet.")
        append_run_log(service, cfg, "no_active_monitors", 0, "nothing_to_scan")
        print("[INFO] ===== monitor end (no active monitors) =====")
        return

    statuses = []
    for row in monitors:
        status = handle_row(service, cfg, row)
        statuses.append(status)

    notes = "; ".join(statuses)
    print("[INFO] Finished all active monitors. Summary:", notes)
    append_run_log(service, cfg, "completed", len(monitors), notes)
    print("[INFO] ===== monitor end (completed) =====")


if __name__ == "__main__":
    monitor_once()
