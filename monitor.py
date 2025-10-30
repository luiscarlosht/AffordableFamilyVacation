import os
import re
import yaml
import datetime
import json
import smtplib
from typing import List, Dict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from openai import OpenAI
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


# =========================
# Helpers
# =========================

def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def load_config() -> dict:
    print("[INFO] Loading config.yaml ...")
    with open("config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    print("[INFO] Config loaded.")
    return cfg


# =========================
# Google Sheets helpers
# =========================

def get_sheets_service(cfg: dict):
    print("[INFO] Initializing Google Sheets client ...")
    sa_path = cfg["google_sheets"]["service_account_json"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    service = build("sheets", "v4", credentials=creds)
    print("[INFO] Google Sheets client ready.")
    return service


def read_monitors(service, cfg: dict) -> List[Dict]:
    print(f"[INFO] Reading monitors from tab '{cfg['google_sheets']['monitors_tab']}' ...")
    sheet_id = cfg["google_sheets"]["sheet_id"]
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{cfg['google_sheets']['monitors_tab']}!A1:O1000"
    ).execute()

    values = result.get("values", [])
    if len(values) <= 1:
        print("[WARN] Monitors tab has no data rows.")
        return []

    headers = values[0]
    monitors = []
    for i, row in enumerate(values[1:], start=2):
        row_dict = {headers[j]: row[j] if j < len(row) else "" for j in range(len(headers))}
        row_dict["_row_num"] = i
        if row_dict.get("Active", "").strip().upper() == "YES":
            monitors.append(row_dict)

    print(f"[INFO] Found {len(monitors)} active monitor row(s).")
    return monitors


def append_run_log(service, cfg: dict, status: str, active_count: int, notes: str):
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


def update_monitor_row(service, cfg: dict, row_num: int, price: str, link: str):
    sheet_id = cfg["google_sheets"]["sheet_id"]
    tab = cfg["google_sheets"]["monitors_tab"]
    ts = now_utc_iso()

    print(f"[INFO] Updating Monitors row {row_num} price={price} link={link}")
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{tab}!M{row_num}:O{row_num}",
        valueInputOption="USER_ENTERED",
        body={"values": [[price, link, ts]]},
    ).execute()


def append_alert_to_sheet(service, cfg: dict, deal: Dict, monitor_row: Dict):
    """
    Optional: you can call this in future if you want to log every alert-worthy deal
    into Alerts_Log. Right now we only send email, but we can wire this at any time.
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
        deal.get("total_usd", ""),
        deal.get("booking_link", ""),
        monitor_row.get("Notes", ""),
    ]]

    print("[ALERT] Logging alert row in Alerts_Log for",
          deal.get("origin", ""), "->", deal.get("destination", ""),
          "price=", deal.get("price_per_person_usd", ""))

    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{alerts_tab}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


# =========================
# OpenAI model call
# =========================

def call_gpt_web(prompt: str, cfg: dict) -> str:
    """
    Call the OpenAI Responses API using the model from config.
    Return raw text output from the assistant.
    This version is defensive around SDK return shapes.
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
                    "- You MUST browse current public airfare prices.\n"
                    "- You MUST return ONLY JSON.\n"
                    "- Format:\n"
                    "{\n"
                    "  \"deals\": [\n"
                    "    {\n"
                    "      \"origin\": \"DFW\",\n"
                    "      \"destination\": \"PHX\",\n"
                    "      \"depart\": \"2025-12-19\",\n"
                    "      \"return\": \"2025-12-27\",\n"
                    "      \"airline\": \"SomeAir\",\n"
                    "      \"price_per_person_usd\": 123,\n"
                    "      \"total_usd\": 369,\n"
                    "      \"seats_available\": 3,\n"
                    "      \"booking_link\": \"https://...\"\n"
                    "    }\n"
                    "  ]\n"
                    "}\n"
                    "- Do not include explanation or commentary."
                ),
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        extra_headers=cfg["openai"].get("extra_headers", {}),
    )

    # ---- Robust extraction of assistant text ----
    # Try new-style convenience: completion.output_text
    if hasattr(completion, "output_text") and completion.output_text:
        raw_text = completion.output_text
        print("[DEBUG] Used completion.output_text")
    else:
        # Fallback: iterate completion.output which may contain messages
        collected_chunks = []
        if hasattr(completion, "output"):
            for item in completion.output:
                # some SDKs return objects with attributes, not dicts
                if hasattr(item, "content"):
                    # item.content can be list[dict-like] or list[objects]
                    for c in item.content:
                        # Each c may look like {"type": "output_text", "text": "..."}
                        # or an object with .type/.text
                        c_type = getattr(c, "type", None)
                        c_text = getattr(c, "text", None)

                        if isinstance(c, dict):
                            c_type = c.get("type", c_type)
                            c_text = c.get("text", c_text)

                        if c_type == "output_text" and c_text:
                            collected_chunks.append(c_text)

        raw_text = "\n".join(collected_chunks)
        print("[DEBUG] Used manual chunk extraction")

    print("[INFO] Model response received.")
    print("[DEBUG] First 400 chars of model output:\n", raw_text[:400])
    return raw_text


# =========================
# Parsing deals JSON
# =========================

def extract_deals_from_gpt(raw_text: str) -> List[Dict]:
    """
    Extract a deals array from the model output, which should be pure JSON.
    We'll:
    1. find the substring that starts with {"deals":
    2. try json.loads on it
    """
    # Find a JSON-looking block starting with {"deals"
    match = re.search(r'(\{[^{]*"deals"\s*:\s*\[.*?\}\s*\])', raw_text, flags=re.DOTALL)
    # The regex above is aggressive. We'll also try a simpler backup.
    if not match:
        match = re.search(r'(\{"deals"\s*:\s*\[.*?\}\s*)', raw_text, flags=re.DOTALL)

    if not match:
        print("[WARN] No deals JSON block matched.")
        return []

    candidate = match.group(1)
    # Try to close it if model forgot trailing brace '}'.
    if not candidate.strip().endswith("}"):
        candidate = candidate.strip() + "}"

    try:
        data = json.loads(candidate)
        deals = data.get("deals", [])
        print(f"[INFO] Parsed {len(deals)} deal(s) from model output.")
        return deals
    except Exception as e:
        print("[ERROR] JSON parse failed:", e)
        print("[DEBUG] candidate was:\n", candidate[:500])
        return []


# =========================
# Email alert
# =========================

def send_email_alert(cfg: dict, deal: Dict, seats_requested: str):
    email_cfg = cfg["email"]

    subject = (
        f"Flight Alert: {deal.get('origin')} → {deal.get('destination')} "
        f"${deal.get('price_per_person_usd')} per person"
    )

    body_html = f"""
    <h3>Flight Deal Found</h3>
    <p><b>Route:</b> {deal.get('origin')} → {deal.get('destination')}<br>
    <b>Depart:</b> {deal.get('depart')}<br>
    <b>Return:</b> {deal.get('return')}<br>
    <b>Airline:</b> {deal.get('airline')}<br>
    <b>Seats requested:</b> {seats_requested}<br>
    <b>Seats available (model est):</b> {deal.get('seats_available')}<br>
    <b>Price/person:</b> ${deal.get('price_per_person_usd')}<br>
    <b>Total est:</b> ${deal.get('total_usd')}<br>
    <b>Booking link:</b> <a href="{deal.get('booking_link')}">{deal.get('booking_link')}</a></p>
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
# Per-row monitor logic
# =========================

def handle_row(service, cfg: dict, row: Dict) -> str:
    rownum = row["_row_num"]
    origin = row.get("OriginAirports", "")
    dest = row.get("Destination", "") or "anywhere"
    seats = row.get("Seats", "1")
    depart_start = row.get("DepartStart", "")
    depart_end = row.get("DepartEnd", "")
    return_start = row.get("ReturnStart", "")
    return_end = row.get("ReturnEnd", "")
    max_pp = row.get("MaxPricePerPersonUSD", "")
    max_pp_val = float(max_pp) if max_pp else 9999.0

    print(f"[INFO] Processing monitor row {rownum}: {origin} -> {dest}")

    # Build prompt we send to GPT model
    prompt = (
        f"Find round-trip economy flights for {seats} travelers "
        f"from these origin airports: {origin}. "
        f"Destination: {dest}. "
        f"Depart between {depart_start} and {depart_end}. "
        f"Return between {return_start} and {return_end}. "
        f"Prices in USD. Output deals[]. "
        f'Threshold per person: ${max_pp_val}.'
    )

    try:
        raw = call_gpt_web(prompt, cfg)
    except Exception as e:
        print(f"[ERROR] OpenAI request failed for row {rownum}: {e}")
        update_monitor_row(service, cfg, rownum, "", "")
        return "gpt_error"

    deals = extract_deals_from_gpt(raw)

    if not deals:
        print(f"[INFO] Row {rownum}: no deals parsed.")
        update_monitor_row(service, cfg, rownum, "", "")
        return "no_deals_found"

    # Sort by price_per_person_usd
    def price_val(d):
        try:
            return float(d.get("price_per_person_usd", 999999))
        except Exception:
            return 999999.0

    deals.sort(key=price_val)
    best = deals[0]

    best_price = price_val(best)
    best_link = best.get("booking_link", "")

    print(f"[INFO] Row {rownum}: best {best.get('origin')} -> {best.get('destination')} @ {best_price} per person")

    # Update sheet with best found
    update_monitor_row(service, cfg, rownum, str(best_price), best_link)

    # If it's under your cap -> alert
    if best_price <= max_pp_val:
        print(f"[INFO] Row {rownum}: meets threshold ({best_price} <= {max_pp_val}), sending alert.")
        send_email_alert(cfg, best, seats_requested=seats)
        # If you want to log alerts to Alerts_Log sheet, uncomment:
        # append_alert_to_sheet(service, cfg, best, row)
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
