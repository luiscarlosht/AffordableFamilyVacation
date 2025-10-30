import os
import re
import yaml
import datetime
from openai import OpenAI
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ========== CONFIG LOADER ==========

def load_config():
    print("[INFO] Loading config.yaml ...")
    with open("config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    print("[INFO] Config loaded.")
    return cfg

# ========== GOOGLE SHEETS CLIENT ==========

def get_sheets_service(cfg):
    print("[INFO] Initializing Google Sheets client ...")
    sa_path = cfg["google_sheets"]["service_account_json"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    service = build("sheets", "v4", credentials=creds)
    print("[INFO] Google Sheets client ready.")
    return service

# ========== SHEET OPERATIONS ==========

def read_monitors(service, cfg):
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

def append_run_log(service, cfg, status, active_count, notes):
    run_tab = cfg["google_sheets"]["runlog_tab"]
    sheet_id = cfg["google_sheets"]["sheet_id"]
    timestamp = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    row = [timestamp, status, str(active_count), notes]
    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{run_tab}!A:D",
        valueInputOption="USER_ENTERED",
        body={"values": [row]}
    ).execute()
    print(f"[INFO] Appending Run_Log entry: status={status} active_count={active_count} notes={notes}")
    print("[INFO] Run_Log append OK.")

def update_monitor_row(service, cfg, row_num, price, link):
    sheet_id = cfg["google_sheets"]["sheet_id"]
    tab = cfg["google_sheets"]["monitors_tab"]
    timestamp = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{tab}!M{row_num}:O{row_num}",
        valueInputOption="USER_ENTERED",
        body={"values": [[price, link, timestamp]]}
    ).execute()
    print(f"[INFO] Updating Monitors row {row_num} price={price} link={link}")

# ========== OPENAI CALL ==========

def call_gpt_web(prompt: str, cfg: dict) -> str:
    """
    Calls OpenAI model using the Responses API and returns raw text.
    Uses gpt-4o-mini for cheaper cost.
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
                    "You are a flight deal scraper. "
                    "You MUST browse live airfare from public travel/airline websites. "
                    "You MUST respond ONLY with JSON in the format: "
                    "{\"deals\": [{\"origin\":..., \"destination\":..., \"depart\":..., \"return\":..., "
                    "\"price_per_person_usd\":..., \"total_usd\":..., \"booking_link\":...}]}"
                ),
            },
            {"role": "user", "content": prompt},
        ],
        extra_headers=cfg["openai"].get("extra_headers", {}),
    )

    text_chunks = []
    for item in completion.output:
        if item.get("content"):
            for c in item["content"]:
                if c.get("type") == "output_text":
                    text_chunks.append(c["text"])

    full_text = "\n".join(text_chunks)
    print("[INFO] Model response received.")
    print("[DEBUG] First 300 chars of model output:\n", full_text[:300])
    return full_text

# ========== PARSER ==========

def extract_deals_from_gpt(raw_text):
    match = re.search(r'(\{"deals".*?\])', raw_text, flags=re.DOTALL)
    if not match:
        print("[WARN] No deals JSON found in GPT output.")
        return []
    json_text = match.group(1)
    try:
        import json
        data = json.loads(json_text + "}")
        return data.get("deals", [])
    except Exception as e:
        print("[ERROR] JSON parse failed:", e)
        return []

# ========== EMAIL ALERTS ==========

def send_email_alert(cfg, deal):
    email_cfg = cfg["email"]
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Flight Alert: {deal['origin']} → {deal['destination']} ${deal['price_per_person_usd']}/person"
    msg["From"] = email_cfg["from"]
    msg["To"] = ", ".join(email_cfg["to"])

    html = f"""
    <h3>Flight Deal Found</h3>
    <p><b>Origin:</b> {deal['origin']}<br>
    <b>Destination:</b> {deal['destination']}<br>
    <b>Depart:</b> {deal['depart']}<br>
    <b>Return:</b> {deal['return']}<br>
    <b>Seats:</b> {deal.get('seats', '?')}<br>
    <b>Price per person:</b> ${deal['price_per_person_usd']}<br>
    <b>Total:</b> ${deal['total_usd']}<br>
    <b>Link:</b> <a href="{deal['booking_link']}">Book Here</a></p>
    """
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(email_cfg["smtp_host"], email_cfg["smtp_port"]) as server:
        server.starttls()
        server.login(email_cfg["smtp_user"], email_cfg["smtp_pass"])
        server.send_message(msg)

    print(f"[ALERT] Sent email alert for {deal['origin']} → {deal['destination']}")

# ========== MAIN MONITOR ==========

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

    notes_summary = []
    for row in monitors:
        origin = row.get("OriginAirports", "")
        dest = row.get("Destination", "")
        rownum = row["_row_num"]
        print(f"[INFO] Processing monitor row {rownum}: {origin} -> {dest or '(open destination)'}")

        prompt = (
            f"Find round-trip flights for {row.get('Seats','1')} travelers "
            f"from {origin} to {dest or 'anywhere'} departing between "
            f"{row.get('DepartStart')} and {row.get('DepartEnd')} "
            f"and returning between {row.get('ReturnStart')} and {row.get('ReturnEnd')}. "
            f"Only include economy prices in USD."
        )

        try:
            raw = call_gpt_web(prompt, cfg)
            deals = extract_deals_from_gpt(raw)
            if not deals:
                update_monitor_row(service, cfg, rownum, "", "")
                notes_summary.append("no_deals_found")
                continue

            best = sorted(deals, key=lambda d: d["price_per_person_usd"])[0]
            update_monitor_row(service, cfg, rownum, best["price_per_person_usd"], best["booking_link"])

            if best["price_per_person_usd"] <= float(row.get("MaxPricePerPersonUSD") or 9999):
                send_email_alert(cfg, best)
                notes_summary.append("alert_sent")
            else:
                notes_summary.append("checked_no_alert")

        except Exception as e:
            print(f"[ERROR] GPT call failed for row {rownum} err=", e)
            update_monitor_row(service, cfg, rownum, "", "")
            notes_summary.append("gpt_error")

    summary_str = "; ".join(notes_summary)
    print(f"[INFO] Finished all active monitors. Summary: {summary_str}")
    append_run_log(service, cfg, "completed", len(monitors), summary_str)
    print("[INFO] ===== monitor end (completed) =====")

# ========== ENTRY POINT ==========

if __name__ == "__main__":
    monitor_once()
