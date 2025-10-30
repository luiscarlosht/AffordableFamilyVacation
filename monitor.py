import os
import yaml
import datetime
import smtplib
from email.mime.text import MIMEText
from typing import List, Dict
import json
import re

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from openai import OpenAI


def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def get_sheets_service(cfg: dict):
    """
    Builds Google Sheets service using service account JSON file
    from config.yaml.
    """
    sa_path = cfg["google_sheets"]["service_account_json"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    service = build("sheets", "v4", credentials=creds)
    return service


def read_monitors(service, cfg: dict) -> List[Dict]:
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
        row_dict["_row_number"] = idx
        if row_dict.get("Active", "").strip().upper() == "YES":
            active_rows.append(row_dict)

    return active_rows


def build_prompt_for_search(monitor_row: Dict, cfg: dict) -> str:
    scan_defaults = cfg["scan_defaults"]

    origin_home = monitor_row.get("OriginHome", "").strip() or scan_defaults["home_fallback"]
    origin_airports = monitor_row.get("OriginAirports", "").strip()
    if not origin_airports:
        # fallback to config defaults
        origin_airports = ",".join(scan_defaults["default_origin_airports"])

    destination = monitor_row.get("Destination", "").strip()
    depart_start = monitor_row.get("DepartStart", "")
    depart_end = monitor_row.get("DepartEnd", "")
    return_start = monitor_row.get("ReturnStart", "")
    return_end = monitor_row.get("ReturnEnd", "")

    seats = monitor_row.get("Seats", "").strip() or str(scan_defaults["default_seats"])

    max_price = monitor_row.get("MaxPricePerPersonUSD", "").strip()
    if not max_price:
        max_price = str(scan_defaults["default_threshold"])

    open_destination = (destination == "")

    prompt = f"""
You are a flight deal scout. Search live flight prices on major public travel sites.

Goal:
Find the CHEAPEST flight options that satisfy:
- Origin airports: {origin_airports} (airports ~100 miles from {origin_home} are allowed).
- Destination: {"OPEN / ANYWHERE cheap" if open_destination else destination}.
- Departure date between {depart_start} and {depart_end}.
- Return date between {return_start} and {return_end} (if blank, treat as one-way).
- Number of travelers: {seats} seats.
- We are cost-driven. Connections/layovers ARE OK.
- Alert if price per person is <= ${max_price} USD.

For each top 3 cheapest valid options, give:
1. Origin airport code
2. Destination airport code / city
3. Departure date and time
4. Return date and time (if round-trip)
5. Airline(s)
6. Total price per PERSON in USD
7. Total price for ALL travelers in USD
8. Number of seats available at that price (if shown)
9. Direct booking link or closest publicly bookable link

Return a valid JSON object with a 'deals' array (max length 3),
each entry using keys:
["origin","destination","depart","return","airline","price_per_person_usd","total_for_group_usd","seats_available","booking_url"]

After that JSON, also include one short human note telling me if any option is <= ${max_price} per person.
    """.strip()

    return prompt


def call_gpt_web(prompt: str, cfg: dict) -> str:
    """
    Calls GPT-5 Thinking with browsing.
    """
    client = OpenAI(api_key=cfg["openai"]["api_key"])

    completion = client.responses.create(
        model=cfg["openai"]["model"],
        input=[{
            "role": "user",
            "content": prompt
        }],
        extra_headers=cfg["openai"].get("extra_headers", {})
    )

    # Extract assistant text chunks
    text_chunks = []
    for item in completion.output:
        if item.get("content"):
            for c in item["content"]:
                if c["type"] == "output_text":
                    text_chunks.append(c["text"])

    return "\n".join(text_chunks)


def extract_deals_from_gpt(raw_text: str) -> list[Dict]:
    """
    Pull the "deals" array from GPT output.
    """
    m = re.search(r'("deals"\s*:\s*\[.*?\])', raw_text, flags=re.DOTALL)
    if not m:
        return []

    json_snippet = "{ " + m.group(1) + " }"
    try:
        data = json.loads(json_snippet)
        return data.get("deals", [])
    except Exception:
        return []


def send_email_alert(deal: Dict, monitor_row: Dict, cfg: dict):
    """
    Uses SMTP creds from YAML.
    """
    email_cfg = cfg["email"]

    subject = f"✈ Flight Deal Alert: {deal.get('origin')} → {deal.get('destination')} at ${deal.get('price_per_person_usd')} per person"
    body = f"""
Deal found!

Route: {deal.get('origin')} → {deal.get('destination')}
Depart: {deal.get('depart')}
Return: {deal.get('return')}
Airline: {deal.get('airline')}
Seats Requested: {monitor_row.get('Seats')}
Seats Available (reported): {deal.get('seats_available')}

Price per person: ${deal.get('price_per_person_usd')}
Total for group: ${deal.get('total_for_group_usd')}

Booking link:
{deal.get('booking_url')}

Monitor Notes: {monitor_row.get('Notes')}

Timestamp (UTC): {now_utc_iso()}
    """.strip()

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = email_cfg["from"]
    msg["To"] = ", ".join(email_cfg["to"])

    with smtplib.SMTP(email_cfg["smtp_host"], email_cfg["smtp_port"]) as server:
        server.starttls()
        server.login(email_cfg["smtp_user"], email_cfg["smtp_pass"])
        server.sendmail(email_cfg["from"], email_cfg["to"], msg.as_string())


def append_alert_to_sheet(service, deal: Dict, monitor_row: Dict, cfg: dict):
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

    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{alerts_tab}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


def update_monitor_row(service, monitor_row: Dict, best_deal: Dict, cfg: dict):
    """
    Writes LastBestPricePerPerson, LastBestLink, LastCheckedUTC
    back to the Monitors tab, using the row number we tracked.
    """
    sheet_id = cfg["google_sheets"]["sheet_id"]
    monitors_tab = cfg["google_sheets"]["monitors_tab"]

    sheet_row_number = monitor_row["_row_number"]

    # Columns:
    # M = LastBestPricePerPerson
    # N = LastBestLink
    # O = LastCheckedUTC
    cell_range = f"{monitors_tab}!M{sheet_row_number}:O{sheet_row_number}"

    values = [[
        best_deal.get("price_per_person_usd", ""),
        best_deal.get("booking_url", ""),
        now_utc_iso()
    ]]

    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=cell_range,
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


def monitor_once():
    cfg = load_config("config.yaml")

    service = get_sheets_service(cfg)
    monitors = read_monitors(service, cfg)

    if not monitors:
        print("No active monitors.")
        return

    for mrow in monitors:
        prompt = build_prompt_for_search(mrow, cfg)
        raw = call_gpt_web(prompt, cfg)
        deals = extract_deals_from_gpt(raw)

        # default threshold
        try:
            threshold = float(mrow.get("MaxPricePerPersonUSD", "").strip() or cfg["scan_defaults"]["default_threshold"])
        except:
            threshold = float(cfg["scan_defaults"]["default_threshold"])

        # no deals? just mark timestamp
        if not deals:
            dummy_deal = {
                "price_per_person_usd": mrow.get("LastBestPricePerPerson", ""),
                "booking_url": mrow.get("LastBestLink", "")
            }
            update_monitor_row(service, mrow, dummy_deal, cfg)
            continue

        # Pick cheapest
        def price_val(d):
            try:
                return float(d.get("price_per_person_usd", "999999"))
            except:
                return 999999.0

        deals_sorted = sorted(deals, key=price_val)
        best_deal = deals_sorted[0]

        # Update sheet
        update_monitor_row(service, mrow, best_deal, cfg)

        # Send alert ONLY if cheaper than threshold
        try:
            best_price_float = float(best_deal.get("price_per_person_usd", "999999"))
        except:
            best_price_float = 999999.0

        if best_price_float <= threshold:
            send_email_alert(best_deal, mrow, cfg)
            append_alert_to_sheet(service, best_deal, mrow, cfg)


if __name__ == "__main__":
    monitor_once()
