import os
import re
import yaml
import datetime
import smtplib
import json
import re
from typing import List, Dict
from email.mime.text import MIMEText

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from openai import OpenAI


# -----------------------------------------------------------------------------
# CONFIG LOADER (supports ${ENV_VAR} substitution)
# -----------------------------------------------------------------------------
def load_config(path="config.yaml"):
    """Load YAML and replace ${VAR} with environment variables if set."""
    with open(path, "r") as f:
        text = f.read()

    text = re.sub(
        r"\$\{([^}]+)\}",
        lambda m: os.getenv(m.group(1), m.group(0)),
        text,
    )
    return yaml.safe_load(text)


def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


# -----------------------------------------------------------------------------
# GOOGLE SHEETS UTILITIES
# -----------------------------------------------------------------------------
def get_sheets_service(cfg: dict):
    sa_path = cfg["google_sheets"]["service_account_json"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    service = build("sheets", "v4", credentials=creds)
    return service


def read_monitors(service, cfg: dict) -> List[Dict]:
    sheet_id = cfg["google_sheets"]["sheet_id"]
    monitors_tab = cfg["google_sheets"]["monitors_tab"]

