import os
import json
import ipinfo
import gspread
import streamlit as st
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials


_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]


def log_visitor():
    #Log visitor info to googlesheets
    if st.session_state.get("_visitor_logged"):
        return

    try:
        # # capturing request headers
        headers = st.context.headers
        raw_ip = headers.get("X-Forwarded-For", "")
        ip = raw_ip.split(",")[0].strip() if raw_ip else None
        user_agent = headers.get("User-Agent", "Unknown")

        if not ip:
            return

        # populating ipinfo details
        ipinfo_token = (os.getenv("IPINFO_TOKEN") or st.secrets.get("IPINFO_TOKEN", "")).strip()
        handler = ipinfo.getHandler(ipinfo_token)
        details = handler.getDetails(ip)
        data = details.all
        org  = data.get("org",  "Unknown")
        city = data.get("city", "Unknown")

        # #writing to google sheets
        sheet_id = os.getenv("GOOGLE_SHEET_ID") or st.secrets.get("GOOGLE_SHEET_ID")
        sa_raw   = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        sa_info  = json.loads(sa_raw)

        creds  = Credentials.from_service_account_info(sa_info, scopes=_SCOPES)
        client = gspread.authorize(creds)
        sheet  = client.open_by_key(sheet_id).sheet1

        sheet.append_row([
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            ip,
            org,
            city,
            user_agent,
        ])

        st.session_state._visitor_logged = True

    except Exception as e:
        st.session_state._tracker_error = str(e)
