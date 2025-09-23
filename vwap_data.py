# vwap_data.py
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
import time

# -------------------- CONFIG --------------------
SERVICE_ACCOUNT_FILE = "service_account.json"  # path to your service account JSON
SHEET_ID = os.environ.get("SHEET_ID")          # GitHub secret for Google Sheet ID
UPDATE_INTERVAL = 60                            # in seconds

symbols = [
    "ADANIENT", "MARUTI", "BAJFINANCE", "EICHERMOT", "MM", "SHRIRAMFIN",
    "JSWSTEEL", "AXISBANK", "BAJAJFINSV", "NTPC", "SBIN", "POWERGRID",
    "INDUSINDBK", "TATAMOTORS", "DRREDDY", "TATASTEEL", "BAJAJ-AUTO", "TATACONSUM",
    "INFY", "KOTAKBANK", "ADANIPORTS", "COALINDIA", "HINDALCO", "ICICIBANK",
    "WIPRO", "LT", "TCS", "HDFCBANK", "HEROMOTOCO", "ONGC",
    "BEL", "SUNPHARMA", "APOLLOHOSP", "RELIANCE", "JIOFIN", "SBILIFE",
    "ITC", "TITAN", "HCLTECH", "CIPLA", "BHARTIARTL", "ETERNAL",
    "HINDUNILVR", "HDFCLIFE", "ASIANPAINT", "GRASIM", "NESTLEIND", "ULTRACEMCO",
    "TECHM", "TRENT"
]

# -------------------- GOOGLE SHEETS SETUP --------------------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).sheet1

# Write headers if sheet is empty
if len(sheet.get_all_values()) == 0:
    headers = ["Timestamp", "Symbol", "Prev Close", "Open", "High", "Low", "VWAP"]
    sheet.append_row(headers)

# -------------------- NSE SESSION --------------------
url = "https://www.nseindia.com/api/quote-equity"
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/get-quotes/equity",
    "Accept-Language": "en-US,en;q=0.9",
}

session = requests.Session()
session.get("https://www.nseindia.com", headers=headers)  # get cookies

# -------------------- MAIN LOOP --------------------
while True:
    records = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for symbol in symbols:
        params = {"symbol": symbol, "section": "trade_info"}
        try:
            response = session.get(url, headers=headers, params=params, timeout=10)
            # Try parsing JSON, fallback to printing raw if fails
            try:
                data = response.json()
            except Exception:
                print(f"[{timestamp}] Failed to parse JSON for {symbol}, raw response: {response.text[:500]}")
                continue

            # Correct JSON parsing for NSE trade info
            trade_info = data.get("priceInfo") or data.get("marketDeptOrderBook", {}).get("tradeInfo", {})

            prev_close = trade_info.get("previousClose")
            open_price = trade_info.get("open")
            high_price = trade_info.get("intraDayHighLow", {}).get("max")
            low_price = trade_info.get("intraDayHighLow", {}).get("min")
            vwap = trade_info.get("vwap")

            records.append({
                "Timestamp": timestamp,
                "Symbol": symbol,
                "Prev Close": prev_close,
                "Open": open_price,
                "High": high_price,
                "Low": low_price,
                "VWAP": vwap
            })

        except Exception as e:
            print(f"[{timestamp}] Error fetching {symbol}: {e}")
            records.append({
                "Timestamp": timestamp,
                "Symbol": symbol,
                "Prev Close": None,
                "Open": None,
                "High": None,
                "Low": None,
                "VWAP": None
            })

        time.sleep(0.5)  # avoid NSE blocking

    # Convert to DataFrame
    df = pd.DataFrame(records)
    print(df.head())  # check top rows in console

    # Append to Google Sheet
    start_row = len(sheet.get_all_values()) + 1
    set_with_dataframe(sheet, df, row=start_row, include_index=False, include_column_header=False)
    print(f"[{timestamp}] Data pushed to Google Sheet successfully!")

    time.sleep(UPDATE_INTERVAL)
