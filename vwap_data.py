# vwap_data.py
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
import time

#
# -------------------- CONFIG --------------------
SERVICE_ACCOUNT_FILE = "service_account.json"
SHEET_ID = os.environ.get("SHEET_ID")  # read from GitHub secret

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
sheet = client.open_by_key(SHEET_ID).sheet1  # default first worksheet

# Write headers if sheet is empty
if len(sheet.get_all_values()) == 0:
    headers = ["Timestamp", "Symbol", "Prev Close", "Open", "High", "Low", "VWAP"]
    sheet.append_row(headers)

# -------------------- NSE SESSION --------------------
url = "https://www.nseindia.com/api/quote-equity"
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com"
}
session = requests.Session()
session.get("https://www.nseindia.com", headers=headers)  # get cookies

# -------------------- FETCH DATA --------------------
records = []
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for symbol in symbols:
    params = {"symbol": symbol, "section": "trade_info"}
    try:
        response = session.get(url, headers=headers, params=params, timeout=10)
        data = response.json()
        trade_info = data.get("marketDeptOrderBook", {}).get("tradeInfo", {})

        records.append({
            "Timestamp": timestamp,
            "Symbol": symbol,
            "Prev Close": trade_info.get("previousClose"),
            "Open": trade_info.get("open"),
            "High": trade_info.get("intraDayHighLow", {}).get("max"),
            "Low": trade_info.get("intraDayHighLow", {}).get("min"),
            "VWAP": trade_info.get("vwap")
        })

    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        records.append({
            "Timestamp": timestamp,
            "Symbol": symbol,
            "Prev Close": None,
            "Open": None,
            "High": None,
            "Low": None,
            "VWAP": None
        })

    time.sleep(1)  # avoid NSE blocking

# -------------------- PUSH TO GOOGLE SHEET --------------------
df = pd.DataFrame(records)
set_with_dataframe(sheet, df, row=len(sheet.get_all_values())+1, include_index=False, include_column_header=False)
print(f"{timestamp} - Data pushed successfully!")
