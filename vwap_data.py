import requests
import time
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# -------------------- CONFIG --------------------
SHEET_NAME = "VWAP stocks data"  # Your Google Sheet name
SERVICE_ACCOUNT_FILE = "path_to_your_service_account.json"  # JSON file path
UPDATE_INTERVAL = 60  # seconds

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
# sheet = client.open(VWAP stocks data).sheet1
sheet = client.open("VWAP stocks data").worksheet("VWAP data")

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
session.get("https://www.nseindia.com", headers=headers)

# -------------------- MAIN LOOP --------------------
while True:
    records = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for symbol in symbols:
        params = {"symbol": symbol, "section": "trade_info"}
        try:
            response = session.get(url, headers=headers, params=params, timeout=10)
            data = response.json()

            trade_info = data.get("marketDeptOrderBook", {}).get("tradeInfo", {})

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

    # Convert to DataFrame
    df = pd.DataFrame(records)

    # Append to Google Sheet
    set_with_dataframe(sheet, df, row=len(sheet.get_all_values())+1, include_index=False, include_column_header=False)
    print(f"{timestamp} - Data pushed successfully!")

    # Wait for next update
    time.sleep(UPDATE_INTERVAL)
