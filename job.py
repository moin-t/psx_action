import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # Python 3.9+
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

# ----------------------------
# Config — "yesterday" in PKT
# ----------------------------
PK_TZ = ZoneInfo("Asia/Karachi")
YESTERDAY = (datetime.now(PK_TZ) - timedelta(days=1)).date()
TARGET_YEAR, TARGET_MONTH = YESTERDAY.year, YESTERDAY.month

# ----------------------------
# Load company list
# ----------------------------
list_comp = pd.read_excel("Companies_listed_PSX_Cleaned.xlsx").head()

# ----------------------------
# Global session setup
# ----------------------------
def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "https://dps.psx.com.pk/historical",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive"
    })
    return session

# ----------------------------
# Scraper for yesterday
# ----------------------------
def fetch_yesterday(session, symbol, year, month, target_date):
    url = "https://dps.psx.com.pk/historical"

    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ GET failed for {symbol} {month}/{year}: {e}")
        return []

    form_data = {"symbol": symbol, "year": str(year), "month": str(month)}
    try:
        resp2 = session.post(url, data=form_data, timeout=25)
        resp2.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ POST failed for {symbol} {month}/{year}: {e}")
        return []

    soup = BeautifulSoup(resp2.text, "html.parser")
    rows = soup.select("tbody.tbl__body tr")

    out = []
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) != 6:
            continue

        raw_date = tds[0].get_text(strip=True)
        open_s   = tds[1].get_text(strip=True).replace(",", "")
        high_s   = tds[2].get_text(strip=True).replace(",", "")
        low_s    = tds[3].get_text(strip=True).replace(",", "")
        close_s  = tds[4].get_text(strip=True).replace(",", "")
        vol_s    = tds[5].get_text(strip=True).replace(",", "")

        dt = pd.to_datetime(raw_date, errors="coerce")
        if pd.isna(dt):
            continue

        d = dt.date()
        if d != target_date:
            continue

        try:
            out.append({
                "Date": d.isoformat(),
                "Open": float(open_s),
                "High": float(high_s),
                "Low": float(low_s),
                "Close": float(close_s),
                "Volume": int(float(vol_s)),
                "Year": d.year,
                "Month": d.month,
                "Symbol": symbol,
            })
        except ValueError:
            continue

    return out

# ----------------------------
# MAIN — only yesterday
# ----------------------------
session = create_session()
all_records = []

print(f"Collecting ONLY {YESTERDAY.isoformat()} (PKT)")

for symbol in list_comp["Symbol"]:
    print(f"🔎 {symbol} → {TARGET_YEAR}-{TARGET_MONTH:02d}")
    recs = fetch_yesterday(session, symbol, TARGET_YEAR, TARGET_MONTH, YESTERDAY)
    if recs:
        all_records.extend(recs)
    time.sleep(0.3)

df = pd.DataFrame(all_records).sort_values(["Symbol", "Date"]).drop_duplicates().reset_index(drop=True)

# add sector & company name
df["Sector"] = df["Symbol"].map(list_comp.set_index("Symbol")["Sector"])
df["Company Name"] = df["Symbol"].map(list_comp.set_index("Symbol")["Company Name"])

# ----------------------------
# Save to CSV (append rows only)
# ----------------------------
csv_path = Path("PSX_Historical_update.csv")

if csv_path.exists():
    # Append without header
    df.to_csv(csv_path, mode="a", index=False, header=False)
    print(f"✅ Appended {len(df)} rows to {csv_path}")
else:
    # First time: write with header
    df.to_csv(csv_path, mode="w", index=False, header=True)
    print(f"✅ Created {csv_path} with {len(df)} rows")
