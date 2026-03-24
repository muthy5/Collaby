# How to Run Collaby

Collaby searches 11 apartment/sublet websites in NYC and saves what it finds into spreadsheets.

---

## iPhone / iPad

1. Open this link in Safari: https://colab.research.google.com/github/muthy5/Collaby/blob/main/collaby_colab.ipynb
2. Sign in with your Google account
3. Tap **Runtime** > **Run all**
4. Wait 5-15 minutes
5. A zip file downloads automatically when done

That's it. Open the CSV files in Google Sheets or Numbers.

---

## Windows

1. Install Python from https://www.python.org/downloads/ — CHECK the box **"Add Python to PATH"**
2. Download this repo (green **Code** button > **Download ZIP**), unzip it
3. Double-click `run.bat`

---

## Mac / Linux

1. Install Python from https://www.python.org/downloads/
2. Download this repo (green **Code** button > **Download ZIP**), unzip it
3. Double-click `run.sh`

---

## PowerShell

```
cd Collaby
python scrape.py
```

---

Everything installs automatically on first run. Wait 5-15 minutes for results.

---

## Your Results

| File | What's In It |
|------|-------------|
| `output/01_candidates_filtered.csv` | Listings that match your search (the good stuff) |
| `output/01_exact_address_hits.csv` | Listings with exact addresses found |
| `output/01_contact_queue.csv` | Listings where you need to message for the address |
| `output/01_all_raw_listings.csv` | Everything found, before filtering |
| `nyc_listings_YYYYMMDD_HHMM.zip` | All of the above in one zip file |

Open CSV files with Excel, Google Sheets, or Numbers.

---

## If Something Goes Wrong

**"python is not recognized"** — Reinstall Python and CHECK the "Add to PATH" box.

**A website shows 0 listings** — That site may have changed its layout. The scraper skips it and keeps going.

**"Login not confirmed" error** — A password may have changed. Open `scrape.py`, find `CREDS` near the top, update the password.

---

## Change Your Search

Open `scrape.py` (or the notebook) in any text editor and change these numbers near the top:

```python
SEARCH_MAX_MONTHLY = 4500       # max rent per month
SEARCH_MIN_MONTHS = 3           # minimum lease length
SEARCH_MAX_MONTHS = 6           # maximum lease length
TARGET_STREET_MIN = 20          # southern boundary (20th St)
TARGET_STREET_MAX = 62          # northern boundary (62nd St)
```

Save and run again.
