# How to Run Collaby

Collaby searches 11 apartment/sublet websites in NYC and saves what it finds into spreadsheets.

---

## Run It

### 1. Get Python (if you don't have it)

- **Windows**: Download from https://www.python.org/downloads/ and install. CHECK THE BOX that says **"Add Python to PATH"**
- **Mac**: Download from https://www.python.org/downloads/ and install

### 2. Download and run

Download this repo (green **Code** button > **Download ZIP** on GitHub), unzip it, then:

- **Windows**: Double-click `run.bat`
- **Mac/Linux**: Double-click `run.sh` (or open Terminal, `cd Collaby`, `./run.sh`)
- **PowerShell**: Open PowerShell, then:
  ```
  cd Collaby
  python scrape.py
  ```

That's it. Everything else installs automatically the first time.

Wait 5-15 minutes. When it's done, your results are in the `output/` folder and a zip file.

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

**"python is not recognized"** — Python isn't installed or not in PATH. Reinstall Python and CHECK the "Add to PATH" box.

**A website shows 0 listings** — That site may have changed its layout. The scraper skips it and keeps going.

**"Login not confirmed" error** — A password may have changed. Open `scrape.py`, find `CREDS` near the top, update the password.

---

## Change Your Search

Open `scrape.py` in any text editor and change these numbers near the top:

```python
SEARCH_MAX_MONTHLY = 4500       # max rent per month
SEARCH_MIN_MONTHS = 3           # minimum lease length
SEARCH_MAX_MONTHS = 6           # maximum lease length
TARGET_STREET_MIN = 20          # southern boundary (20th St)
TARGET_STREET_MAX = 62          # northern boundary (62nd St)
```

Save and run again.
