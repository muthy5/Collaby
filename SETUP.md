# How to Run Collaby (NYC Listings Scout)

## What This Does

Collaby searches 11 apartment/sublet websites in NYC and saves everything it finds into spreadsheets (CSV files) and a zip file you can download to your phone.

---

## Step 1: Get Python

If you don't have Python yet:

- **Mac**: Open Terminal (search "Terminal" in Spotlight). Type:
  ```
  brew install python
  ```
  If that doesn't work, download Python from https://www.python.org/downloads/

- **Windows**: Download Python from https://www.python.org/downloads/ and install it. CHECK THE BOX that says "Add Python to PATH" during install.

To check if Python is installed, open Terminal (Mac) or Command Prompt (Windows) and type:
```
python3 --version
```
You should see something like `Python 3.11.5`. Any version 3.9+ is fine.

---

## Step 2: Download Collaby

Open Terminal / Command Prompt and type:
```
git clone https://github.com/muthy5/Collaby.git
cd Collaby
```

---

## Step 3: Install What Collaby Needs

Type these commands one at a time:
```
pip install requests beautifulsoup4 lxml pandas playwright
playwright install chromium --with-deps
```

Wait for each one to finish before typing the next one.

---

## Step 4: Set Up Your API Key (for brain.js only)

This step is only needed if you want to run `brain.js` (the AI agent). Skip this if you only want to run the scraper.

1. Create a file called `.env` in the Collaby folder
2. Put this inside it (replace with your real key):
   ```
   ANTHROPIC_API_KEY=sk-ant-your-key-here
   ```
3. Save the file

---

## Step 5: Run the Scraper

Type:
```
python3 scrape.py
```

Then wait. It takes 5-15 minutes because it visits each website, logs in, and collects listings politely (with pauses so it doesn't get blocked).

---

## What Happens When It's Done

You'll see a summary printed in the terminal. Your results are saved in:

| File | What's In It |
|------|-------------|
| `output/01_candidates_filtered.csv` | Listings that match your search (the good stuff) |
| `output/01_exact_address_hits.csv` | Listings with exact addresses found |
| `output/01_contact_queue.csv` | Listings where you need to message for the address |
| `output/01_all_raw_listings.csv` | Everything found, before filtering |
| `output/01_rejected_with_reasons.csv` | Listings that didn't match and why |
| `nyc_listings_YYYYMMDD_HHMM.zip` | All of the above in one zip file |

Open the CSV files in Excel, Google Sheets, or Numbers.

---

## If Something Goes Wrong

**"command not found: python3"**
Try `python` instead of `python3`.

**"No module named playwright"**
Run `pip install playwright` again, then `playwright install chromium --with-deps`.

**A website shows 0 listings**
That website might have changed its layout. The scraper will skip it and keep going with the others.

**"Login not confirmed" error**
A password might have changed. Open `scrape.py`, find the `CREDS` section near the top, and update the password.

---

## How to Change What You're Searching For

Open `scrape.py` in any text editor and find these lines near the top:

```python
SEARCH_MAX_MONTHLY = 4500          # max rent per month
SEARCH_MIN_MONTHS = 3              # minimum lease length
SEARCH_MAX_MONTHS = 6              # maximum lease length
TARGET_AREA_LABEL = "Hell's Kitchen / Chelsea / Hudson Yards / Lincoln Center"
TARGET_STREET_MIN = 20             # southern boundary (20th St)
TARGET_STREET_MAX = 62             # northern boundary (62nd St)
TARGET_AVENUE_MIN = 8              # western boundary (8th Ave)
TARGET_AVENUE_MAX = 12             # eastern boundary (12th Ave)
```

Change the numbers, save the file, and run `python3 scrape.py` again.
