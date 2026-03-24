# Codex Prompt: Fix All Bugs in Collaby Scraper

## Project Overview

Collaby is a NYC apartment/sublet scraper in `/home/user/Collaby/`. The main file is `scrape.py` (2767 lines). There is also a Jupyter notebook `collaby_colab.ipynb` and a Node.js AI agent `brain.js`.

The scraper:
1. Auto-installs dependencies (requests, beautifulsoup4, lxml, pandas, playwright)
2. Runs preflight health checks on 12 listing sites
3. Logs into 9 sites using hardcoded credentials
4. Scrapes listings from each site using Playwright browser automation
5. Enriches/filters results by price, location, duration
6. Exports CSVs, JSON, diagnostics zip

## YOUR TASK

Fix every bug in `scrape.py`, `collaby_colab.ipynb`, and `brain.js` so the project runs end-to-end without crashing. Below is the complete list of bugs found during audit. Fix ALL of them.

---

## BUG LIST FOR scrape.py

### Bug 1: `lp_pg.close()` crashes when `lp_pg` is None (Line 2488)

**Problem**: In the Listings Project scraper's `finally` block (line 2488), `lp_pg.close()` is called directly. But `lp_pg` is initialized to `None` on line 2384, and if `ctx.new_page()` fails on line 2388, `lp_pg` stays `None`. The bare `try/except` catches the AttributeError but this is sloppy.

**Fix**: Replace `lp_pg.close()` on line 2488 with `safe_close_page(lp_pg)`. The `safe_close_page()` function already exists at line 675 and handles None safely.

**Before** (line 2486-2490):
```python
    finally:
        try:
            lp_pg.close()
        except Exception:
            pass
```

**After**:
```python
    finally:
        safe_close_page(lp_pg)
```

### Bug 2: `f'${pn:,}/{pp}'` crashes when `pn` is 0 (Line 1354)

**Problem**: The format string `f'${pn:,}/{pp}' if pn else ''` uses `pn` as a truthy check. But `pn` could be `0` (a valid price parse result), and `0` is falsy in Python, so `f'${0:,}/month'` would never execute — it would return `''` instead. More importantly, if `pn` is `None`, the `:,` format specifier would crash with TypeError.

**Fix**: Change the condition from `if pn` to `if pn is not None` everywhere this pattern appears.

Search for ALL occurrences of this pattern across the file (it appears in every scraper section — Craigslist, LeaseBreak, SpareRoom, Sublet.com, SabbaticalHomes, Zumper, Loftey, Ohana, June Homes, RentHop, Listings Project). Fix every single one:

**Before**:
```python
'price_raw': f'${pn:,}/{pp}' if pn else '',
```

**After**:
```python
'price_raw': f'${pn:,}/{pp}' if pn is not None else '',
```

### Bug 3: Loftey borough hardcoded incorrectly (Line 2017)

**Problem**: `boro = 'Manhattan' if hood in ["Hell's Kitchen",'Upper West Side','Midtown West','Chelsea'] else 'Brooklyn'` defaults to `'Brooklyn'` for ANY neighborhood not in the short list. But Loftey searches include Manhattan neighborhoods like "Midtown East", "Murray Hill", "Gramercy", "Greenwich Village", "Flatiron", "NoMad", etc. These would all incorrectly get `'Brooklyn'`.

**Fix**: Change the default to `'Manhattan'` since Loftey's search URLs in this scraper are all Manhattan neighborhoods. Brooklyn listings are not being searched.

**Before**:
```python
boro = 'Manhattan' if hood in ["Hell's Kitchen",'Upper West Side','Midtown West','Chelsea'] else 'Brooklyn'
```

**After**:
```python
boro = 'Manhattan'
```

### Bug 4: Google Drive mount code is commented out (Lines 842-843)

**Problem**: The `drive.mount()` calls are commented out, so when running in Google Colab with `SAVE_TO_DRIVE = True`, the script sets `DRIVE_DIR` to `/content/drive/MyDrive/NYC_Listings` but Drive is never actually mounted. The `mkdir()` on line 847 will fail or create a local directory that isn't synced to Drive.

**Fix**: Uncomment the drive mount lines so they actually run in Colab.

**Before** (lines 840-844):
```python
    if running_in_colab():
        try:
#             from google.colab import drive  # Colab-only
#             drive.mount('/content/drive')  # Colab-only
            DRIVE_DIR = Path('/content/drive/MyDrive/NYC_Listings')
```

**After**:
```python
    if running_in_colab():
        try:
            from google.colab import drive
            drive.mount('/content/drive')
            DRIVE_DIR = Path('/content/drive/MyDrive/NYC_Listings')
```

### Bug 5: Inconsistent capitalization in SabbaticalHomes email (Line 813)

**Problem**: The `sh` (SabbaticalHomes) credential has `'Caballerodante421@gmail.com'` with a capital `C`, while all other entries use lowercase `'caballerodante421@gmail.com'`. Some login forms are case-sensitive and this could cause auth failures.

**Fix**: Make it lowercase to match the others.

**Before**:
```python
    'sh':     {'email': 'Caballerodante421@gmail.com',    'password': 'sufba1-wozreb-Hedcug'},
```

**After**:
```python
    'sh':     {'email': 'caballerodante421@gmail.com',    'password': 'sufba1-wozreb-Hedcug'},
```

### Bug 6: Use `wait_for_load_state` instead of fixed `wait_for_timeout` after login (Multiple locations)

**Problem**: After clicking login submit buttons, the code uses `pg.wait_for_timeout(5000)` which is a fixed 5-second wait. This is unreliable — on slow connections it's too short (login not complete), on fast connections it wastes time.

**Fix**: After each login submit click, add `wait_for_load_state('networkidle')` with a fallback timeout, instead of (or in addition to) the fixed wait. Apply this to ALL login flows. The locations are:

- **LeaseBreak login** (around line 1427): After `pass_in.press('Enter')`, change `pg.wait_for_timeout(5000)` to:
  ```python
  try:
      pg.wait_for_load_state('networkidle', timeout=15000)
  except Exception:
      pg.wait_for_timeout(5000)
  ```

- **SpareRoom login** (around line 1539): Same fix after submit
- **Sublet.com login** (around line 1649): Same fix after submit
- **SabbaticalHomes login** (around line 1749): Same fix after submit
- **Zumper login** (around line 1858): Same fix after submit
- **Loftey login** (around line 1970): Same fix after submit
- **Ohana login** (around line 2110): Same fix after submit
- **June Homes login** (around line 2258): Same fix after submit
- **Listings Project login** (around line 2415): Same fix after submit

### Bug 7: `record_scrape_result` called but result not used for skipped sources (Logic gap)

**Problem**: When a source FAILS preflight and `PREFLIGHT_SKIP_FAILING_SOURCES` is True, the scraper section is skipped entirely. But `record_scrape_result()` is never called, so `SOURCE_HEALTH` stays at whatever the preflight set. This is actually correct behavior, BUT the individual scraper sections don't check `SOURCE_HEALTH` before running — they run unconditionally.

**Fix**: At the top of each scraper section (Craigslist, LeaseBreak, SpareRoom, etc.), add a guard that skips the scraper if preflight marked it as FAIL:

```python
if PREFLIGHT_SKIP_FAILING_SOURCES and SOURCE_HEALTH.get('SourceName', {}).get('status') == 'FAIL':
    print('⏭️ Skipping SourceName (preflight FAIL)')
else:
    # ... existing scraper code ...
```

Check each scraper section — some may already have this guard. Add it where missing. The source names are:
- `Craigslist`
- `LeaseBreak`
- `SpareRoom`
- `Sublet.com`
- `SabbaticalHomes`
- `Zumper`
- `Loftey`
- `Ohana`
- `June Homes`
- `RentHop`
- `Listings Project`

---

## BUG LIST FOR collaby_colab.ipynb

### Bug 8: Same bugs as scrape.py

The notebook contains essentially the same code as scrape.py. Apply all the same fixes (Bugs 1-7) to the corresponding cells in the notebook.

### Bug 9: Missing `display()` fallback in notebook

**Problem**: The notebook assumes IPython's `display()` is always available (since it runs in Colab). But if someone downloads and runs it locally in Jupyter, it might not have it.

**Fix**: In the notebook cell that contains the preflight section, ensure this block exists at the top:
```python
try:
    from IPython.display import display
except ImportError:
    def display(x): print(x)
```

### Bug 10: Drive mount commented out in notebook too

Same as Bug 4. Uncomment the `drive.mount()` lines in the notebook's Cell 3.

---

## BUG LIST FOR brain.js

### Bug 11: No API key validation (Line 6)

**Problem**: `new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })` doesn't check if the key exists. If `.env` is missing or key is empty, it fails with a cryptic error.

**Fix**: Add validation at the top of the file, after imports:

```javascript
if (!process.env.ANTHROPIC_API_KEY) {
    console.error('❌ ANTHROPIC_API_KEY not set. Copy .env.example to .env and add your key.');
    process.exit(1);
}
```

### Bug 12: Infinite loop risk in main agent loop (Lines 79-133)

**Problem**: The `while (true)` loop has no iteration limit. If Claude never returns `stop_reason === "end_turn"`, it loops forever burning API credits.

**Fix**: Add a max iteration counter:

```javascript
const MAX_ITERATIONS = 50;
let iteration = 0;

while (iteration < MAX_ITERATIONS) {
    iteration++;
    // ... existing loop body ...
}

if (iteration >= MAX_ITERATIONS) {
    console.log('⚠️ Reached maximum iterations. Stopping.');
}
```

### Bug 13: Model name may need updating (Line 81)

**Problem**: `model: "claude-opus-4-5"` — verify this is the correct model ID. The current valid model IDs are:
- `claude-opus-4-6`
- `claude-sonnet-4-6`
- `claude-haiku-4-5-20251001`

**Fix**: Update to `claude-sonnet-4-6` (good balance of capability and cost for an agent):

```javascript
model: "claude-sonnet-4-6",
```

---

## VERIFICATION STEPS

After making all fixes, verify:

1. **Syntax check**: Run `python3 -c "import py_compile; py_compile.compile('scrape.py', doraise=True)"` — should pass with no errors.

2. **Import check**: Run `python3 -c "exec(open('scrape.py').read())"` — this will fail on network (expected in sandbox), but should get past all imports and config without crashing.

3. **brain.js syntax**: Run `node --check brain.js` — should pass.

4. **Verify all `pn` checks fixed**: Run `grep -n "if pn else" scrape.py` — should return 0 results (all changed to `if pn is not None else`).

5. **Verify lp_pg fix**: Run `grep -n "lp_pg.close()" scrape.py` — should return 0 results (changed to `safe_close_page(lp_pg)`).

6. **Verify drive mount uncommented**: Run `grep -n "# .*drive.mount\|# .*from google.colab import drive" scrape.py` — should return 0 results.

---

## IMPORTANT NOTES

- Do NOT change the overall structure of the file — only fix bugs.
- Do NOT add new features, refactor, or "improve" code that isn't broken.
- Do NOT remove or change credentials (they are intentionally hardcoded for ease of use).
- Do NOT change search parameters (SEARCH_MAX_MONTHLY, TARGET_STREET_MIN, etc.).
- Do NOT add type hints, docstrings, or comments to code you didn't change.
- Preserve exact indentation (the file uses 4-space indentation throughout).
- The notebook `.ipynb` is JSON — be careful with escaping when editing it.
- Commit all changes with a clear message and push to the branch.
