#!/usr/bin/env python3
"""Quick connectivity check for all sources used by the discovery notebook."""

import requests
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

TIMEOUT = 10

ENDPOINTS = {
    "Craigslist":       "https://newyork.craigslist.org/search/mnh/sub?format=rss&max_price=4500&hasPic=1",
    "LeaseBreak":       "https://www.leasebreak.com/login",
    "SpareRoom":        "https://www.spareroom.com/logon/",
    "Sublet.com":       "https://www.sublet.com/login",
    "SabbaticalHomes":  "https://www.sabbaticalhomes.com/Login",
    "Zumper":           "https://www.zumper.com/login",
    "Loftey":           "https://loftey.com/login",
    "Ohana":            "https://liveohana.ai/login",
    "June Homes":       "https://junehomes.com/login",
    "RentHop":          "https://www.renthop.com/apartments-for-rent/new-york-ny/sublet",
    "Listings Project": "https://www.listingsproject.com/login",
}


def check(name, url):
    try:
        r = requests.get(url, timeout=TIMEOUT, allow_redirects=True)
        return name, r.status_code, None
    except Exception as e:
        return name, None, str(e)


def main():
    print(f"Checking {len(ENDPOINTS)} endpoints (timeout={TIMEOUT}s) ...\n")
    results = []

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(check, n, u): n for n, u in ENDPOINTS.items()}
        for f in as_completed(futures):
            results.append(f.result())

    results.sort(key=lambda r: r[0])

    ok, fail = 0, 0
    for name, status, err in results:
        if status and status < 400:
            print(f"  ✅ {name:<20} HTTP {status}")
            ok += 1
        elif status:
            print(f"  ⚠️  {name:<20} HTTP {status}")
            fail += 1
        else:
            short_err = err.split(":")[0] if err else "unknown"
            print(f"  ❌ {name:<20} {short_err}")
            fail += 1

    print(f"\n{'='*40}")
    print(f"Reachable: {ok}/{len(ENDPOINTS)}  |  Failed: {fail}/{len(ENDPOINTS)}")

    if fail > 0:
        print("\nTip: If on phone hotspot, check that your tethering is active")
        print("     and your firewall/VPN isn't blocking outbound HTTPS.")
    else:
        print("\nAll good — you're ready to run the notebook! 🚀")

    sys.exit(1 if fail == len(ENDPOINTS) else 0)


if __name__ == "__main__":
    main()
