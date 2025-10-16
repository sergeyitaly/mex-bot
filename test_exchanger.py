# playwright_bybit.py
from playwright.sync_api import sync_playwright
import re, json

def fetch_bybit_markets():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)   # or headless=False for debugging
        page = browser.new_page(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ))
        # Visit derivatives markets page (choose locale path that works)
        page.goto("https://www.bybit.com/en-US/markets/derivatives", timeout=30000)
        # wait for network to be mostly idle
        page.wait_for_load_state("networkidle", timeout=15000)

        # Option 1: capture XHR responses that look like instruments lists
        markets = set()
        def handle_resp(resp):
            try:
                url = resp.url
                if "instruments-info" in url or "/market/instruments" in url or "instruments" in url.lower():
                    txt = resp.text()
                    try:
                        j = json.loads(txt)
                        # walk for lists
                        if isinstance(j, dict):
                            for k in ("result","data","list","rows"):
                                part = j.get(k)
                                if isinstance(part, list):
                                    for it in part:
                                        if isinstance(it, dict):
                                            sym = it.get("symbol") or it.get("instId") or it.get("instrument") or it.get("name")
                                            if sym:
                                                markets.add(re.sub(r'[^A-Z0-9]', '', str(sym).upper()))
                    except Exception:
                        pass
            except Exception:
                pass

        page.on("response", handle_resp)

        # Option 2: also try to extract embedded JSON/JS vars
        html = page.content()
        # look for big JSON-like blobs
        m = re.search(r'window\.__INITIAL_STATE__\s*=\s*({.+?});\s*</script>', html, flags=re.S)
        if not m:
            m = re.search(r'window\.__DATA__\s*=\s*({.+?});', html, flags=re.S)
        if m:
            try:
                obj = json.loads(m.group(1))
                # search recursively for symbol lists
                def find_symbols(obj):
                    res = set()
                    if isinstance(obj, dict):
                        for k,v in obj.items():
                            if k.lower() in ("symbols","markets","pairs","instruments","tickers","contracts"):
                                if isinstance(v, list):
                                    for it in v:
                                        if isinstance(it, dict):
                                            sym = it.get("symbol") or it.get("pair") or it.get("instId") or it.get("name")
                                            if sym:
                                                res.add(re.sub(r'[^A-Z0-9]', '', str(sym).upper()))
                            else:
                                res |= find_symbols(v)
                    elif isinstance(obj, list):
                        for it in obj:
                            res |= find_symbols(it)
                    return res
                markets |= find_symbols(obj)
            except Exception:
                pass

        browser.close()
        return sorted(markets)

if __name__ == "__main__":
    ms = fetch_bybit_markets()
    print("Bybit markets from browser:", len(ms))
    print(ms[:200])
