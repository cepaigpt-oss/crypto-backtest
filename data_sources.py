import requests
import os
import time
import random

COINMARKETCAP_API_KEY = os.getenv("COINMARKETCAP_API_KEY")

# -----------------------------
# Config
# -----------------------------
BASE_DELAY = 8        # seconds base backoff
MAX_ATTEMPTS = 5      # retry limit


# -----------------------------
# Helper: Safe GET with retry
# -----------------------------
def safe_get(url, label, headers=None, params=None):
    for attempt in range(MAX_ATTEMPTS):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=20)
            if resp.status_code == 429:
                wait = BASE_DELAY * (2 ** attempt) + random.uniform(2, 5)
                print(f"[{label}] 429 Too Many Requests — waiting {wait:.1f}s", flush=True)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            wait = BASE_DELAY * (2 ** attempt) + random.uniform(1, 3)
            print(f"[{label}] Request failed ({e}). Retrying in {wait:.1f}s...", flush=True)
            time.sleep(wait)
    print(f"[{label}] Max retries reached — skipping.", flush=True)
    return None


# -----------------------------
# Individual Data Fetchers
# -----------------------------
def get_from_coingecko():
    print("[Data] Fetching from CoinGecko...", flush=True)
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 250, "page": 1}
    data = safe_get(url, "CoinGecko", params=params)
    if not data:
        print("[CoinGecko] No data returned.", flush=True)
        return []
    return [
        {
            "id": coin.get("id"),
            "symbol": coin.get("symbol", "").upper(),
            "price": coin.get("current_price"),
            "volume": coin.get("total_volume"),
            "source": "coingecko",
        }
        for coin in data
    ]


def get_from_coinmarketcap():
    print("[Data] Fetching from CoinMarketCap...", flush=True)
    headers = {"X-CMC_PRO_API_KEY": COINMARKETCAP_API_KEY} if COINMARKETCAP_API_KEY else {}
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    params = {"limit": 250, "convert": "USD"}
    data = safe_get(url, "CoinMarketCap", headers=headers, params=params)
    if not data or "data" not in data:
        print("[CoinMarketCap] No data returned.", flush=True)
        return []
    return [
        {
            "id": str(d.get("id")),
            "symbol": d.get("symbol", "").upper(),
            "price": d["quote"]["USD"].get("price"),
            "volume": d["quote"]["USD"].get("volume_24h"),
            "source": "coinmarketcap",
        }
        for d in data["data"]
    ]


# -----------------------------
# Combined Fetcher with Deduplication
# -----------------------------
def get_combined_market_data():
    print("[Data] Fetching from multiple sources (CoinGecko, CoinMarketCap)...", flush=True)
    all_data = []

    cg = get_from_coingecko()
    if cg:
        print(f"[Data] CoinGecko returned {len(cg)} coins", flush=True)
        all_data.extend(cg)

    cmc = get_from_coinmarketcap()
    if cmc:
        print(f"[Data] CoinMarketCap returned {len(cmc)} coins", flush=True)
        all_data.extend(cmc)

    # Deduplicate by symbol (case-insensitive)
    seen = set()
    unique_data = []
    for coin in all_data:
        symbol = coin["symbol"].upper()
        if symbol not in seen:
            seen.add(symbol)
            unique_data.append(coin)

    print(f"[Data] Combined total (after deduplication): {len(unique_data)}", flush=True)
    return unique_data
