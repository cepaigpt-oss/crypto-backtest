import requests
import os

COINMARKETCAP_API_KEY = os.getenv("COINMARKETCAP_API_KEY")

# -----------------------------
# Individual Data Fetchers
# -----------------------------

def get_from_coingecko():
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100, "page": 1}
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
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
    except Exception as e:
        print(f"[CoinGecko] Failed: {e}", flush=True)
        return []


def get_from_coinmarketcap():
    try:
        headers = {"X-CMC_PRO_API_KEY": COINMARKETCAP_API_KEY}
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
        params = {"limit": 100, "convert": "USD"}
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        return [
            {
                "id": str(d.get("id")),
                "symbol": d.get("symbol", "").upper(),
                "price": d["quote"]["USD"].get("price"),
                "volume": d["quote"]["USD"].get("volume_24h"),
                "source": "coinmarketcap",
            }
            for d in data
        ]
    except Exception as e:
        print(f"[CoinMarketCap] Failed: {e}", flush=True)
        return []


def get_from_binance():
    try:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        return [
            {
                "id": b.get("symbol"),
                "symbol": b.get("symbol", "").upper(),
                "price": float(b.get("lastPrice", 0.0)),
                "volume": float(b.get("quoteVolume", 0.0)),
                "source": "binance",
            }
            for b in data
        ]
    except Exception as e:
        print(f"[Binance] Failed: {e}", flush=True)
        return []


# -----------------------------
# Combined Fetcher with Deduplication
# -----------------------------

def get_combined_market_data():
    print("[Data] Fetching from multiple sources...", flush=True)
    all_data = []

    coingecko = get_from_coingecko()
    if coingecko:
        print(f"[Data] CoinGecko returned {len(coingecko)} coins", flush=True)
        all_data.extend(coingecko)

    coinmarketcap = get_from_coinmarketcap()
    if coinmarketcap:
        print(f"[Data] CoinMarketCap returned {len(coinmarketcap)} coins", flush=True)
        all_data.extend(coinmarketcap)

    binance = get_from_binance()
    if binance:
        print(f"[Data] Binance returned {len(binance)} tickers", flush=True)
        all_data.extend(binance)

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
