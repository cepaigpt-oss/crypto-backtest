import requests
import time
import random
import os

COINMARKETCAP_API_KEY = os.getenv("COINMARKETCAP_API_KEY")

def get_from_coingecko():
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100, "page": 1}
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()
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
        return [{"id": d["id"], "symbol": d["symbol"], "price": d["quote"]["USD"]["price"]} for d in data]
    except Exception as e:
        print(f"[CoinMarketCap] Failed: {e}", flush=True)
        return []

def get_from_binance():
    try:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[Binance] Failed: {e}", flush=True)
        return []

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
        print(f"[Data] Binance returned {len(binance)} coins", flush=True)
        all_data.extend(binance)

    print(f"[Data] Combined total assets fetched: {len(all_data)}", flush=True)
    return all_data
