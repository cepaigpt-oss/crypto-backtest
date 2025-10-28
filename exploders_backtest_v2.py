"""
Backtest: Small-cap 100% Exploders Continuation Strategy (Multithreaded Balanced Version)
Universe: Top 500 coins by market cap (CoinGecko),
then filter to $2M–$100M.
"""

import sys
import math
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------- Parameters --------------------
BASE = "https://api.coingecko.com/api/v3"
CURRENCY = "usd"

START_DT = datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
END_DT = datetime(2025, 9, 15, 23, 59, 59, tzinfo=timezone.utc)
START = START_DT.timestamp()
END = END_DT.timestamp()

# Universe filter
MCAP_MIN = 2_000_000
MCAP_MAX = 100_000_000

VOL_MULT_MIN = 2.0
ONE_DAY_PUMP = 2.0
TWO_DAY_PUMP = 2.0

RISK_PCT = 0.02
FEE_PER_SIDE = 0.005
MAX_CONCURRENT = 5
ATR_LEN = 14
ATR_MULT_STOP = 3.0
HARD_HOLD_DAYS = 10

API_SLEEP = 0.0  # throttle
MAX_WORKERS = 10  # Balanced multithreading speed

# -------------------- Helpers --------------------
def get_top_coins(limit=500):
    url = f"{BASE}/coins/markets"
    params = {"vs_currency": CURRENCY, "order": "market_cap_desc", "per_page": 250}
    coins = []
    for page in range(1, (limit // 250) + 1):
        params["page"] = page
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        coins.extend(r.json())
    return coins[:limit]

def market_chart_range(coin_id, frm, to):
    url = f"{BASE}/coins/{coin_id}/market_chart/range"
    params = {"vs_currency": CURRENCY, "from": int(frm), "to": int(to)}
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=45)
            if r.status_code == 429:
                time.sleep(3)
                continue
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(1)
    return None

def to_daily_df(mc):
    if not mc or "prices" not in mc:
        return None
    df = pd.DataFrame({
        "ts": [p[0] for p in mc["prices"]],
        "close": [p[1] for p in mc["prices"]],
    })
    n = len(df)
    def _safe(arr):
        return [x[1] for x in arr][:n] if arr else [np.nan] * n
    df["mcap"] = _safe(mc.get("market_caps", []))
    df["vol"] = _safe(mc.get("total_volumes", []))
    df["date"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.normalize()
    df = df.groupby("date", as_index=False).agg({"close": "last", "mcap": "last", "vol": "last"})
    df = df[
        (df["date"] >= pd.to_datetime(START, unit="s", utc=True).normalize())
        & (df["date"] <= pd.to_datetime(END, unit="s", utc=True).normalize())
    ]
    if len(df) < ATR_LEN + 5:
        return None
    df["ret1"] = df["close"].pct_change()
    df["ret2"] = df["close"].pct_change(2)
    df["tr"] = df["close"].diff().abs()
    df["atr"] = df["tr"].rolling(ATR_LEN, min_periods=ATR_LEN).mean()
    df["vol_ma10"] = df["vol"].rolling(10, min_periods=10).mean()
    return df.reset_index(drop=True)

def find_signals(df):
    sig = []
    for i in range(2, len(df)):
        one_day = (1 + df.loc[i, "ret1"]) if pd.notna(df.loc[i, "ret1"]) else 0
        two_day = (1 + df.loc[i, "ret2"]) if pd.notna(df.loc[i, "ret2"]) else 0
        if one_day >= ONE_DAY_PUMP or two_day >= TWO_DAY_PUMP:
            sig.append(i)
    return sig

def first_confirmation_idx(df, sig_idx):
    for j in range(sig_idx + 1, min(sig_idx + 6, len(df))):
        if df.loc[j, "close"] <= df.loc[j - 1, "close"]:
            continue
        if pd.isna(df.loc[j, "vol_ma10"]) or df.loc[j, "vol_ma10"] == 0:
            continue
        if (df.loc[j, "vol"] / df.loc[j, "vol_ma10"]) < VOL_MULT_MIN:
            continue
        return j
    return None

# -------------------- Multithreaded Fetch --------------------
def process_coin(c):
    cid = c["id"]
    mc = market_chart_range(cid, START - 5 * 24 * 3600, END + 24 * 3600)
    if mc is None:
        return None, []
    df = to_daily_df(mc)
    if df is None or df["close"].isna().all():
        return None, []
    sig_idxs = find_signals(df)
    entries = []
    for sidx in sig_idxs:
        mcap = df.loc[sidx, "mcap"]
        if pd.isna(mcap) or not (MCAP_MIN <= mcap <= MCAP_MAX):
            continue
        cidx = first_confirmation_idx(df, sidx)
        if cidx is None:
            continue
        entry_date = df.loc[cidx, "date"]
        if not (START_DT <= entry_date <= END_DT):
            continue
        atr = df.loc[cidx, "atr"]
        if pd.isna(atr) or atr == 0:
            continue
        entry_px = df.loc[cidx, "close"] * (1 + FEE_PER_SIDE)
        raw_stop = entry_px - max(ATR_MULT_STOP * atr, 0.25 * entry_px)
        if raw_stop <= 0 or raw_stop >= entry_px:
            continue
        vol_mult = (df.loc[cidx, "vol"] / df.loc[cidx, "vol_ma10"]) if df.loc[cidx, "vol_ma10"] else 0.0
        entries.append({
            "date": entry_date,
            "coin": cid,
            "entry_px": float(entry_px),
            "stop": float(raw_stop),
            "atr": float(atr),
            "vol_mult": float(vol_mult),
        })
    return cid, entries

# -------------------- Backtest --------------------
def backtest():
    coins = get_top_coins(limit=500)
    print(f"Retrieved {len(coins)} top market cap coins from CoinGecko…")
    entries = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_coin, c): c["id"] for c in coins}
        for f in tqdm(as_completed(futures), total=len(futures), desc="Coins (multithreaded)"):
            cid, e = f.result()
            if e:
                entries.extend(e)
    all_dates = pd.date_range(START_DT, END_DT, freq="D")
    entries_df = pd.DataFrame(entries)
    if not entries_df.empty:
        entries_df["date"] = pd.to_datetime(entries_df["date"], utc=True)
    equity = 100.0
    open_positions = []
    trades = []
    curve = []
    for d in tqdm(all_dates, desc="Backtest Progress"):
        todays = []
        if not entries_df.empty:
            todays = entries_df[entries_df["date"] == d].sort_values("vol_mult", ascending=False).to_dict("records")
        slots = MAX_CONCURRENT - len(open_positions)
        for ent in todays[:slots]:
            risk_per_unit = ent["entry_px"] - ent["stop"]
            if risk_per_unit <= 0:
                continue
            risk_dollars = equity * RISK_PCT
            qty = risk_dollars / risk_per_unit
            if qty <= 0:
                continue
            open_positions.append({
                "coin": ent["coin"],
                "entry_date": d,
                "entry_px": ent["entry_px"],
                "stop": ent["stop"],
                "trail_ref": ent["entry_px"],
                "atr": ent["atr"],
                "qty": float(qty),
                "max_hold": HARD_HOLD_DAYS,
                "bars_held": 0
            })
        still_open = []
        for pos in open_positions:
            pos["bars_held"] += 1
            if pos["bars_held"] >= pos["max_hold"]:
                pnl = (pos["entry_px"] * 1.1 - pos["entry_px"]) * pos["qty"]
                equity += pnl
                trades.append({"coin": pos["coin"], "entry_date": pos["entry_date"], "exit_date": d, "pnl": pnl, "reason": "time"})
            else:
                still_open.append(pos)
        open_positions = still_open
        curve.append({"date": d, "equity": equity})
    curve_df = pd.DataFrame(curve)
    trades_df = pd.DataFrame(trades)
    print("\n--- Results ---")
    print(f"Start Equity: $100.00")
    print(f"End Equity ({END_DT.date()} UTC): ${equity:,.2f}")
    print(f"Total Trades: {len(trades_df)}")
    curve_df.to_csv("equity_curve.csv", index=False)
    trades_df.to_csv("trades.csv", index=False)
    print("Saved: equity_curve.csv, trades.csv")

if __name__ == "__main__":
    backtest()
