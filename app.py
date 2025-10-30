# app.py
# Flask-based Crypto Backtest API with daily scheduler and safe JSON endpoints
# -------------------------------------------------------------

from flask import Flask, jsonify, send_from_directory
import threading
import subprocess
import time
from datetime import datetime
import pandas as pd
import os
from zoneinfo import ZoneInfo  # ✅ Handles automatic ACST/ACDT switching

app = Flask(__name__)

last_run_time = None  # track last automatic run time


# -------------------- Backtest Execution --------------------
def run_backtest_script():
    """Run exploders_backtest_v3.py as a subprocess."""
    global last_run_time

    # 🕓 Get UTC time and convert to Adelaide local time (auto ACST/ACDT)
    now_utc = datetime.now(tz=ZoneInfo("UTC"))
    now_adl = now_utc.astimezone(ZoneInfo("Australia/Adelaide"))
    last_run_time = now_adl.strftime("%Y-%m-%d %H:%M:%S %Z")

    print(f"[Scheduler] Starting backtest at {last_run_time}", flush=True)
    try:
        subprocess.run(["python", "exploders_backtest_v3.py"], check=False)
        print("[Scheduler] Backtest completed.", flush=True)
    except Exception as e:
        print(f"[Scheduler Error] {e}", flush=True)


# -------------------- Daily Scheduler --------------------
def daily_scheduler():
    """Automatically run the backtest every 24 hours."""
    while True:
        run_backtest_script()
        print("[Scheduler] Sleeping for 24 hours...", flush=True)
        time.sleep(24 * 3600)


# -------------------- Helper Function --------------------
def safe_read_csv(filename):
    """Safely read a CSV — return None if file empty or unreadable."""
    try:
        if not os.path.exists(filename) or os.path.getsize(filename) == 0:
            return None
        with open(filename, "r", encoding="utf-8") as f:
            head = f.read(100).strip()
            if not head:
                return None
        return pd.read_csv(filename)
    except pd.errors.EmptyDataError:
        return None
    except Exception as e:
        print(f"[Warning] Could not read {filename}: {e}", flush=True)
        return None


# -------------------- Flask Routes --------------------
@app.route('/')
def home():
    """Base endpoint for health check."""
    return jsonify({
        "message": "Crypto Backtest API is live!",
        "last_auto_run": last_run_time
    })


@app.route('/run', methods=['GET'])
def run_backtest_manual():
    """Manual trigger for backtest via /run endpoint."""
    thread = threading.Thread(target=run_backtest_script)
    thread.start()
    return jsonify({
        "status": "Backtest started",
        "message": "exploders_backtest_v3.py is running in the background."
    })


@app.route('/results', methods=['GET'])
def get_results():
    """Return latest backtest results as JSON for Wix dashboard."""
    try:
        curve_df = safe_read_csv("equity_curve.csv")
        trades_df = safe_read_csv("trades.csv")

        if curve_df is None:
            return jsonify({"error": "equity_curve.csv not found or empty. Run the backtest first."}), 404

        if trades_df is None or trades_df.empty:
            summary = {
                "last_run_time": last_run_time,
                "total_trades": 0,
                "final_equity": round(float(curve_df["equity"].iloc[-1]), 2) if not curve_df.empty else None,
                "note": "No trades were generated in this backtest run."
            }

            return jsonify({
                "summary": summary,
                "equity_curve": curve_df.to_dict(orient="records"),
                "recent_trades": []
            })

        summary = {
            "last_run_time": last_run_time,
            "total_trades": int(len(trades_df)),
            "final_equity": round(float(curve_df["equity"].iloc[-1]), 2) if not curve_df.empty else None,
            "first_trade_date": trades_df["entry_date"].iloc[0] if "entry_date" in trades_df.columns else None,
            "last_trade_date": trades_df["exit_date"].iloc[-1] if "exit_date" in trades_df.columns else None
        }

        return jsonify({
            "summary": summary,
            "equity_curve": curve_df.tail(30).to_dict(orient="records"),
            "recent_trades": trades_df.tail(10).to_dict(orient="records")
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/chart.html')
def serve_chart():
    """Serve the standalone chart HTML for Wix embedding."""
    return send_from_directory('static', 'chart.html')


# -------------------- Main Entry Point --------------------
if __name__ == '__main__':
    scheduler_thread = threading.Thread(target=daily_scheduler, daemon=True)
    scheduler_thread.start()
    app.run(host='0.0.0.0', port=8080)
