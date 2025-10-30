from flask import Flask, jsonify
import threading
import subprocess
import time
from datetime import datetime
import pandas as pd
import os

app = Flask(__name__)

last_run_time = None  # track last automatic run


def run_backtest_script():
    """Run exploders_backtest_v3.py as a subprocess."""
    global last_run_time
    last_run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[Scheduler] Starting backtest at {last_run_time}", flush=True)
    try:
        subprocess.run(["python", "exploders_backtest_v3.py"], check=False)
    except Exception as e:
        print(f"[Scheduler Error] {e}", flush=True)


def daily_scheduler():
    """Runs the backtest automatically every 24 hours."""
    while True:
        run_backtest_script()
        print("[Scheduler] Sleeping for 24 hours...", flush=True)
        time.sleep(24 * 3600)


@app.route('/')
def home():
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
    """Return the latest backtest results as JSON for Wix dashboard."""
    try:
        # Ensure files exist before reading
        if not os.path.exists("trades.csv") or not os.path.exists("equity_curve.csv"):
            return jsonify({
                "error": "No results found yet. Run the backtest first via /run or wait for the daily scheduler."
            }), 404

        trades_df = pd.read_csv("trades.csv")
        curve_df = pd.read_csv("equity_curve.csv")

        # Build summary
        summary = {
            "last_run_time": last_run_time,
            "total_trades": int(len(trades_df)),
            "final_equity": round(float(curve_df["equity"].iloc[-1]), 2) if not curve_df.empty else None,
            "first_trade_date": trades_df["entry_date"].iloc[0] if not trades_df.empty else None,
            "last_trade_date": trades_df["exit_date"].iloc[-1] if not trades_df.empty else None
        }

        # Return a compact version (not all rows)
        response = {
            "summary": summary,
            "equity_curve": curve_df.tail(30).to_dict(orient="records"),  # last 30 days for performance
            "recent_trades": trades_df.tail(10).to_dict(orient="records")  # last 10 trades
        }

        return jsonify(response)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    # start the daily scheduler in a separate thread
    scheduler_thread = threading.Thread(target=daily_scheduler, daemon=True)
    scheduler_thread.start()

    app.run(host='0.0.0.0', port=8080)
