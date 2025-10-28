from flask import Flask, jsonify
import threading
import subprocess
import time
from datetime import datetime

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


if __name__ == '__main__':
    # start the daily scheduler in a separate thread
    scheduler_thread = threading.Thread(target=daily_scheduler, daemon=True)
    scheduler_thread.start()

    app.run(host='0.0.0.0', port=8080)
