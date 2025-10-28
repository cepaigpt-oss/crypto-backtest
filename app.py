from flask import Flask, jsonify
import threading
import subprocess
import time

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({"message": "Crypto Backtest API is live!"})

@app.route('/run', methods=['GET'])
def run_backtest():
    def run_script():
        subprocess.run(["python", "exploders_backtest_v3.py"], check=False)
    thread = threading.Thread(target=run_script)
    thread.start()
    return jsonify({
        "status": "Backtest started",
        "message": "exploders_backtest_v3.py is running in the background."
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
