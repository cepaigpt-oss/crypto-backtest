from flask import Flask, jsonify
from exploders_backtest_v2 import backtest

app = Flask(__name__)

@app.route("/")
def home():
    return jsonify({"message": "Crypto Backtest API is live!"})

@app.route("/run", methods=["GET"])
def run_backtest():
    try:
        backtest()
        return jsonify({"status": "success", "message": "Backtest completed"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
