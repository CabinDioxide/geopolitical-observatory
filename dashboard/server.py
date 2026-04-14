"""
Local Flask server for the Geopolitical Observatory dashboard.

Usage:
    cd dashboard
    python3 server.py

Then open http://localhost:5050
"""
from flask import Flask, send_from_directory, jsonify
from flask_cors import CORS
from pathlib import Path
import json

BASE_DIR = Path(__file__).parent
PUBLIC_DIR = BASE_DIR / "public"
DATA_DIR = BASE_DIR / "data"

app = Flask(__name__, static_folder=str(PUBLIC_DIR))
CORS(app)

@app.after_request
def add_no_cache(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    return response


@app.route("/")
def index():
    return send_from_directory(PUBLIC_DIR, "index.html")


@app.route("/<path:path>")
def static_files(path):
    """Serve static files from public/ first, then try data/."""
    public_path = PUBLIC_DIR / path

    # Directory request: serve index.html from it
    if public_path.is_dir():
        index_path = public_path / "index.html"
        if index_path.exists():
            return send_from_directory(public_path, "index.html")

    if public_path.exists():
        return send_from_directory(PUBLIC_DIR, path)

    # Serve data files
    if path.startswith("data/"):
        data_path = BASE_DIR / path
        if data_path.exists():
            return send_from_directory(data_path.parent, data_path.name)

    return "Not found", 404


@app.route("/data/<path:path>")
def serve_data(path):
    """Serve generated GeoJSON data files."""
    file_path = DATA_DIR / path
    if file_path.exists() and file_path.suffix in ('.geojson', '.json'):
        return send_from_directory(file_path.parent, file_path.name,
                                   mimetype='application/geo+json')
    return jsonify({"error": "not found"}), 404


@app.route("/api/status")
def status():
    """Pipeline status endpoint."""
    meta_path = DATA_DIR / "_meta" / "last_update.json"
    if meta_path.exists():
        with open(meta_path) as f:
            return jsonify(json.load(f))
    return jsonify({"status": "no data yet", "hint": "run: python3 pipeline/run_all.py"})


if __name__ == "__main__":
    print(f"\n  Geopolitical Observatory Dashboard")
    print(f"  http://localhost:5050\n")
    app.run(host="127.0.0.1", port=5050, debug=True)
