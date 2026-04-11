"""
Pipeline configuration — API keys, endpoints, constants.
"""
import os
from pathlib import Path

# --- Paths ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CONFLICTS_DIR = DATA_DIR / "conflicts"
MARITIME_DIR = DATA_DIR / "maritime"
BASES_DIR = DATA_DIR / "bases"
TRADE_DIR = DATA_DIR / "trade"
META_DIR = DATA_DIR / "_meta"
LOG_DIR = BASE_DIR / "logs"

# --- GDELT GEO 2.0 API ---
# No API key required
GDELT_GEO_ENDPOINT = "https://api.gdeltproject.org/api/v2/geo/geo"
GDELT_QUERIES = [
    "conflict OR airstrike OR shelling OR missile",
    "military clash OR battle OR firefight",
    "explosion OR bombing OR attack",
]
GDELT_MAX_RECORDS = 2500  # per query

# --- ACLED API (2026 OAuth) ---
# Register at https://acleddata.com (myACLED account)
# Set env vars: ACLED_USERNAME, ACLED_PASSWORD
# OAuth token endpoint: https://acleddata.com/oauth/token
# API base: https://acleddata.com/api/acled
ACLED_DAYS_BACK = 30  # fetch last N days

# --- Event type color mapping ---
EVENT_COLORS = {
    "Battles": "#e63946",
    "Explosions/Remote violence": "#f4a261",
    "Violence against civilians": "#9b2226",
    "Strategic developments": "#457b9d",
    "Protests": "#2a9d8f",
    "Riots": "#e9c46a",
    # GDELT
    "ASSAULT": "#e63946",
    "FIGHT": "#e63946",
    "KILL": "#9b2226",
    "COERCE": "#f4a261",
    "PROTEST": "#2a9d8f",
    "default": "#6c757d",
}
