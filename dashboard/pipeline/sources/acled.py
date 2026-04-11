"""
ACLED API client (2026 OAuth-based API).

Fetches conflict events from the Armed Conflict Location & Event Data Project.
New API uses OAuth2 password grant: username + password → access_token (24h).

Register at https://acleddata.com (myACLED account).
Set environment variables:
    ACLED_USERNAME=your_email
    ACLED_PASSWORD=your_password
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import requests

from pipeline.config import CONFLICTS_DIR, ACLED_DAYS_BACK

logger = logging.getLogger(__name__)

# --- Config from environment ---
ACLED_USERNAME = os.environ.get("ACLED_USERNAME", "")
ACLED_PASSWORD = os.environ.get("ACLED_PASSWORD", "")
ACLED_TOKEN_URL = "https://acleddata.com/oauth/token"
ACLED_API_BASE = "https://acleddata.com/api/acled"

# Token cache (in-memory, valid 24h)
_token_cache = {"access_token": "", "expires_at": 0}


def get_access_token() -> str:
    """Obtain OAuth access token via password grant."""
    if not ACLED_USERNAME or not ACLED_PASSWORD:
        logger.warning(
            "ACLED credentials not configured. "
            "Set ACLED_USERNAME and ACLED_PASSWORD env vars. "
            "Register at https://acleddata.com"
        )
        return ""

    # Return cached token if still valid
    import time
    if _token_cache["access_token"] and time.time() < _token_cache["expires_at"]:
        return _token_cache["access_token"]

    logger.info("Requesting ACLED OAuth token...")
    try:
        resp = requests.post(ACLED_TOKEN_URL, data={
            "username": ACLED_USERNAME,
            "password": ACLED_PASSWORD,
            "grant_type": "password",
            "client_id": "acled",
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        token = data.get("access_token", "")
        expires_in = data.get("expires_in", 86400)  # default 24h
        _token_cache["access_token"] = token
        _token_cache["expires_at"] = time.time() + expires_in - 60  # 1min margin

        logger.info(f"ACLED token obtained, valid for {expires_in // 3600}h")
        return token
    except Exception as e:
        logger.error(f"ACLED OAuth token request failed: {e}")
        return ""


def fetch_acled_events() -> list[dict]:
    """Fetch conflict events from ACLED API. Returns list of event dicts."""
    token = get_access_token()
    if not token:
        return []

    since = (datetime.utcnow() - timedelta(days=ACLED_DAYS_BACK)).strftime("%Y-%m-%d")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    all_events = []
    page = 1

    headers = {"Authorization": f"Bearer {token}"}

    while True:
        try:
            params = {
                "_format": "json",
                "event_date": f"{since}|{today}",
                "event_date_where": "BETWEEN",
                "limit": 5000,
                "page": page,
            }
            resp = requests.get(ACLED_API_BASE, params=params,
                                headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            events = data.get("data", [])
            if not events:
                break

            all_events.extend(events)
            logger.info(f"ACLED page {page}: {len(events)} events")

            if len(events) < 5000:
                break
            page += 1
        except Exception as e:
            logger.error(f"ACLED fetch failed on page {page}: {e}")
            break

    logger.info(f"ACLED total: {len(all_events)} events since {since}")
    return all_events


def acled_to_geojson(events: list[dict]) -> dict:
    """Convert ACLED event list to GeoJSON FeatureCollection."""
    features = []
    for evt in events:
        try:
            lat = float(evt.get("latitude", 0))
            lon = float(evt.get("longitude", 0))
            if lat == 0 and lon == 0:
                continue

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat],
                },
                "properties": {
                    "id": f"acled-{evt.get('data_id', '')}",
                    "source": "acled",
                    "event_type": evt.get("event_type", ""),
                    "sub_type": evt.get("sub_event_type", ""),
                    "date": evt.get("event_date", ""),
                    "title": f"{evt.get('event_type', '')} — {evt.get('location', '')}, {evt.get('country', '')}",
                    "description": evt.get("notes", ""),
                    "fatalities": int(evt.get("fatalities", 0)),
                    "actors": [
                        a for a in [evt.get("actor1", ""), evt.get("actor2", "")] if a
                    ],
                    "country": evt.get("country", ""),
                    "region": evt.get("region", ""),
                    "links": [evt.get("source", "")] if evt.get("source") else [],
                    "confidence": 0.9,
                },
            }
            features.append(feature)
        except (ValueError, TypeError) as e:
            logger.debug(f"Skipping malformed ACLED event: {e}")

    return {"type": "FeatureCollection", "features": features}


def save_acled_raw(geojson: dict) -> Path:
    """Save ACLED GeoJSON to disk."""
    CONFLICTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CONFLICTS_DIR / "acled_latest.geojson"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)
    logger.info(f"Saved ACLED data to {out_path} ({len(geojson['features'])} features)")
    return out_path


def run():
    """Fetch, convert, and save ACLED data. Returns GeoJSON dict."""
    events = fetch_acled_events()
    if not events:
        return {"type": "FeatureCollection", "features": []}
    geojson = acled_to_geojson(events)
    save_acled_raw(geojson)
    return geojson
