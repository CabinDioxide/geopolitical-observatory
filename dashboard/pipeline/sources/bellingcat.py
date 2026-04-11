"""
Bellingcat OSINT data source client.

Uses the osint-geo-extractor library (geo_extractor) to fetch
geolocated conflict events verified by Bellingcat investigators.

pip install osint-geo-extractor
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from pipeline.config import CONFLICTS_DIR

logger = logging.getLogger(__name__)


def fetch_bellingcat_events() -> list:
    """Fetch Bellingcat geolocated events via geo_extractor."""
    try:
        from geo_extractor import get_bellingcat_data
    except ImportError:
        logger.error(
            "geo_extractor not installed. Run: pip install osint-geo-extractor"
        )
        return []

    try:
        events = get_bellingcat_data()
        logger.info(f"Bellingcat: fetched {len(events)} events")
        return events
    except Exception as e:
        logger.error(f"Bellingcat fetch failed: {e}")
        return []


def events_to_geojson(events: list) -> dict:
    """Convert Bellingcat datapoints to GeoJSON FeatureCollection."""
    features = []
    for evt in events:
        try:
            lat = float(evt.latitude) if evt.latitude else 0
            lon = float(evt.longitude) if evt.longitude else 0
            if lat == 0 and lon == 0:
                continue

            date_str = ""
            if evt.date:
                date_str = str(evt.date)[:10]  # YYYY-MM-DD

            desc = str(evt.description or "")
            title = desc[:120] if desc else "Bellingcat verified event"

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat],
                },
                "properties": {
                    "id": f"blk-{evt.id}" if evt.id else "",
                    "source": "bellingcat",
                    "event_type": "OSINT verified",
                    "sub_type": "",
                    "date": date_str,
                    "title": title,
                    "description": desc[:500],
                    "fatalities": None,
                    "actors": [],
                    "country": "",
                    "links": [str(evt.source)] if evt.source else [],
                    "confidence": 0.92,  # Bellingcat is investigator-verified
                },
            }
            features.append(feature)
        except Exception as e:
            logger.debug(f"Skipping malformed Bellingcat event: {e}")

    logger.info(f"Bellingcat: converted {len(features)} events to GeoJSON")
    return {"type": "FeatureCollection", "features": features}


def save_bellingcat_raw(geojson: dict) -> Path:
    """Save Bellingcat GeoJSON to disk."""
    CONFLICTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CONFLICTS_DIR / "bellingcat_latest.geojson"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)
    logger.info(f"Saved {len(geojson['features'])} Bellingcat events to {out_path}")
    return out_path


def run() -> dict:
    """Fetch, convert, and save Bellingcat data. Returns GeoJSON dict."""
    events = fetch_bellingcat_events()
    if not events:
        return {"type": "FeatureCollection", "features": []}
    geojson = events_to_geojson(events)
    save_bellingcat_raw(geojson)
    return geojson
