"""
UCDP Candidate Events Dataset client.

Downloads the latest monthly release of UCDP Candidate Events (v26.x).
Academic-grade conflict data with precise fatality estimates (best/low/high),
named conflict actors, and georeferenced locations.

Free, CC BY 4.0. No API key required for CSV download.
Source: https://ucdp.uu.se/downloads/
"""
from __future__ import annotations

import csv
import json
import logging
from datetime import datetime
from io import StringIO
from pathlib import Path

import requests

from pipeline.config import CONFLICTS_DIR

logger = logging.getLogger(__name__)

# UCDP download page to find latest candidate CSV
UCDP_DOWNLOADS_URL = "https://ucdp.uu.se/downloads/"
# Direct URL for latest known candidate release (updated periodically)
UCDP_CANDIDATE_URLS = [
    "https://ucdp.uu.se/downloads/candidateged/GEDEvent_v26_0_2.csv",
    "https://ucdp.uu.se/downloads/candidateged/GEDEvent_v26_0_1.csv",
]

# type_of_violence codes
VIOLENCE_TYPES = {
    "1": "State-based conflict",
    "2": "Non-state conflict",
    "3": "One-sided violence",
}


def download_candidate_csv() -> str:
    """Download the latest UCDP Candidate Events CSV. Returns CSV text."""
    for url in UCDP_CANDIDATE_URLS:
        try:
            logger.info(f"Trying UCDP Candidate: {url}")
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200 and not resp.text.strip().startswith("<!DOCTYPE"):
                logger.info(f"Downloaded UCDP Candidate from {url}")
                return resp.text
        except Exception as e:
            logger.warning(f"Failed {url}: {e}")

    raise RuntimeError("Could not download any UCDP Candidate CSV")


def parse_csv_to_events(csv_text: str) -> list[dict]:
    """Parse UCDP CSV into list of event dicts."""
    reader = csv.DictReader(StringIO(csv_text))
    events = []
    for row in reader:
        try:
            lat = row.get("latitude", "").strip()
            lon = row.get("longitude", "").strip()
            if not lat or not lon:
                continue
            lat_f, lon_f = float(lat), float(lon)
            if lat_f == 0 and lon_f == 0:
                continue

            best = int(float(row.get("best", 0) or 0))
            low = int(float(row.get("low", 0) or 0))
            high = int(float(row.get("high", 0) or 0))

            date_start = row.get("date_start", "")[:10]  # YYYY-MM-DD
            viol_type = VIOLENCE_TYPES.get(
                row.get("type_of_violence", ""), "Unknown violence"
            )

            side_a = row.get("side_a", "").strip()
            side_b = row.get("side_b", "").strip()
            conflict = row.get("conflict_name", "").strip()
            country = row.get("country", "").strip()
            region = row.get("region", "").strip()
            headline = row.get("source_headline", "").strip()

            # Build title
            title = f"{viol_type} — {country}"
            if conflict:
                title = f"{conflict} — {country}"

            events.append({
                "id": row.get("id", ""),
                "date": date_start,
                "lat": lat_f,
                "lon": lon_f,
                "event_type": viol_type,
                "conflict_name": conflict,
                "side_a": side_a,
                "side_b": side_b,
                "fatalities_best": best,
                "fatalities_low": low,
                "fatalities_high": high,
                "country": country,
                "region": region,
                "title": title,
                "headline": headline[:500] if headline else "",
            })
        except (ValueError, TypeError) as e:
            logger.debug(f"Skipping malformed UCDP row: {e}")

    logger.info(f"Parsed {len(events)} UCDP Candidate events")
    return events


def events_to_geojson(events: list[dict]) -> dict:
    """Convert parsed UCDP events to GeoJSON FeatureCollection."""
    features = []
    for evt in events:
        actors = [a for a in [evt["side_a"], evt["side_b"]] if a]

        # Construct description from headline
        desc = evt["headline"]
        if evt["fatalities_best"] > 0:
            desc = f"Fatalities: {evt['fatalities_best']} (range {evt['fatalities_low']}-{evt['fatalities_high']}). {desc}"

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [evt["lon"], evt["lat"]],
            },
            "properties": {
                "id": f"ucdp-{evt['id']}",
                "source": "ucdp",
                "event_type": evt["event_type"],
                "sub_type": evt["conflict_name"],
                "date": evt["date"],
                "title": evt["title"],
                "description": desc[:500],
                "fatalities": evt["fatalities_best"],
                "fatalities_low": evt["fatalities_low"],
                "fatalities_high": evt["fatalities_high"],
                "actors": actors,
                "country": evt["country"],
                "region": evt["region"],
                "links": [],
                "confidence": 0.95,  # UCDP is academic-grade, human-curated
            },
        }
        features.append(feature)

    return {"type": "FeatureCollection", "features": features}


def save_ucdp_raw(geojson: dict) -> Path:
    """Save UCDP GeoJSON to disk."""
    CONFLICTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CONFLICTS_DIR / "ucdp_latest.geojson"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)
    logger.info(f"Saved {len(geojson['features'])} UCDP events to {out_path}")
    return out_path


def run() -> dict:
    """Download, parse, convert and save UCDP data. Returns GeoJSON dict."""
    csv_text = download_candidate_csv()
    events = parse_csv_to_events(csv_text)
    geojson = events_to_geojson(events)
    save_ucdp_raw(geojson)
    return geojson
