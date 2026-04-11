"""
GDELT Events 2.0 data source.

Uses GDELT's bulk CSV exports (updated every 15 min) instead of the flaky GEO API.
Downloads the latest events export, filters for conflict-related CAMEO codes,
and converts to GeoJSON.

CAMEO root codes for conflict:
  14: Protest
  15: Exhibit force posture
  17: Coerce
  18: Assault
  19: Fight
  20: Use unconventional mass violence
"""
from __future__ import annotations

import csv
import io
import json
import logging
import zipfile
from pathlib import Path

import requests

from pipeline.config import CONFLICTS_DIR

logger = logging.getLogger(__name__)

GDELT_LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# CAMEO event root codes related to conflict/violence
CONFLICT_CAMEO_ROOTS = {"14", "15", "17", "18", "19", "20"}

# GDELT export CSV column indices (GDELT 2.0 Events format)
# See: https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/
COL_GLOBALEVENTID = 0
COL_DATE = 1       # YYYYMMDD
COL_ACTOR1NAME = 6
COL_ACTOR2NAME = 16
COL_EVENTCODE = 26
COL_EVENTBASECODE = 27
COL_EVENTROOTCODE = 28
COL_GOLDSTEIN = 30
COL_NUMMENTIONS = 31
COL_AVGTONE = 34
COL_ACTOR1GEO_LAT = 40  # Actually at 39-40 depending on version
COL_ACTOR1GEO_LONG = 41
COL_ACTION_GEO_TYPE = 51
COL_ACTION_GEO_FULLNAME = 52
COL_ACTION_GEO_COUNTRYCODE = 53
COL_ACTION_GEO_LAT = 56
COL_ACTION_GEO_LONG = 57
COL_SOURCEURL = 60

CAMEO_LABELS = {
    "14": "Protest",
    "15": "Force posture",
    "17": "Coerce",
    "18": "Assault",
    "19": "Fight",
    "20": "Mass violence",
}


def fetch_export_urls(count: int = 12) -> list[str]:
    """Get URLs of the latest N GDELT events CSV exports (~15min each, 12 = 3 hours)."""
    resp = requests.get(
        "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
        timeout=15,
    )
    resp.raise_for_status()
    urls = [
        line.split()[-1]
        for line in resp.text.strip().split("\n")
        if ".export.CSV.zip" in line
    ]
    return urls[-count:]


def download_and_parse_export(url: str) -> list[dict]:
    """Download GDELT export CSV zip, parse and filter for conflict events."""
    logger.info(f"Downloading GDELT export: {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    events = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for name in zf.namelist():
            if name.endswith(".CSV"):
                with zf.open(name) as f:
                    reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"), delimiter="\t")
                    for row in reader:
                        if len(row) < 58:
                            continue
                        try:
                            root_code = row[COL_EVENTROOTCODE].strip()
                            if root_code not in CONFLICT_CAMEO_ROOTS:
                                continue

                            lat = row[COL_ACTION_GEO_LAT].strip()
                            lon = row[COL_ACTION_GEO_LONG].strip()
                            if not lat or not lon:
                                continue

                            lat_f = float(lat)
                            lon_f = float(lon)
                            if lat_f == 0 and lon_f == 0:
                                continue

                            date_raw = row[COL_DATE].strip()
                            date_str = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}" if len(date_raw) == 8 else ""

                            goldstein = float(row[COL_GOLDSTEIN]) if row[COL_GOLDSTEIN].strip() else 0
                            tone = float(row[COL_AVGTONE]) if row[COL_AVGTONE].strip() else 0
                            mentions = int(row[COL_NUMMENTIONS]) if row[COL_NUMMENTIONS].strip() else 0

                            source_url = row[COL_SOURCEURL].strip() if len(row) > COL_SOURCEURL else ""
                            location = row[COL_ACTION_GEO_FULLNAME].strip() if len(row) > COL_ACTION_GEO_FULLNAME else ""
                            country_code = row[COL_ACTION_GEO_COUNTRYCODE].strip() if len(row) > COL_ACTION_GEO_COUNTRYCODE else ""
                            actor1 = row[COL_ACTOR1NAME].strip() if row[COL_ACTOR1NAME].strip() else ""
                            actor2 = row[COL_ACTOR2NAME].strip() if len(row) > COL_ACTOR2NAME and row[COL_ACTOR2NAME].strip() else ""

                            # --- Data quality filter: detect geocoding mismatches ---
                            # GDELT machine-translates multilingual sources, causing
                            # geolocation errors (e.g., Gwangju/Korea → Guangzhou/China)
                            skip = False

                            # Cross-check: source URL domain vs geocoded country
                            url_lower = source_url.lower()
                            geo_cc = country_code.upper()

                            # Korean source (.kr, korean media) geolocated to China
                            if any(d in url_lower for d in ['.kr/', 'korean', 'yonhap', 'chosun', 'hankyoreh', 'donga.com', 'kbs.co']) and geo_cc == 'CH':
                                skip = True
                            # Japanese source (.jp, japanese media) geolocated to China
                            if any(d in url_lower for d in ['.jp/', 'nhk.', 'asahi.', 'mainichi.', 'yomiuri.', 'nikkei.']) and geo_cc == 'CH':
                                skip = True
                            # Chinese source geolocated to Japan/Korea
                            if any(d in url_lower for d in ['xinhua', 'people.com.cn', 'chinadaily', 'globaltimes', 'cctv.com']) and geo_cc in ('JA', 'KS'):
                                skip = True
                            # Arabic source geolocated to wrong region
                            if any(d in url_lower for d in ['aljazeera', 'alarabiya']) and geo_cc in ('US', 'UK', 'GM'):
                                skip = True

                            # Geographic sanity: actors mention one country, geolocated in another continent
                            actor_text = (actor1 + ' ' + actor2).upper()
                            if 'KOREA' in actor_text and geo_cc == 'CH' and lat_f > 20 and lat_f < 30 and lon_f > 110:
                                skip = True  # Korean actor event placed in South China
                            if 'JAPAN' in actor_text and geo_cc == 'CH' and lat_f > 20 and lat_f < 35 and lon_f > 100:
                                skip = True
                            if 'CHINA' in actor_text and geo_cc in ('JA', 'KS') and lat_f > 33 and lon_f > 125:
                                skip = True

                            if skip:
                                continue

                            events.append({
                                "id": row[COL_GLOBALEVENTID].strip(),
                                "date": date_str,
                                "lat": lat_f,
                                "lon": lon_f,
                                "root_code": root_code,
                                "event_code": row[COL_EVENTCODE].strip() if len(row) > COL_EVENTCODE else "",
                                "event_type": CAMEO_LABELS.get(root_code, f"CAMEO-{root_code}"),
                                "goldstein": goldstein,
                                "tone": tone,
                                "mentions": mentions,
                                "actor1": actor1,
                                "actor2": actor2,
                                "location": location,
                                "country_code": country_code,
                                "source_url": source_url,
                            })
                        except (ValueError, IndexError):
                            continue

    logger.info(f"Parsed {len(events)} conflict events from GDELT export")
    return events


def events_to_geojson(events: list[dict]) -> dict:
    """Convert parsed GDELT events to GeoJSON FeatureCollection."""
    features = []
    for evt in events:
        actors = [a for a in [evt["actor1"], evt["actor2"]] if a]
        title = f"{evt['event_type']}"
        if evt["location"]:
            title += f" — {evt['location']}"

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [evt["lon"], evt["lat"]],
            },
            "properties": {
                "id": f"gdelt-{evt['id']}",
                "source": "gdelt",
                "event_type": evt["event_type"],
                "sub_type": f"CAMEO {evt['event_code']}",
                "date": evt["date"],
                "title": title,
                "description": "",
                "fatalities": None,
                "actors": actors,
                "country": evt["country_code"],
                "links": [evt["source_url"]] if evt["source_url"] else [],
                "confidence": min(0.3 + evt["mentions"] * 0.05, 0.9),
                "tone": evt["tone"],
                "goldstein": evt["goldstein"],
                "mentions": evt["mentions"],
            },
        }
        features.append(feature)

    return {"type": "FeatureCollection", "features": features}


def save_gdelt_raw(geojson: dict) -> Path:
    """Save GDELT GeoJSON to disk."""
    CONFLICTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CONFLICTS_DIR / "gdelt_raw.geojson"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)
    logger.info(f"Saved {len(geojson['features'])} GDELT events to {out_path}")
    return out_path


def run(export_count: int = 12) -> dict:
    """Fetch multiple GDELT exports, parse, deduplicate, and save. Returns GeoJSON."""
    urls = fetch_export_urls(count=export_count)
    logger.info(f"Fetching {len(urls)} GDELT exports (~{len(urls)*15}min of data)")

    all_events = []
    seen_ids = set()
    for url in urls:
        try:
            events = download_and_parse_export(url)
            for e in events:
                if e["id"] not in seen_ids:
                    seen_ids.add(e["id"])
                    all_events.append(e)
        except Exception as ex:
            logger.warning(f"Skipping export {url}: {ex}")

    logger.info(f"GDELT total: {len(all_events)} unique conflict events")
    geojson = events_to_geojson(all_events)
    save_gdelt_raw(geojson)
    return geojson
