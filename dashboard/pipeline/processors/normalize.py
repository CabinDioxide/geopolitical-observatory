"""
Normalize all conflict data sources to a unified GeoJSON schema.

Common schema per feature:
{
  "type": "Feature",
  "geometry": {"type": "Point", "coordinates": [lon, lat]},
  "properties": {
    "id": str,
    "source": "acled" | "gdelt" | "geoconfirmed",
    "event_type": str,
    "sub_type": str,
    "date": "YYYY-MM-DD",
    "title": str,
    "description": str,
    "fatalities": int | null,
    "actors": [str],
    "country": str,
    "links": [str],
    "confidence": float (0-1),
  }
}
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from pipeline.config import CONFLICTS_DIR, EVENT_COLORS

logger = logging.getLogger(__name__)


def normalize_gdelt(raw_geojson: dict) -> dict:
    """GDELT data from bulk export is already in common schema. Pass through."""
    features = raw_geojson.get("features", [])
    logger.info(f"GDELT: {len(features)} features (already normalized)")
    return raw_geojson


def normalize_acled(raw_geojson: dict) -> dict:
    """ACLED data is already in common schema from acled.py. Just tag confidence."""
    for f in raw_geojson.get("features", []):
        f["properties"]["confidence"] = 0.9  # ACLED is highly curated
        f["properties"]["source"] = "acled"
    return raw_geojson


def merge_conflict_sources(*sources: dict) -> dict:
    """Merge multiple normalized GeoJSON sources into one FeatureCollection."""
    all_features = []
    for src in sources:
        all_features.extend(src.get("features", []))

    # Sort by date descending (most recent first)
    def sort_key(f):
        d = f.get("properties", {}).get("date", "")
        return d if d else "0000-00-00"

    all_features.sort(key=sort_key, reverse=True)

    merged = {
        "type": "FeatureCollection",
        "metadata": {
            "merged_at": datetime.utcnow().isoformat() + "Z",
            "total_features": len(all_features),
            "sources": list({
                f.get("properties", {}).get("source", "unknown")
                for f in all_features
            }),
        },
        "features": all_features,
    }

    # Save merged output
    CONFLICTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CONFLICTS_DIR / "merged_conflicts.geojson"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False)
    logger.info(f"Merged {len(all_features)} conflict events → {out_path}")

    return merged
