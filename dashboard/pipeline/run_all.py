#!/usr/bin/env python3
"""
Main pipeline entry point. Fetches all data sources, normalizes, and merges.

Usage:
    cd dashboard
    python3 -m pipeline.run_all

Or from project root:
    python3 dashboard/pipeline/run_all.py
"""
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure the dashboard directory is in the Python path
dashboard_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(dashboard_dir))

from pipeline.config import CONFLICTS_DIR, META_DIR, LOG_DIR
from pipeline.sources import gdelt, acled, ucdp, bellingcat, ais
from pipeline.processors.normalize import normalize_gdelt, normalize_acled, merge_conflict_sources

# --- Logging ---
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "pipeline.log"),
    ],
)
logger = logging.getLogger("pipeline")


def run_pipeline():
    """Execute the full data pipeline."""
    start = time.time()
    logger.info("=" * 60)
    logger.info("Pipeline started")

    sources_status = {}
    normalized_sources = []

    # --- GDELT (no API key required) ---
    try:
        logger.info("--- Fetching GDELT ---")
        gdelt_raw = gdelt.run()
        gdelt_normalized = normalize_gdelt(gdelt_raw)
        normalized_sources.append(gdelt_normalized)
        sources_status["gdelt"] = {
            "status": "ok",
            "features": len(gdelt_normalized.get("features", [])),
        }
    except Exception as e:
        logger.error(f"GDELT failed: {e}")
        sources_status["gdelt"] = {"status": "error", "error": str(e)}

    # --- ACLED (requires OAuth: ACLED_USERNAME + ACLED_PASSWORD env vars) ---
    try:
        logger.info("--- Fetching ACLED ---")
        acled_raw = acled.run()
        if acled_raw.get("features"):
            acled_normalized = normalize_acled(acled_raw)
            normalized_sources.append(acled_normalized)
            sources_status["acled"] = {
                "status": "ok",
                "features": len(acled_normalized.get("features", [])),
            }
        else:
            sources_status["acled"] = {
                "status": "skipped",
                "reason": "no credentials or no data",
            }
    except Exception as e:
        logger.error(f"ACLED failed: {e}")
        sources_status["acled"] = {"status": "error", "error": str(e)}

    # --- UCDP Candidate Events (academic-grade, free CSV) ---
    try:
        logger.info("--- Fetching UCDP ---")
        ucdp_raw = ucdp.run()
        if ucdp_raw.get("features"):
            normalized_sources.append(ucdp_raw)
            sources_status["ucdp"] = {
                "status": "ok",
                "features": len(ucdp_raw.get("features", [])),
            }
        else:
            sources_status["ucdp"] = {"status": "skipped", "reason": "no data"}
    except Exception as e:
        logger.error(f"UCDP failed: {e}")
        sources_status["ucdp"] = {"status": "error", "error": str(e)}

    # --- Bellingcat OSINT (via geo_extractor) ---
    try:
        logger.info("--- Fetching Bellingcat ---")
        blk_raw = bellingcat.run()
        if blk_raw.get("features"):
            normalized_sources.append(blk_raw)
            sources_status["bellingcat"] = {
                "status": "ok",
                "features": len(blk_raw.get("features", [])),
            }
        else:
            sources_status["bellingcat"] = {"status": "skipped", "reason": "no data"}
    except Exception as e:
        logger.error(f"Bellingcat failed: {e}")
        sources_status["bellingcat"] = {"status": "error", "error": str(e)}

    # --- AIS Vessel Snapshot (requires AISSTREAM_API_KEY) ---
    try:
        logger.info("--- AIS Vessel Snapshot ---")
        ais_raw = ais.run(per_region=15)
        if ais_raw.get("features"):
            sources_status["ais"] = {
                "status": "ok",
                "features": len(ais_raw.get("features", [])),
            }
        else:
            sources_status["ais"] = {"status": "skipped", "reason": "no API key or no data"}
    except Exception as e:
        logger.error(f"AIS failed: {e}")
        sources_status["ais"] = {"status": "error", "error": str(e)}

    # --- Merge all conflict sources ---
    if normalized_sources:
        merged = merge_conflict_sources(*normalized_sources)
        logger.info(f"Merged {len(merged.get('features', []))} total conflict events")
    else:
        logger.warning("No conflict data available from any source")

    # --- Write metadata ---
    META_DIR.mkdir(parents=True, exist_ok=True)
    meta = {
        "last_run": datetime.utcnow().isoformat() + "Z",
        "duration_seconds": round(time.time() - start, 1),
        "sources": sources_status,
    }
    with open(META_DIR / "last_update.json", "w") as f:
        json.dump(meta, f, indent=2)

    elapsed = round(time.time() - start, 1)
    logger.info(f"Pipeline completed in {elapsed}s")
    logger.info(f"Sources: {json.dumps(sources_status, indent=2)}")

    return meta


if __name__ == "__main__":
    run_pipeline()
