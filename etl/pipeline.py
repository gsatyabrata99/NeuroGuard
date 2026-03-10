"""
NeuroGuard — ETL Pipeline Orchestrator
Runs ingestion → transform → de-identify → load in sequence.
Provides logging, timing, and a summary report.

Usage:
    python etl/pipeline.py
    python etl/pipeline.py --dry-run        # runs up to transform, skips DB load
    python etl/pipeline.py --stage ingest   # run a single stage only
"""

import argparse
import logging
import time
from datetime import datetime

# =============================================================================
# LOGGING SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# =============================================================================
# STAGE RUNNER
# =============================================================================

def _timed(label: str, fn, *args, **kwargs):
    """Runs fn(*args, **kwargs), logs elapsed time, returns result."""
    logger.info(f"\n{'=' * 60}")
    logger.info(f"STAGE: {label}")
    logger.info(f"{'=' * 60}")
    start = time.time()
    result = fn(*args, **kwargs)
    elapsed = round(time.time() - start, 2)
    logger.info(f"✓ {label} completed in {elapsed}s")
    return result


def run_pipeline(dry_run: bool = False, stage: str | None = None):
    """
    Full ETL pipeline: ingest → transform → deidentify → load.

    Args:
        dry_run: If True, skip the load stage (no DB writes)
        stage:   If set, run only this stage ('ingest', 'transform', 'deident', 'load')
    """
    started_at = datetime.utcnow()
    logger.info(f"\n🧠 NeuroGuard ETL Pipeline")
    logger.info(f"   Started:  {started_at.isoformat()}")
    logger.info(f"   Dry run:  {dry_run}")
    logger.info(f"   Stage:    {stage or 'all'}\n")

    # Late imports so each module can also be run standalone
    from ingestion  import run_ingestion
    from transform  import run_transform
    from deidentify import run_deidentification
    from load       import run_load

    results = {}

    # -------------------------------------------------------------------------
    # STAGE 1: INGESTION
    # -------------------------------------------------------------------------
    if stage in (None, "ingest"):
        results["ingested"] = _timed("INGESTION", run_ingestion)

        _log_ingestion_summary(results["ingested"])

        if stage == "ingest":
            logger.info("Stage-only run — stopping after ingestion.")
            return results

    # -------------------------------------------------------------------------
    # STAGE 2: TRANSFORM
    # -------------------------------------------------------------------------
    if stage in (None, "transform"):
        if "ingested" not in results:
            logger.error("Transform requires ingestion output. Run with --stage ingest first or omit --stage.")
            return results

        results["transformed"] = _timed("TRANSFORM", run_transform, results["ingested"])
        _log_transform_summary(results["transformed"])

        if stage == "transform":
            logger.info("Stage-only run — stopping after transform.")
            return results

    # -------------------------------------------------------------------------
    # STAGE 3: DE-IDENTIFICATION
    # -------------------------------------------------------------------------
    if stage in (None, "deident"):
        if "transformed" not in results:
            logger.error("De-identification requires transform output.")
            return results

        results["deidentified"] = _timed("DE-IDENTIFICATION", run_deidentification, results["transformed"])

        phi_ok = results["deidentified"].get("phi_check_passed", False)
        if not phi_ok:
            logger.error("❌ PHI CHECK FAILED — pipeline halted. Review de-identification output.")
            return results
        logger.info("✅ PHI check passed")

        if stage == "deident":
            logger.info("Stage-only run — stopping after de-identification.")
            return results

    # -------------------------------------------------------------------------
    # STAGE 4: LOAD
    # -------------------------------------------------------------------------
    if stage in (None, "load"):
        if dry_run:
            logger.info("DRY RUN — skipping load stage")
        else:
            if "transformed" not in results or "deidentified" not in results:
                logger.error("Load requires transform and deidentify outputs.")
                return results

            results["load_stats"] = _timed(
                "LOAD",
                run_load,
                results["transformed"],
                results["deidentified"]
            )
            _log_load_summary(results["load_stats"])

    # -------------------------------------------------------------------------
    # PIPELINE SUMMARY
    # -------------------------------------------------------------------------
    finished_at = datetime.utcnow()
    total_seconds = round((finished_at - started_at).total_seconds(), 2)

    logger.info(f"\n{'=' * 60}")
    logger.info(f"PIPELINE COMPLETE")
    logger.info(f"  Total time: {total_seconds}s")
    logger.info(f"  Finished:   {finished_at.isoformat()}")
    if "load_stats" in results:
        s = results["load_stats"]
        logger.info(f"  Records loaded:")
        logger.info(f"    Patients (identified):    {s.get('patients', 0)}")
        logger.info(f"    Patients (de-identified): {s.get('deident', 0)}")
        logger.info(f"    FHIR transfers:           {s.get('fhir', 0)}")
        logger.info(f"    MRI registrations:        {s.get('mri', 0)}")
        if s.get("errors"):
            logger.warning(f"    Errors: {s['errors']}")
    logger.info(f"{'=' * 60}\n")

    return results


# =============================================================================
# SUMMARY HELPERS
# =============================================================================

def _log_ingestion_summary(ingested: dict):
    csv_rows  = len(ingested.get("patients_csv", []))
    fhir_keys = list(ingested.get("fhir", {}).keys())
    mri_rows  = len(ingested.get("mri_manifest", []))
    logger.info(f"  CSV patients:     {csv_rows} rows")
    logger.info(f"  FHIR resources:   {fhir_keys}")
    logger.info(f"  MRI manifest:     {mri_rows} entries")


def _log_transform_summary(transformed: dict):
    for key, df in transformed.items():
        if hasattr(df, "__len__"):
            logger.info(f"  {key}: {len(df)} rows")


def _log_load_summary(stats: dict):
    logger.info(f"  DB write results: {stats}")


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NeuroGuard ETL Pipeline")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run ingestion, transform, and deidentify but skip DB load"
    )
    parser.add_argument(
        "--stage",
        choices=["ingest", "transform", "deident", "load"],
        default=None,
        help="Run only a specific stage"
    )
    args = parser.parse_args()
    run_pipeline(dry_run=args.dry_run, stage=args.stage)
