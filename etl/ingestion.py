"""
NeuroGuard — ETL Ingestion Layer
Reads raw data from three sources:
  1. Synthetic patient CSVs       → data/raw/
  2. FHIR R4 JSON bundles         → data/fhir/
  3. MRI file metadata manifest   → data/mri/manifest.csv

Returns standardized DataFrames for the transform layer.
"""

import os
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
from fhir.resources.bundle import Bundle

logger = logging.getLogger(__name__)

RAW_DIR    = Path(os.getenv("RAW_DIR",    "data/raw"))
FHIR_DIR   = Path(os.getenv("FHIR_DIR",  "data/fhir"))
MRI_DIR    = Path(os.getenv("MRI_DIR",   "data/mri"))

# =============================================================================
# SOURCE 1 — Synthetic Patient CSVs
# =============================================================================

EXPECTED_PATIENT_COLS = {
    "first_name", "last_name", "date_of_birth", "gender",
    "ssn", "phone", "email",
    "address_line1", "address_city", "address_state", "address_zip",
    "blood_type", "primary_physician", "insurance_provider", "insurance_id"
}

def ingest_patient_csvs() -> pd.DataFrame:
    """
    Reads all CSV files from data/raw/ and concatenates into a single DataFrame.
    Each CSV must contain at minimum: first_name, last_name, date_of_birth, gender.
    """
    csv_files = list(RAW_DIR.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files found in {RAW_DIR}")
        return pd.DataFrame()

    frames = []
    for path in csv_files:
        try:
            df = pd.read_csv(path, dtype=str)
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            df["_source_file"] = path.name
            df["_ingested_at"] = datetime.utcnow().isoformat()
            frames.append(df)
            logger.info(f"  Ingested {len(df)} rows from {path.name}")
        except Exception as e:
            logger.error(f"  Failed to read {path.name}: {e}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"CSV ingestion complete: {len(combined)} total rows from {len(frames)} file(s)")
    return combined


# =============================================================================
# SOURCE 2 — FHIR R4 JSON Bundles
# =============================================================================

def _extract_fhir_patient(resource: dict) -> dict:
    """Flatten a FHIR Patient resource into a flat dict."""
    name = resource.get("name", [{}])[0]
    given = " ".join(name.get("given", []))
    family = name.get("family", "")

    telecom = resource.get("telecom", [])
    phone = next((t.get("value") for t in telecom if t.get("system") == "phone"), None)
    email = next((t.get("value") for t in telecom if t.get("system") == "email"), None)

    address = resource.get("address", [{}])[0]

    return {
        "fhir_resource_id":  resource.get("id"),
        "fhir_resource_type": "Patient",
        "first_name":        given,
        "last_name":         family,
        "date_of_birth":     resource.get("birthDate"),
        "gender":            resource.get("gender"),
        "phone":             phone,
        "email":             email,
        "address_line1":     " ".join(address.get("line", [])),
        "address_city":      address.get("city"),
        "address_state":     address.get("state"),
        "address_zip":       address.get("postalCode"),
    }


def _extract_fhir_diagnostic_report(resource: dict) -> dict:
    """Flatten a FHIR DiagnosticReport resource."""
    subject_ref = resource.get("subject", {}).get("reference", "")
    return {
        "fhir_resource_id":   resource.get("id"),
        "fhir_resource_type": "DiagnosticReport",
        "subject_reference":  subject_ref,
        "status":             resource.get("status"),
        "issued":             resource.get("issued"),
        "conclusion":         resource.get("conclusion"),
    }


def _extract_fhir_observation(resource: dict) -> dict:
    """Flatten a FHIR Observation resource."""
    subject_ref = resource.get("subject", {}).get("reference", "")
    value = resource.get("valueQuantity", {})
    return {
        "fhir_resource_id":   resource.get("id"),
        "fhir_resource_type": "Observation",
        "subject_reference":  subject_ref,
        "status":             resource.get("status"),
        "effective_datetime": resource.get("effectiveDateTime"),
        "value":              value.get("value"),
        "unit":               value.get("unit"),
    }


FHIR_EXTRACTORS = {
    "Patient":           _extract_fhir_patient,
    "DiagnosticReport":  _extract_fhir_diagnostic_report,
    "Observation":       _extract_fhir_observation,
}


def ingest_fhir_bundles() -> dict[str, pd.DataFrame]:
    """
    Reads all FHIR JSON bundles from data/fhir/.
    Returns a dict keyed by resource type → DataFrame.
    """
    json_files = list(FHIR_DIR.glob("*.json"))
    if not json_files:
        logger.warning(f"No FHIR JSON files found in {FHIR_DIR}")
        return {}

    records: dict[str, list] = {}

    for path in json_files:
        try:
            with open(path) as f:
                raw = json.load(f)

            # Accept both raw bundles and single resources
            entries = []
            if raw.get("resourceType") == "Bundle":
                entries = [e.get("resource", {}) for e in raw.get("entry", [])]
            elif raw.get("resourceType"):
                entries = [raw]

            for resource in entries:
                rtype = resource.get("resourceType")
                extractor = FHIR_EXTRACTORS.get(rtype)
                if extractor:
                    row = extractor(resource)
                    row["_source_file"] = path.name
                    row["_ingested_at"] = datetime.utcnow().isoformat()
                    row["_raw_bundle"]  = json.dumps(raw)
                    records.setdefault(rtype, []).append(row)

            logger.info(f"  Parsed {path.name}: {len(entries)} resource(s)")

        except Exception as e:
            logger.error(f"  Failed to parse {path.name}: {e}")

    result = {rtype: pd.DataFrame(rows) for rtype, rows in records.items()}
    for rtype, df in result.items():
        logger.info(f"FHIR ingestion — {rtype}: {len(df)} records")
    return result


# =============================================================================
# SOURCE 3 — MRI File Metadata Manifest
# =============================================================================

MRI_MANIFEST_COLS = ["file_path", "patient_ref", "scan_date", "modality", "image_format"]

def _hash_file(path: Path) -> str:
    """SHA-256 hash of file contents for integrity checking."""
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except FileNotFoundError:
        # File doesn't exist yet (placeholder path) — hash the path string instead
        return hashlib.sha256(str(path).encode()).hexdigest()


def ingest_mri_manifest() -> pd.DataFrame:
    """
    Reads data/mri/manifest.csv which maps MRI files to patient references.

    Expected columns:
        file_path     — relative path to MRI image
        patient_ref   — patient identifier (matched against MPI during transform)
        scan_date     — datetime of scan
        modality      — e.g. MRI, CT
        image_format  — e.g. JPG, DICOM

    If manifest doesn't exist, scans the MRI directory directly and builds one.
    """
    manifest_path = MRI_DIR / "manifest.csv"

    if manifest_path.exists():
        df = pd.read_csv(manifest_path, dtype=str)
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        logger.info(f"MRI manifest loaded: {len(df)} entries")
    else:
        logger.warning("manifest.csv not found — scanning MRI directory for images")
        image_files = list(MRI_DIR.rglob("*.jpg")) + list(MRI_DIR.rglob("*.png"))
        if not image_files:
            logger.warning(f"No MRI images found in {MRI_DIR}")
            return pd.DataFrame(columns=MRI_MANIFEST_COLS)

        rows = []
        for img_path in image_files:
            rows.append({
                "file_path":    str(img_path),
                "patient_ref":  None,           # unknown — will be matched in transform
                "scan_date":    datetime.utcnow().isoformat(),
                "modality":     "MRI",
                "image_format": img_path.suffix.lstrip(".").upper()
            })
        df = pd.DataFrame(rows)
        logger.info(f"Auto-generated MRI manifest: {len(df)} images found")

    df["_file_hash"]   = df["file_path"].apply(lambda p: _hash_file(Path(p)))
    df["_ingested_at"] = datetime.utcnow().isoformat()
    return df


# =============================================================================
# COMBINED INGESTION ENTRY POINT
# =============================================================================

def run_ingestion() -> dict:
    """
    Runs all three ingestion sources and returns a dict of raw DataFrames.

    Returns:
        {
            "patients_csv":   DataFrame,
            "fhir":           { "Patient": DataFrame, "DiagnosticReport": DataFrame, ... },
            "mri_manifest":   DataFrame
        }
    """
    logger.info("=" * 60)
    logger.info("INGESTION LAYER — START")
    logger.info("=" * 60)

    result = {
        "patients_csv":  ingest_patient_csvs(),
        "fhir":          ingest_fhir_bundles(),
        "mri_manifest":  ingest_mri_manifest(),
    }

    logger.info("INGESTION LAYER — COMPLETE")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    data = run_ingestion()
    print(f"\nSummary:")
    print(f"  Patient CSVs:  {len(data['patients_csv'])} rows")
    print(f"  FHIR bundles:  {list(data['fhir'].keys())}")
    print(f"  MRI manifest:  {len(data['mri_manifest'])} files")
