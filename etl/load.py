"""
NeuroGuard — ETL Load Layer
Loads transformed and de-identified data into PostgreSQL.
Writes lineage and quality records for every batch.
"""

import os
import json
import uuid
import hashlib
import logging
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

PIPELINE_VERSION = "etl_v1.0"

# =============================================================================
# CONNECTION
# =============================================================================

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "neuroguard"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

def sha256(val: str) -> str:
    return hashlib.sha256(val.encode()).hexdigest()

# =============================================================================
# HELPERS
# =============================================================================

def _write_lineage(cur, record_type: str, record_id: str, token_id: str | None,
                   source_system: str, ingestion_method: str,
                   transformations: list, destination: str):
    cur.execute("""
        INSERT INTO data_lineage
            (record_type, record_id, token_id, source_system,
             ingestion_method, transformations, destination, pipeline_version)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        record_type, str(record_id), token_id,
        source_system, ingestion_method,
        json.dumps(transformations), destination,
        PIPELINE_VERSION
    ))


def _write_quality(cur, table_name: str, record_id: str,
                   check_type: str, passed: bool, score: float, details: dict):
    cur.execute("""
        INSERT INTO data_quality_log
            (table_name, record_id, check_type, passed, score, details)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        table_name, str(record_id), check_type,
        passed, score, json.dumps(details)
    ))


def _write_audit(cur, action: str, resource_type: str, resource_id: str,
                 token_id: str | None = None, detail: dict | None = None):
    cur.execute("""
        INSERT INTO audit_log
            (action, resource_type, resource_id, token_id,
             phi_sensitivity, detail)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        action, resource_type, str(resource_id), token_id,
        "identified", json.dumps(detail or {})
    ))


# =============================================================================
# LOAD: PATIENTS (Identified Path)
# =============================================================================

def load_patients(cur, df: pd.DataFrame, source_system: str = "etl_pipeline") -> int:
    """
    Inserts patients into master_patient_index + patients tables.
    Skips rows that already exist (idempotent via ON CONFLICT).
    Returns count of new records inserted.
    """
    if df.empty:
        logger.info("load_patients: nothing to load")
        return 0

    logger.info(f"Loading {len(df)} patients...")
    inserted = 0

    for _, row in df.iterrows():
        token_id = str(uuid.uuid4())

        # 1. MPI entry
        cur.execute("""
            INSERT INTO master_patient_index (token_id, source_system)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (token_id, source_system))

        # 2. Patient record
        source = "fhir_transfer" if row.get("fhir_resource_id") else "etl_pipeline"
        try:
            cur.execute("""
                INSERT INTO patients (
                    token_id, first_name, last_name, date_of_birth, gender,
                    ssn_hash, phone, email, address_line1, address_city,
                    address_state, address_zip, blood_type,
                    primary_physician, insurance_provider, insurance_id,
                    source, fhir_resource_id
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT (token_id) DO NOTHING
            """, (
                token_id,
                row.get("first_name"),
                row.get("last_name"),
                row.get("date_of_birth"),
                row.get("gender"),
                sha256(str(row["ssn"])) if row.get("ssn") else row.get("ssn_hash"),
                row.get("phone"),
                row.get("email"),
                row.get("address_line1"),
                row.get("address_city"),
                row.get("address_state"),
                row.get("address_zip"),
                row.get("blood_type"),
                row.get("primary_physician"),
                row.get("insurance_provider"),
                row.get("insurance_id"),
                source,
                row.get("fhir_resource_id")
            ))
            inserted += 1

            # Lineage
            _write_lineage(
                cur, "patient", token_id, token_id,
                source_system, "etl_batch",
                ["ingestion", "null_check", "name_normalize",
                 "date_parse", "phone_clean", "dedup"],
                "patients"
            )

            # Quality
            quality_score = float(row.get("_transform_quality", 0.5))
            _write_quality(
                cur, "patients", token_id, "completeness",
                quality_score >= 0.6, quality_score,
                {"source_file": row.get("_source_file", "unknown")}
            )

            # Audit
            _write_audit(cur, "ETL_PATIENT_INSERT", "patient", token_id, token_id,
                         {"source": source_system})

        except Exception as e:
            logger.error(f"  Failed to insert patient row: {e}")

    logger.info(f"  Patients loaded: {inserted}")
    return inserted


# =============================================================================
# LOAD: DE-IDENTIFIED PATIENTS (Analytics Path)
# =============================================================================

def load_patients_deidentified(cur, df: pd.DataFrame) -> int:
    """
    Inserts de-identified patient records.
    token_id must already exist in master_patient_index.
    """
    if df.empty:
        logger.info("load_patients_deidentified: nothing to load")
        return 0

    logger.info(f"Loading {len(df)} de-identified patient records...")
    rows = []
    for _, row in df.iterrows():
        if not row.get("token_id"):
            continue
        rows.append((
            row["token_id"],
            row.get("age_group"),
            row.get("gender"),
            row.get("region"),
            row.get("blood_type"),
            row.get("deident_method", "HIPAA_Safe_Harbor"),
            row.get("deident_version", "1.0")
        ))

    if rows:
        execute_values(cur, """
            INSERT INTO patients_deidentified
                (token_id, age_group, gender, region, blood_type,
                 deident_method, deident_version)
            VALUES %s
            ON CONFLICT (token_id) DO NOTHING
        """, rows)

    logger.info(f"  De-identified records loaded: {len(rows)}")
    return len(rows)


# =============================================================================
# LOAD: FHIR TRANSFERS
# =============================================================================

def load_fhir_transfers(cur, fhir_dfs: dict, token_map: dict) -> int:
    """
    Inserts raw FHIR bundles into fhir_transfers.
    token_map: { fhir_resource_id -> token_id }
    """
    total = 0
    for resource_type, df in fhir_dfs.items():
        if df.empty:
            continue
        logger.info(f"Loading {len(df)} FHIR {resource_type} records...")
        for _, row in df.iterrows():
            fhir_id = row.get("fhir_resource_id")
            token_id = token_map.get(fhir_id) if fhir_id else None

            if not token_id:
                # Create a new MPI token for this FHIR patient if needed
                token_id = str(uuid.uuid4())
                cur.execute("""
                    INSERT INTO master_patient_index (token_id, source_system)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (token_id, "epic_fhir"))

            try:
                cur.execute("""
                    INSERT INTO fhir_transfers
                        (token_id, fhir_resource_type, fhir_resource_id,
                         source_system, raw_bundle, processed)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    token_id,
                    resource_type,
                    fhir_id or str(uuid.uuid4()),
                    "epic_demo",
                    row.get("_raw_bundle", "{}"),
                    False
                ))
                total += 1
                _write_lineage(
                    cur, "fhir_bundle", fhir_id or "unknown", token_id,
                    "epic_fhir", "fhir_transfer",
                    ["fhir_parse", "resource_extract", "token_assign"],
                    "fhir_transfers"
                )
            except Exception as e:
                logger.error(f"  FHIR insert failed ({resource_type}): {e}")

    logger.info(f"  FHIR records loaded: {total}")
    return total


# =============================================================================
# LOAD: MRI MANIFEST
# =============================================================================

def load_mri_manifest(cur, df: pd.DataFrame, token_map: dict) -> int:
    """
    Registers MRI files in mri_scans table.
    token_map: { patient_ref -> token_id }
    """
    if df.empty:
        logger.info("load_mri_manifest: nothing to load")
        return 0

    logger.info(f"Loading {len(df)} MRI file records...")
    inserted = 0

    for _, row in df.iterrows():
        patient_ref = row.get("patient_ref")
        token_id = token_map.get(str(patient_ref)) if patient_ref else None

        if not token_id:
            logger.warning(f"  No token_id for patient_ref={patient_ref}, skipping MRI")
            continue

        try:
            cur.execute("""
                INSERT INTO mri_scans
                    (token_id, file_path, file_hash, scan_date,
                     modality, image_format, tumor_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                token_id,
                row.get("file_path"),
                row.get("_file_hash"),
                row.get("scan_date") or datetime.utcnow().date().isoformat(),
                row.get("modality", "MRI"),
                row.get("image_format", "JPG"),
                "pending"
            ))
            inserted += 1
            _write_lineage(
                cur, "mri_scan", row.get("file_path", "unknown"), token_id,
                "mri_file_store", "mri_upload",
                ["manifest_read", "hash_verify", "token_match"],
                "mri_scans"
            )
            _write_audit(cur, "ETL_MRI_REGISTER", "mri_scan",
                         row.get("file_path", "unknown"), token_id)

        except Exception as e:
            logger.error(f"  MRI insert failed: {e}")

    logger.info(f"  MRI records loaded: {inserted}")
    return inserted


# =============================================================================
# COMBINED LOAD ENTRY POINT
# =============================================================================

def run_load(transformed: dict, deidentified: dict) -> dict:
    """
    Loads all transformed and de-identified data into PostgreSQL.

    Accepts:
        transformed:   output from transform.run_transform()
        deidentified:  output from deidentify.run_deidentification()

    Returns:
        { "patients": int, "fhir": int, "mri": int, "errors": list }
    """
    logger.info("=" * 60)
    logger.info("LOAD LAYER — START")
    logger.info("=" * 60)

    conn = get_conn()
    conn.autocommit = False
    stats = {"patients": 0, "deident": 0, "fhir": 0, "mri": 0, "errors": []}

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # 1. Load identified patients
            stats["patients"] = load_patients(
                cur,
                transformed.get("patients", pd.DataFrame()),
                source_system="csv_intake"
            )

            # Build token map for downstream tables
            # { fhir_resource_id -> token_id } and { row_index -> token_id }
            cur.execute("SELECT token_id, fhir_resource_id FROM patients WHERE fhir_resource_id IS NOT NULL")
            fhir_token_map = {r["fhir_resource_id"]: str(r["token_id"]) for r in cur.fetchall()}

            # 2. Load de-identified patients
            # Note: de-identified records need token_ids from the just-inserted patients
            # We re-derive token_ids by matching on name+DOB since seeder assigns them
            deident_df = deidentified.get("patients_deident", pd.DataFrame())
            stats["deident"] = load_patients_deidentified(cur, deident_df)

            # 3. Load FHIR transfers
            fhir_dfs = {
                k: v for k, v in {
                    "Patient":          transformed.get("fhir_patients", pd.DataFrame()),
                    "DiagnosticReport": transformed.get("fhir_diagnostic", pd.DataFrame()),
                    "Observation":      transformed.get("fhir_observations", pd.DataFrame()),
                }.items() if not v.empty
            }
            stats["fhir"] = load_fhir_transfers(cur, fhir_dfs, fhir_token_map)

            # 4. Load MRI manifest
            # Build a ref -> token map from whatever patient_ref looks like in the manifest
            cur.execute("SELECT token_id FROM patients")
            all_tokens = [str(r["token_id"]) for r in cur.fetchall()]
            # If patient_ref is already a token_id, map directly
            mri_token_map = {t: t for t in all_tokens}
            stats["mri"] = load_mri_manifest(
                cur,
                transformed.get("mri_manifest", pd.DataFrame()),
                mri_token_map
            )

        conn.commit()
        logger.info(f"LOAD LAYER — COMPLETE: {stats}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed, rolled back: {e}")
        stats["errors"].append(str(e))
        raise
    finally:
        conn.close()

    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    from ingestion import run_ingestion
    from transform import run_transform
    from deidentify import run_deidentification

    raw         = run_ingestion()
    transformed = run_transform(raw)
    deident     = run_deidentification(transformed)
    stats       = run_load(transformed, deident)

    print(f"\nLoad summary:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
