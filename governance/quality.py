"""
NeuroGuard — Data Governance: Data Quality Engine
Runs quality checks across core tables and writes results to data_quality_log.
Provides scoring and a summary report for the governance dashboard.
"""

import os
import json
import logging
from datetime import date, datetime

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

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

# =============================================================================
# CHECK DEFINITIONS
# =============================================================================

# (check_type, description, minimum passing threshold)
QUALITY_CHECKS = {
    "completeness":           "Required fields are non-null",
    "validity":               "Field values are within expected domains",
    "referential_integrity":  "Foreign key references resolve to active records",
    "dedup":                  "No duplicate records detected",
    "phi_presence":           "No PHI columns present in de-identified table",
    "mri_file_registered":    "MRI scan has a registered file path",
}

PATIENT_REQUIRED_FIELDS = [
    "first_name", "last_name", "date_of_birth", "gender"
]
PATIENT_SCORED_FIELDS = [
    "first_name", "last_name", "date_of_birth", "gender",
    "phone", "email", "blood_type", "insurance_provider",
    "address_city", "address_state"
]

# =============================================================================
# PATIENT QUALITY CHECKS
# =============================================================================

def check_patient_completeness(cur) -> list[dict]:
    """Checks that required fields are populated for each patient."""
    cur.execute("""
        SELECT patient_id, token_id, first_name, last_name,
               date_of_birth, gender, phone, email,
               blood_type, insurance_provider,
               address_city, address_state
        FROM patients
        WHERE is_deleted = FALSE
    """)
    patients = cur.fetchall()
    results = []

    for p in patients:
        present = sum(1 for f in PATIENT_SCORED_FIELDS if p[f] is not None)
        score   = round(present / len(PATIENT_SCORED_FIELDS), 2)
        passed  = all(p[f] is not None for f in PATIENT_REQUIRED_FIELDS)

        results.append({
            "table_name": "patients",
            "record_id":  str(p["patient_id"]),
            "check_type": "completeness",
            "passed":     passed,
            "score":      score,
            "details":    {
                "scored_fields": PATIENT_SCORED_FIELDS,
                "present":       present,
                "total":         len(PATIENT_SCORED_FIELDS)
            }
        })
    return results


def check_patient_validity(cur) -> list[dict]:
    """Validates field values against expected domains."""
    cur.execute("""
        SELECT patient_id, date_of_birth, gender, blood_type, address_state
        FROM patients WHERE is_deleted = FALSE
    """)
    patients = cur.fetchall()

    VALID_GENDERS     = {"Male", "Female", "Non-binary", "Prefer not to say"}
    VALID_BLOOD_TYPES = {"A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"}
    results = []

    for p in patients:
        issues = []

        # DOB must be in the past and reasonable
        dob = p["date_of_birth"]
        if dob:
            age = (date.today() - dob).days // 365
            if age < 0 or age > 130:
                issues.append(f"date_of_birth out of range: {dob}")
        else:
            issues.append("date_of_birth is null")

        if p["gender"] and p["gender"] not in VALID_GENDERS:
            issues.append(f"invalid gender: {p['gender']}")

        if p["blood_type"] and p["blood_type"] not in VALID_BLOOD_TYPES:
            issues.append(f"invalid blood_type: {p['blood_type']}")

        results.append({
            "table_name": "patients",
            "record_id":  str(p["patient_id"]),
            "check_type": "validity",
            "passed":     len(issues) == 0,
            "score":      1.0 if not issues else round(1 - (len(issues) / 3), 2),
            "details":    {"issues": issues}
        })

    return results


def check_patient_dedup(cur) -> list[dict]:
    """Detects duplicate patients by last_name + date_of_birth."""
    cur.execute("""
        SELECT last_name, date_of_birth,
               COUNT(*) AS cnt,
               ARRAY_AGG(patient_id) AS ids
        FROM patients
        WHERE is_deleted = FALSE
        GROUP BY last_name, date_of_birth
        HAVING COUNT(*) > 1
    """)
    dupes = cur.fetchall()
    results = []

    for d in dupes:
        for pid in d["ids"]:
            results.append({
                "table_name": "patients",
                "record_id":  str(pid),
                "check_type": "dedup",
                "passed":     False,
                "score":      0.0,
                "details":    {
                    "duplicate_key": f"{d['last_name']}|{d['date_of_birth']}",
                    "duplicate_ids": [str(i) for i in d["ids"]],
                    "count":         d["cnt"]
                }
            })

    if not dupes:
        # Write a single passing record for the table
        results.append({
            "table_name": "patients",
            "record_id":  "table_level",
            "check_type": "dedup",
            "passed":     True,
            "score":      1.0,
            "details":    {"message": "No duplicates detected"}
        })

    return results


# =============================================================================
# MRI SCAN QUALITY CHECKS
# =============================================================================

def check_mri_completeness(cur) -> list[dict]:
    """Checks that MRI scans have required file path and scan date."""
    cur.execute("""
        SELECT scan_id, file_path, scan_date, tumor_status, confidence_score
        FROM mri_scans
    """)
    scans = cur.fetchall()
    results = []

    for s in scans:
        issues = []
        if not s["file_path"]:
            issues.append("missing file_path")
        if not s["scan_date"]:
            issues.append("missing scan_date")

        score = 1.0 if not issues else 0.5
        results.append({
            "table_name": "mri_scans",
            "record_id":  str(s["scan_id"]),
            "check_type": "completeness",
            "passed":     len(issues) == 0,
            "score":      score,
            "details":    {"issues": issues}
        })

    return results


def check_mri_referential_integrity(cur) -> list[dict]:
    """Checks that every mri_scan.token_id resolves to an active MPI record."""
    cur.execute("""
        SELECT m.scan_id, m.token_id,
               mpi.token_id AS mpi_token,
               mpi.is_active
        FROM mri_scans m
        LEFT JOIN master_patient_index mpi ON m.token_id = mpi.token_id
    """)
    results = []
    for row in cur.fetchall():
        passed = row["mpi_token"] is not None and row["is_active"]
        results.append({
            "table_name": "mri_scans",
            "record_id":  str(row["scan_id"]),
            "check_type": "referential_integrity",
            "passed":     passed,
            "score":      1.0 if passed else 0.0,
            "details":    {
                "token_found":  row["mpi_token"] is not None,
                "token_active": row["is_active"] or False
            }
        })
    return results


# =============================================================================
# DE-IDENTIFIED TABLE: PHI CHECK
# =============================================================================

PHI_COLUMN_NAMES = [
    "first_name", "last_name", "ssn", "ssn_hash",
    "phone", "email", "address_line1", "address_city",
    "address_zip", "date_of_birth", "insurance_id"
]

def check_deident_phi_absence(cur) -> list[dict]:
    """
    Verifies that no PHI column names exist in patients_deidentified.
    Checks information_schema — doesn't read row data.
    """
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'patients_deidentified'
          AND table_schema = 'public'
    """)
    existing_cols = {row["column_name"] for row in cur.fetchall()}
    phi_found = [c for c in PHI_COLUMN_NAMES if c in existing_cols]

    passed = len(phi_found) == 0
    return [{
        "table_name": "patients_deidentified",
        "record_id":  "schema_level",
        "check_type": "phi_presence",
        "passed":     passed,
        "score":      1.0 if passed else 0.0,
        "details":    {
            "phi_columns_found": phi_found,
            "message": "No PHI columns detected" if passed else f"PHI DETECTED: {phi_found}"
        }
    }]


# =============================================================================
# WRITE RESULTS
# =============================================================================

def _write_results(cur, results: list[dict]):
    """Batch-inserts quality check results."""
    if not results:
        return
    rows = [(
        r["table_name"], r["record_id"], r["check_type"],
        r["passed"], r["score"], json.dumps(r["details"])
    ) for r in results]
    execute_values(cur, """
        INSERT INTO data_quality_log
            (table_name, record_id, check_type, passed, score, details)
        VALUES %s
    """, rows)


# =============================================================================
# SCORING SUMMARY
# =============================================================================

def get_quality_summary() -> dict:
    """
    Returns aggregated quality scores per table and check type.
    Used by the governance dashboard quality panel.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    table_name,
                    check_type,
                    COUNT(*) AS total_checks,
                    SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed_checks,
                    ROUND(AVG(score)::numeric, 3) AS avg_score,
                    ROUND(
                        100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END) / COUNT(*),
                        1
                    ) AS pass_rate_pct,
                    MAX(checked_at) AS last_checked
                FROM data_quality_log
                GROUP BY table_name, check_type
                ORDER BY table_name, check_type
            """)
            rows = cur.fetchall()
            return {"checks": [dict(r) for r in rows]}
    finally:
        conn.close()


def get_failing_records(table_name: str | None = None, limit: int = 50) -> list[dict]:
    """Returns the most recent failing quality check records."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if table_name:
                cur.execute("""
                    SELECT * FROM data_quality_log
                    WHERE passed = FALSE AND table_name = %s
                    ORDER BY checked_at DESC LIMIT %s
                """, (table_name, limit))
            else:
                cur.execute("""
                    SELECT * FROM data_quality_log
                    WHERE passed = FALSE
                    ORDER BY checked_at DESC LIMIT %s
                """, (limit,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_overall_health_score() -> float:
    """
    Returns a single 0–1 health score for the entire dataset.
    Computed as the weighted average pass rate across all checks.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    ROUND(
                        AVG(CASE WHEN passed THEN 1.0 ELSE 0.0 END)::numeric, 3
                    ) AS health_score
                FROM data_quality_log
            """)
            row = cur.fetchone()
            return float(row["health_score"]) if row and row["health_score"] else 0.0
    finally:
        conn.close()


# =============================================================================
# MAIN RUNNER
# =============================================================================

def run_quality_checks() -> dict:
    """
    Runs all quality checks across all tables and writes results.
    Returns a summary of results.
    """
    logger.info("=" * 60)
    logger.info("QUALITY CHECK — START")
    logger.info("=" * 60)

    conn = get_conn()
    total_checks = 0
    total_passed = 0

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            all_results = []

            # Patient checks
            logger.info("Running patient checks...")
            all_results += check_patient_completeness(cur)
            all_results += check_patient_validity(cur)
            all_results += check_patient_dedup(cur)

            # MRI checks
            logger.info("Running MRI scan checks...")
            all_results += check_mri_completeness(cur)
            all_results += check_mri_referential_integrity(cur)

            # De-identified PHI check
            logger.info("Running de-identification PHI check...")
            all_results += check_deident_phi_absence(cur)

            # Write all results
            _write_results(cur, all_results)

            total_checks = len(all_results)
            total_passed = sum(1 for r in all_results if r["passed"])

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Quality check failed: {e}")
        raise
    finally:
        conn.close()

    pass_rate = round(total_passed / total_checks, 3) if total_checks else 0
    logger.info(f"QUALITY CHECK — COMPLETE: {total_passed}/{total_checks} passed ({pass_rate*100:.1f}%)")

    return {
        "total_checks": total_checks,
        "passed":       total_passed,
        "failed":       total_checks - total_passed,
        "pass_rate":    pass_rate
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    result = run_quality_checks()
    print(f"\nQuality Check Results:")
    for k, v in result.items():
        print(f"  {k}: {v}")
    print(f"\nOverall Health Score: {get_overall_health_score()}")
