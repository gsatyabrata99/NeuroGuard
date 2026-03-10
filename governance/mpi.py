"""
NeuroGuard — Data Governance: Master Patient Index (MDM/MPI)
Maintains a single patient identity across all source systems.
Handles token assignment, duplicate detection, and record merging.
"""

import os
import uuid
import hashlib
import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
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
# TOKEN MANAGEMENT
# =============================================================================

def assign_token(source_system: str = "neuroguard") -> str:
    """
    Creates a new MPI entry and returns its token_id UUID.
    Called during patient intake before any PHI is written.
    """
    token_id = str(uuid.uuid4())
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO master_patient_index (token_id, source_system)
                VALUES (%s, %s)
            """, (token_id, source_system))
        conn.commit()
        logger.info(f"MPI token assigned: {token_id} (source: {source_system})")
        return token_id
    finally:
        conn.close()


def get_token(token_id: str) -> dict | None:
    """Retrieve a single MPI record by token_id."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT token_id, source_system, is_active, merged_into,
                       mpi_created_at, mpi_updated_at
                FROM master_patient_index
                WHERE token_id = %s
            """, (token_id,))
            return cur.fetchone()
    finally:
        conn.close()


def deactivate_token(token_id: str) -> bool:
    """
    Marks an MPI token as inactive (e.g. after a merge or patient deletion request).
    Does not delete — audit trail must remain intact.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE master_patient_index
                SET is_active = FALSE, mpi_updated_at = NOW()
                WHERE token_id = %s
            """, (token_id,))
            updated = cur.rowcount
        conn.commit()
        if updated:
            logger.info(f"MPI token deactivated: {token_id}")
        return updated > 0
    finally:
        conn.close()


# =============================================================================
# DUPLICATE DETECTION
# =============================================================================

def find_potential_duplicates(
    first_name: str,
    last_name: str,
    date_of_birth: str,
    email: str | None = None
) -> list[dict]:
    """
    Searches for existing patients that may be duplicates of a new intake record.

    Matching strategy (deterministic):
      - Exact: last_name + date_of_birth                   → high confidence
      - Fuzzy: last_name + first_name initial + dob year   → medium confidence
      - Email: email match (if provided)                   → high confidence

    Returns list of potential matches with confidence scores.
    """
    conn = get_conn()
    results = []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # HIGH CONFIDENCE: exact last name + DOB
            cur.execute("""
                SELECT p.token_id, p.first_name, p.last_name,
                       p.date_of_birth, p.email, mpi.source_system
                FROM patients p
                JOIN master_patient_index mpi ON p.token_id = mpi.token_id
                WHERE LOWER(p.last_name) = LOWER(%s)
                  AND p.date_of_birth = %s::date
                  AND p.is_deleted = FALSE
                  AND mpi.is_active = TRUE
            """, (last_name, date_of_birth))
            for row in cur.fetchall():
                results.append({**dict(row), "confidence": "high", "match_rule": "last_name+dob"})

            # HIGH CONFIDENCE: email match
            if email:
                cur.execute("""
                    SELECT p.token_id, p.first_name, p.last_name,
                           p.date_of_birth, p.email, mpi.source_system
                    FROM patients p
                    JOIN master_patient_index mpi ON p.token_id = mpi.token_id
                    WHERE LOWER(p.email) = LOWER(%s)
                      AND p.is_deleted = FALSE
                      AND mpi.is_active = TRUE
                """, (email,))
                for row in cur.fetchall():
                    entry = {**dict(row), "confidence": "high", "match_rule": "email"}
                    if entry["token_id"] not in [r["token_id"] for r in results]:
                        results.append(entry)

            # MEDIUM CONFIDENCE: last name + first initial + birth year
            birth_year = str(date_of_birth)[:4]
            first_initial = first_name[0].upper() if first_name else ""
            if first_initial:
                cur.execute("""
                    SELECT p.token_id, p.first_name, p.last_name,
                           p.date_of_birth, p.email, mpi.source_system
                    FROM patients p
                    JOIN master_patient_index mpi ON p.token_id = mpi.token_id
                    WHERE LOWER(p.last_name) = LOWER(%s)
                      AND UPPER(LEFT(p.first_name, 1)) = %s
                      AND EXTRACT(YEAR FROM p.date_of_birth) = %s
                      AND p.is_deleted = FALSE
                      AND mpi.is_active = TRUE
                """, (last_name, first_initial, birth_year))
                for row in cur.fetchall():
                    token = row["token_id"]
                    if token not in [r["token_id"] for r in results]:
                        results.append({**dict(row), "confidence": "medium", "match_rule": "last_name+initial+year"})

    finally:
        conn.close()

    if results:
        logger.info(f"MPI duplicate check: {len(results)} potential match(es) found for {first_name} {last_name} {date_of_birth}")
    return results


# =============================================================================
# RECORD MERGING
# =============================================================================

def merge_tokens(source_token: str, target_token: str, merged_by_user_id: int | None = None) -> bool:
    """
    Merges source_token into target_token.

    - source_token is marked inactive and points to target_token
    - All child records (mri_scans, appointments, etc.) are re-linked to target_token
    - Audit entry is written
    - target_token remains the canonical identity

    This is a destructive operation — use only after human review confirms duplicates.
    """
    if source_token == target_token:
        logger.warning("merge_tokens: source and target are the same, skipping")
        return False

    conn = get_conn()
    try:
        with conn.cursor() as cur:

            # Verify both tokens exist and are active
            cur.execute("""
                SELECT token_id, is_active FROM master_patient_index
                WHERE token_id IN (%s, %s)
            """, (source_token, target_token))
            rows = {str(r[0]): r[1] for r in cur.fetchall()}

            if source_token not in rows:
                raise ValueError(f"Source token not found: {source_token}")
            if target_token not in rows:
                raise ValueError(f"Target token not found: {target_token}")
            if not rows[target_token]:
                raise ValueError(f"Target token is inactive: {target_token}")

            # Re-link child records to target_token
            child_tables = [
                "appointments", "medical_history", "mri_scans",
                "risk_scores", "fhir_transfers", "patients_deidentified",
                "audit_log", "data_lineage"
            ]
            for table in child_tables:
                cur.execute(f"""
                    UPDATE {table} SET token_id = %s WHERE token_id = %s
                """, (target_token, source_token))
                if cur.rowcount:
                    logger.info(f"  Re-linked {cur.rowcount} rows in {table}")

            # Soft-delete source patient record
            cur.execute("""
                UPDATE patients SET is_deleted = TRUE, updated_at = NOW()
                WHERE token_id = %s
            """, (source_token,))

            # Mark source MPI entry as inactive and point to target
            cur.execute("""
                UPDATE master_patient_index
                SET is_active = FALSE, merged_into = %s, mpi_updated_at = NOW()
                WHERE token_id = %s
            """, (target_token, source_token))

            # Audit log the merge
            cur.execute("""
                INSERT INTO audit_log
                    (user_id, action, resource_type, resource_id,
                     token_id, phi_sensitivity, detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                merged_by_user_id,
                "MPI_MERGE",
                "master_patient_index",
                source_token,
                target_token,
                "identified",
                f'{{"source_token": "{source_token}", "target_token": "{target_token}"}}'
            ))

        conn.commit()
        logger.info(f"MPI merge complete: {source_token} → {target_token}")
        return True

    except Exception as e:
        conn.rollback()
        logger.error(f"MPI merge failed: {e}")
        raise
    finally:
        conn.close()


# =============================================================================
# MPI STATS
# =============================================================================

def get_mpi_stats() -> dict:
    """Returns summary statistics for the MPI table."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_tokens,
                    SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_tokens,
                    SUM(CASE WHEN NOT is_active THEN 1 ELSE 0 END) AS inactive_tokens,
                    SUM(CASE WHEN merged_into IS NOT NULL THEN 1 ELSE 0 END) AS merged_tokens,
                    COUNT(DISTINCT source_system) AS source_systems
                FROM master_patient_index
            """)
            return dict(cur.fetchone())
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    stats = get_mpi_stats()
    print("\nMPI Stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
