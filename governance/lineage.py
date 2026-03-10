"""
NeuroGuard — Data Governance: Data Lineage Tracker
Records the origin, transformation steps, and destination of every record.
Provides queryable lineage for the governance dashboard.
"""

import os
import json
import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
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

# =============================================================================
# WRITE LINEAGE
# =============================================================================

def record_lineage(
    record_type: str,
    record_id: str,
    source_system: str,
    ingestion_method: str,
    transformations: list[str],
    destination: str,
    token_id: str | None = None,
    pipeline_version: str = PIPELINE_VERSION
) -> int:
    """
    Writes a lineage entry for a single record.

    Args:
        record_type:       e.g. 'patient', 'mri_scan', 'fhir_bundle'
        record_id:         the record's primary identifier
        source_system:     originating system (e.g. 'epic_fhir', 'csv_intake')
        ingestion_method:  how it arrived (e.g. 'etl_batch', 'fhir_transfer', 'mri_upload')
        transformations:   ordered list of transformation steps applied
        destination:       target DB table (e.g. 'patients', 'mri_scans')
        token_id:          MPI token if applicable
        pipeline_version:  ETL version tag

    Returns:
        lineage_id of the new record
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO data_lineage
                    (record_type, record_id, token_id, source_system,
                     ingestion_method, transformations, destination, pipeline_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING lineage_id
            """, (
                record_type, str(record_id), token_id,
                source_system, ingestion_method,
                json.dumps(transformations),
                destination, pipeline_version
            ))
            lineage_id = cur.fetchone()[0]
        conn.commit()
        return lineage_id
    finally:
        conn.close()


def record_lineage_batch(records: list[dict]) -> int:
    """
    Batch-inserts multiple lineage entries in a single transaction.

    Each dict in records must have keys matching record_lineage() arguments.
    Returns count of rows inserted.
    """
    if not records:
        return 0

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            rows = []
            for r in records:
                rows.append((
                    r["record_type"],
                    str(r["record_id"]),
                    r.get("token_id"),
                    r["source_system"],
                    r["ingestion_method"],
                    json.dumps(r.get("transformations", [])),
                    r["destination"],
                    r.get("pipeline_version", PIPELINE_VERSION)
                ))
            from psycopg2.extras import execute_values
            execute_values(cur, """
                INSERT INTO data_lineage
                    (record_type, record_id, token_id, source_system,
                     ingestion_method, transformations, destination, pipeline_version)
                VALUES %s
            """, rows)
        conn.commit()
        logger.info(f"Lineage batch inserted: {len(rows)} records")
        return len(rows)
    finally:
        conn.close()


# =============================================================================
# QUERY LINEAGE
# =============================================================================

def get_lineage_for_token(token_id: str) -> list[dict]:
    """
    Returns the full lineage chain for a patient token.
    Shows every record type that has been created or transformed for this patient.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT lineage_id, record_type, record_id, source_system,
                       ingestion_method, transformations, destination,
                       pipeline_version, created_at
                FROM data_lineage
                WHERE token_id = %s
                ORDER BY created_at ASC
            """, (token_id,))
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()


def get_lineage_for_record(record_type: str, record_id: str) -> list[dict]:
    """Returns lineage entries for a specific record."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM data_lineage
                WHERE record_type = %s AND record_id = %s
                ORDER BY created_at ASC
            """, (record_type, str(record_id)))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_lineage_by_source(source_system: str, limit: int = 100) -> list[dict]:
    """Returns most recent lineage entries from a given source system."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM data_lineage
                WHERE source_system = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (source_system, limit))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_lineage_summary() -> list[dict]:
    """
    Returns aggregated lineage counts grouped by source system,
    ingestion method, and destination — used for the governance dashboard.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    source_system,
                    ingestion_method,
                    destination,
                    record_type,
                    COUNT(*) AS record_count,
                    MIN(created_at) AS first_seen,
                    MAX(created_at) AS last_seen
                FROM data_lineage
                GROUP BY source_system, ingestion_method, destination, record_type
                ORDER BY last_seen DESC
            """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_pipeline_run_summary(pipeline_version: str = PIPELINE_VERSION) -> dict:
    """
    Returns a summary of records processed by a specific pipeline version.
    Useful for showing 'last ETL run' stats on the dashboard.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    pipeline_version,
                    COUNT(*) AS total_records,
                    COUNT(DISTINCT token_id) AS unique_patients,
                    COUNT(DISTINCT source_system) AS source_systems,
                    MIN(created_at) AS run_started,
                    MAX(created_at) AS run_finished
                FROM data_lineage
                WHERE pipeline_version = %s
                GROUP BY pipeline_version
            """, (pipeline_version,))
            row = cur.fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


# =============================================================================
# LINEAGE GRAPH (for visualization)
# =============================================================================

def get_lineage_graph(token_id: str) -> dict:
    """
    Returns a graph-friendly representation of a patient's lineage.
    Format: { nodes: [...], edges: [...] }
    Suitable for rendering with a JS graph library in the dashboard.
    """
    entries = get_lineage_for_token(token_id)
    if not entries:
        return {"nodes": [], "edges": []}

    nodes = []
    edges = []
    seen_nodes = set()

    for entry in entries:
        source_node = f"{entry['source_system']}"
        dest_node   = f"{entry['destination']}"
        record_node = f"{entry['record_type']}:{entry['record_id'][:8]}"

        for n in [source_node, dest_node, record_node]:
            if n not in seen_nodes:
                nodes.append({"id": n, "label": n})
                seen_nodes.add(n)

        edges.append({
            "from":   source_node,
            "to":     record_node,
            "label":  entry["ingestion_method"]
        })
        edges.append({
            "from":   record_node,
            "to":     dest_node,
            "label":  "→ loaded"
        })

    return {"nodes": nodes, "edges": edges}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    summary = get_lineage_summary()
    print(f"\nLineage Summary ({len(summary)} groups):")
    for row in summary[:10]:
        print(f"  {row['source_system']} → {row['destination']} "
              f"({row['record_type']}, {row['record_count']} records)")
    run = get_pipeline_run_summary()
    if run:
        print(f"\nLast pipeline run ({run.get('pipeline_version')}):")
        print(f"  Records: {run.get('total_records')} | "
              f"Patients: {run.get('unique_patients')} | "
              f"Sources: {run.get('source_systems')}")
