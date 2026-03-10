"""
NeuroGuard — Data Governance: Audit Log
Writes and queries the immutable audit trail.
Every patient record access, MRI upload, role change, and ETL event is logged here.

Rule: audit_log rows are NEVER updated or deleted.
"""

import os
import json
import logging
from datetime import datetime, timedelta

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
# ACTION CONSTANTS
# =============================================================================

class AuditAction:
    # Patient actions
    VIEW_PATIENT        = "VIEW_PATIENT"
    CREATE_PATIENT      = "CREATE_PATIENT"
    UPDATE_PATIENT      = "UPDATE_PATIENT"
    DELETE_PATIENT      = "DELETE_PATIENT"       # soft delete only

    # MRI actions
    UPLOAD_MRI          = "UPLOAD_MRI"
    VIEW_MRI            = "VIEW_MRI"
    VIEW_GRADCAM        = "VIEW_GRADCAM"
    MRI_INFERENCE       = "MRI_INFERENCE"

    # Clinical actions
    ADD_NOTE            = "ADD_NOTE"
    CLINICIAN_OVERRIDE  = "CLINICIAN_OVERRIDE"

    # Appointment actions
    SCHEDULE_APPT       = "SCHEDULE_APPOINTMENT"
    CANCEL_APPT         = "CANCEL_APPOINTMENT"
    UPDATE_APPT         = "UPDATE_APPOINTMENT"

    # Governance actions
    VIEW_LINEAGE        = "VIEW_LINEAGE"
    RUN_QUALITY_CHECK   = "RUN_QUALITY_CHECK"
    VIEW_GOVERNANCE     = "VIEW_GOVERNANCE"
    MPI_MERGE           = "MPI_MERGE"
    EXPORT_AUDIT        = "EXPORT_AUDIT_REPORT"

    # ETL / system actions
    ETL_PATIENT_INSERT  = "ETL_PATIENT_INSERT"
    ETL_MRI_REGISTER    = "ETL_MRI_REGISTER"
    RUN_ETL             = "RUN_ETL"
    VIEW_PIPELINE       = "VIEW_PIPELINE_STATUS"

    # Auth actions
    LOGIN               = "LOGIN"
    LOGOUT              = "LOGOUT"
    LOGIN_FAILED        = "LOGIN_FAILED"
    ROLE_CHANGE         = "ROLE_CHANGE"

    # Audit actions
    VIEW_AUDIT_LOG      = "VIEW_AUDIT_LOG"


# =============================================================================
# WRITE AUDIT ENTRY
# =============================================================================

def log_event(
    action: str,
    resource_type: str,
    resource_id: str,
    user_id: int | None = None,
    token_id: str | None = None,
    phi_sensitivity: str = "identified",
    ip_address: str | None = None,
    session_id: str | None = None,
    detail: dict | None = None
) -> int:
    """
    Writes a single audit log entry.

    Args:
        action:          One of AuditAction constants
        resource_type:   Type of resource affected (e.g. 'patient', 'mri_scan')
        resource_id:     ID of the affected resource
        user_id:         ID of the user performing the action (None for system)
        token_id:        MPI token of the patient involved (if applicable)
        phi_sensitivity: 'identified', 'deidentified', or 'anonymized'
        ip_address:      Client IP
        session_id:      Session identifier
        detail:          Any additional JSON context

    Returns:
        log_id of the new audit entry
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit_log
                    (user_id, action, resource_type, resource_id,
                     token_id, phi_sensitivity, ip_address,
                     session_id, detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING log_id
            """, (
                user_id,
                action,
                resource_type,
                str(resource_id),
                token_id,
                phi_sensitivity,
                ip_address,
                session_id,
                json.dumps(detail or {})
            ))
            log_id = cur.fetchone()[0]
        conn.commit()
        logger.debug(f"Audit: [{action}] {resource_type}/{resource_id} by user {user_id}")
        return log_id
    finally:
        conn.close()


def log_login(user_id: int, success: bool, ip_address: str | None = None,
              session_id: str | None = None):
    """Convenience wrapper for login events."""
    action = AuditAction.LOGIN if success else AuditAction.LOGIN_FAILED
    log_event(
        action=action,
        resource_type="auth",
        resource_id=str(user_id),
        user_id=user_id if success else None,
        phi_sensitivity="anonymized",
        ip_address=ip_address,
        session_id=session_id,
        detail={"success": success}
    )


def log_patient_access(user_id: int, token_id: str, action: str,
                       session_id: str | None = None,
                       ip_address: str | None = None,
                       detail: dict | None = None):
    """Convenience wrapper for patient record access events."""
    log_event(
        action=action,
        resource_type="patient",
        resource_id=token_id,
        user_id=user_id,
        token_id=token_id,
        phi_sensitivity="identified",
        ip_address=ip_address,
        session_id=session_id,
        detail=detail
    )


def log_mri_event(user_id: int, scan_id: int, token_id: str, action: str,
                  session_id: str | None = None,
                  ip_address: str | None = None,
                  detail: dict | None = None):
    """Convenience wrapper for MRI-related events."""
    log_event(
        action=action,
        resource_type="mri_scan",
        resource_id=str(scan_id),
        user_id=user_id,
        token_id=token_id,
        phi_sensitivity="identified",
        ip_address=ip_address,
        session_id=session_id,
        detail=detail
    )


def log_role_change(changed_by_user_id: int, target_user_id: int,
                    old_role: str, new_role: str):
    """Logs a user role change."""
    log_event(
        action=AuditAction.ROLE_CHANGE,
        resource_type="user",
        resource_id=str(target_user_id),
        user_id=changed_by_user_id,
        phi_sensitivity="anonymized",
        detail={
            "target_user_id": target_user_id,
            "old_role": old_role,
            "new_role": new_role
        }
    )


# =============================================================================
# QUERY AUDIT LOG
# =============================================================================

def get_audit_trail(
    token_id: str | None = None,
    user_id: int | None = None,
    action: str | None = None,
    resource_type: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 100
) -> list[dict]:
    """
    Flexible audit log query supporting multiple filter combinations.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            conditions = []
            params = []

            if token_id:
                conditions.append("al.token_id = %s")
                params.append(token_id)
            if user_id:
                conditions.append("al.user_id = %s")
                params.append(user_id)
            if action:
                conditions.append("al.action = %s")
                params.append(action)
            if resource_type:
                conditions.append("al.resource_type = %s")
                params.append(resource_type)
            if since:
                conditions.append("al.logged_at >= %s")
                params.append(since)
            if until:
                conditions.append("al.logged_at <= %s")
                params.append(until)

            where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
            params.append(limit)

            cur.execute(f"""
                SELECT
                    al.log_id, al.logged_at, al.action,
                    al.resource_type, al.resource_id,
                    al.token_id, al.phi_sensitivity,
                    al.ip_address, al.detail,
                    u.username, u.role
                FROM audit_log al
                LEFT JOIN users u ON al.user_id = u.user_id
                {where}
                ORDER BY al.logged_at DESC
                LIMIT %s
            """, params)

            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_patient_audit_trail(token_id: str, limit: int = 50) -> list[dict]:
    """Returns full audit history for a specific patient token."""
    return get_audit_trail(token_id=token_id, limit=limit)


def get_user_activity(user_id: int, days: int = 30) -> list[dict]:
    """Returns audit entries for a user over the past N days."""
    since = datetime.utcnow() - timedelta(days=days)
    return get_audit_trail(user_id=user_id, since=since, limit=500)


def get_recent_phi_access(hours: int = 24, limit: int = 100) -> list[dict]:
    """
    Returns audit entries involving identified PHI access in the last N hours.
    Useful for compliance monitoring on the governance dashboard.
    """
    since = datetime.utcnow() - timedelta(hours=hours)
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    al.log_id, al.logged_at, al.action,
                    al.resource_type, al.resource_id,
                    al.token_id, al.phi_sensitivity,
                    al.ip_address, al.detail,
                    u.username, u.role
                FROM audit_log al
                LEFT JOIN users u ON al.user_id = u.user_id
                WHERE al.phi_sensitivity = 'identified'
                  AND al.logged_at >= %s
                ORDER BY al.logged_at DESC
                LIMIT %s
            """, (since, limit))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# =============================================================================
# AUDIT SUMMARY STATS
# =============================================================================

def get_audit_summary(days: int = 30) -> dict:
    """
    Returns summary stats for the audit dashboard panel.
    """
    since = datetime.utcnow() - timedelta(days=days)
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # Total events
            cur.execute("SELECT COUNT(*) AS total FROM audit_log WHERE logged_at >= %s", (since,))
            total = cur.fetchone()["total"]

            # Events by action type
            cur.execute("""
                SELECT action, COUNT(*) AS cnt
                FROM audit_log
                WHERE logged_at >= %s
                GROUP BY action
                ORDER BY cnt DESC
                LIMIT 10
            """, (since,))
            top_actions = [dict(r) for r in cur.fetchall()]

            # Events by role
            cur.execute("""
                SELECT u.role, COUNT(*) AS cnt
                FROM audit_log al
                JOIN users u ON al.user_id = u.user_id
                WHERE al.logged_at >= %s
                GROUP BY u.role
                ORDER BY cnt DESC
            """, (since,))
            by_role = [dict(r) for r in cur.fetchall()]

            # PHI access breakdown
            cur.execute("""
                SELECT phi_sensitivity, COUNT(*) AS cnt
                FROM audit_log
                WHERE logged_at >= %s
                GROUP BY phi_sensitivity
            """, (since,))
            phi_breakdown = [dict(r) for r in cur.fetchall()]

            # Failed logins
            cur.execute("""
                SELECT COUNT(*) AS cnt FROM audit_log
                WHERE action = 'LOGIN_FAILED' AND logged_at >= %s
            """, (since,))
            failed_logins = cur.fetchone()["cnt"]

            return {
                "period_days":    days,
                "total_events":   total,
                "top_actions":    top_actions,
                "events_by_role": by_role,
                "phi_breakdown":  phi_breakdown,
                "failed_logins":  failed_logins
            }
    finally:
        conn.close()


def export_audit_report(
    since: datetime | None = None,
    until: datetime | None = None,
    format: str = "json"
) -> list[dict]:
    """
    Exports the full audit trail for a given time range.
    Intended for compliance reporting (auditor role only).
    """
    since = since or (datetime.utcnow() - timedelta(days=90))
    until = until or datetime.utcnow()

    log_event(
        action=AuditAction.EXPORT_AUDIT,
        resource_type="audit_log",
        resource_id="export",
        phi_sensitivity="anonymized",
        detail={"since": since.isoformat(), "until": until.isoformat(), "format": format}
    )

    return get_audit_trail(since=since, until=until, limit=10000)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    summary = get_audit_summary(days=30)
    print(f"\nAudit Summary (last 30 days):")
    print(f"  Total events:  {summary['total_events']}")
    print(f"  Failed logins: {summary['failed_logins']}")
    print(f"  Top actions:")
    for a in summary["top_actions"][:5]:
        print(f"    {a['action']}: {a['cnt']}")
    print(f"  PHI access breakdown:")
    for p in summary["phi_breakdown"]:
        print(f"    {p['phi_sensitivity']}: {p['cnt']}")
