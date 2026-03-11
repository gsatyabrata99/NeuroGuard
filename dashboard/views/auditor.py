"""
NeuroGuard — Auditor View
Full audit trail with filtering, export, and compliance summary.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "neuroguard"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )


def show_audit_trail(user: dict):
    st.title("Audit Trail")
    st.caption("Read-only · All times UTC · Records are immutable")

    # Log the audit access itself
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit_log (user_id, action, resource_type, resource_id, phi_sensitivity, detail)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (user["user_id"], "VIEW_AUDIT_LOG", "audit_log", "dashboard", "anonymized",
                  '{"source": "dashboard"}'))
        conn.commit()
        conn.close()
    except Exception:
        pass

    tab1, tab2, tab3 = st.tabs(["📋 Event Log", "📊 Summary", "📤 Export"])

    # ── Tab 1: Event Log ──────────────────────────────────────────────────────
    with tab1:
        _show_event_log()

    # ── Tab 2: Summary ────────────────────────────────────────────────────────
    with tab2:
        _show_audit_summary()

    # ── Tab 3: Export ─────────────────────────────────────────────────────────
    with tab3:
        _show_export(user)


def _show_event_log():
    # ── Filters ───────────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        days_back = st.selectbox("Time Period", [1, 7, 14, 30, 90], index=2,
                                  format_func=lambda x: f"Last {x} days")
    with col2:
        action_filter = st.selectbox("Action", ["All",
            "VIEW_PATIENT", "UPLOAD_MRI", "VIEW_MRI", "LOGIN", "LOGIN_FAILED",
            "SCHEDULE_APPOINTMENT", "MPI_MERGE", "ETL_PATIENT_INSERT",
            "RUN_QUALITY_CHECK", "ROLE_CHANGE", "EXPORT_AUDIT_REPORT"
        ])
    with col3:
        role_filter = st.selectbox("User Role", ["All", "clinician", "admin",
                                                  "data_steward", "engineer", "auditor"])
    with col4:
        phi_filter = st.selectbox("PHI Sensitivity", ["All", "identified", "deidentified", "anonymized"])

    since = datetime.utcnow() - timedelta(days=days_back)

    conn = get_conn()
    try:
        conditions = ["al.logged_at >= %s"]
        params = [since]

        if action_filter != "All":
            conditions.append("al.action = %s")
            params.append(action_filter)
        if role_filter != "All":
            conditions.append("u.role = %s")
            params.append(role_filter)
        if phi_filter != "All":
            conditions.append("al.phi_sensitivity = %s")
            params.append(phi_filter)

        where = "WHERE " + " AND ".join(conditions)
        params.append(500)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"""
                SELECT
                    al.log_id,
                    al.logged_at,
                    COALESCE(u.username, 'system') AS username,
                    COALESCE(u.role::text, 'system') AS role,
                    al.action,
                    al.resource_type,
                    al.resource_id,
                    al.phi_sensitivity,
                    al.ip_address,
                    al.detail
                FROM audit_log al
                LEFT JOIN users u ON al.user_id = u.user_id
                {where}
                ORDER BY al.logged_at DESC
                LIMIT %s
            """, params)
            rows = cur.fetchall()

    except Exception as e:
        st.error(f"Query error: {e}")
        conn.close()
        return

    conn.close()

    if not rows:
        st.info("No events match the selected filters.")
        return

    st.caption(f"{len(rows)} event(s) · Showing most recent 500")

    df = pd.DataFrame(rows)
    df["logged_at"] = pd.to_datetime(df["logged_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Color-code phi_sensitivity
    def _phi_badge(val):
        colors = {"identified": "background-color:#3b1c1c",
                  "deidentified": "#1c2e1c",
                  "anonymized": "#1c1c2e"}
        return colors.get(val, "")

    st.dataframe(
        df.rename(columns={
            "log_id": "ID", "logged_at": "Timestamp",
            "username": "User", "role": "Role",
            "action": "Action", "resource_type": "Resource Type",
            "resource_id": "Resource ID", "phi_sensitivity": "PHI Level",
            "ip_address": "IP", "detail": "Detail"
        }),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Detail": st.column_config.TextColumn(width="medium"),
        }
    )


def _show_audit_summary():
    import plotly.express as px

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Overall counts
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE action = 'LOGIN_FAILED') AS failed_logins,
                    COUNT(*) FILTER (WHERE phi_sensitivity = 'identified') AS phi_events,
                    COUNT(DISTINCT user_id) AS unique_users
                FROM audit_log
                WHERE logged_at >= NOW() - INTERVAL '30 days'
            """)
            counts = dict(cur.fetchone())

            # Top actions
            cur.execute("""
                SELECT action, COUNT(*) AS cnt
                FROM audit_log
                WHERE logged_at >= NOW() - INTERVAL '30 days'
                GROUP BY action ORDER BY cnt DESC LIMIT 10
            """)
            top_actions = cur.fetchall()

            # Events by role over time (daily)
            cur.execute("""
                SELECT DATE(al.logged_at) AS day, u.role, COUNT(*) AS cnt
                FROM audit_log al
                JOIN users u ON al.user_id = u.user_id
                WHERE al.logged_at >= NOW() - INTERVAL '14 days'
                GROUP BY day, u.role ORDER BY day
            """)
            by_role_day = cur.fetchall()

            # PHI access by hour of day
            cur.execute("""
                SELECT EXTRACT(HOUR FROM logged_at)::int AS hour, COUNT(*) AS cnt
                FROM audit_log
                WHERE phi_sensitivity = 'identified'
                  AND logged_at >= NOW() - INTERVAL '30 days'
                GROUP BY hour ORDER BY hour
            """)
            phi_by_hour = cur.fetchall()

    except Exception as e:
        st.error(f"Summary query error: {e}")
        conn.close()
        return

    conn.close()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Events (30d)",   counts["total"])
    c2.metric("PHI Access Events",    counts["phi_events"])
    c3.metric("Failed Logins",        counts["failed_logins"],
              delta=None if not counts["failed_logins"] else "⚠️",
              delta_color="inverse")
    c4.metric("Unique Users",         counts["unique_users"])

    col1, col2 = st.columns(2)

    with col1:
        if top_actions:
            df = pd.DataFrame(top_actions)
            fig = px.bar(df, x="cnt", y="action", orientation="h",
                         title="Top 10 Actions (30 days)",
                         color_discrete_sequence=["#818cf8"],
                         labels={"cnt": "Count", "action": "Action"})
            fig.update_layout(paper_bgcolor="#1e2130", plot_bgcolor="#1e2130",
                              font_color="#e2e8f0", showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if phi_by_hour:
            df = pd.DataFrame(phi_by_hour)
            df.columns = ["Hour", "Events"]
            fig = px.bar(df, x="Hour", y="Events",
                         title="PHI Access Events by Hour of Day",
                         color_discrete_sequence=["#ff4b4b"])
            fig.update_layout(paper_bgcolor="#1e2130", plot_bgcolor="#1e2130",
                              font_color="#e2e8f0")
            st.plotly_chart(fig, use_container_width=True)

    if by_role_day:
        df = pd.DataFrame(by_role_day)
        df["day"] = pd.to_datetime(df["day"]).dt.strftime("%m-%d")
        fig = px.line(df, x="day", y="cnt", color="role",
                      title="Daily Audit Events by Role (14 days)",
                      markers=True,
                      labels={"day": "Date", "cnt": "Events", "role": "Role"})
        fig.update_layout(paper_bgcolor="#1e2130", plot_bgcolor="#1e2130", font_color="#e2e8f0")
        st.plotly_chart(fig, use_container_width=True)


def _show_export(user: dict):
    st.subheader("Export Audit Report")
    st.caption("Exports a CSV of the full audit trail for the selected period.")

    col1, col2 = st.columns(2)
    with col1:
        export_from = st.date_input("From", value=datetime.today() - timedelta(days=30))
    with col2:
        export_to = st.date_input("To", value=datetime.today())

    if st.button("Generate Export", type="primary"):
        since = datetime.combine(export_from, datetime.min.time())
        until = datetime.combine(export_to,   datetime.max.time())

        conn = get_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT al.log_id, al.logged_at, al.action,
                           al.resource_type, al.resource_id,
                           al.phi_sensitivity, al.ip_address,
                           al.detail,
                           COALESCE(u.username, 'system') AS username,
                           COALESCE(u.role::text, 'system') AS role
                    FROM audit_log al
                    LEFT JOIN users u ON al.user_id = u.user_id
                    WHERE al.logged_at BETWEEN %s AND %s
                    ORDER BY al.logged_at DESC
                """, (since, until))
                rows = cur.fetchall()

            # Log the export action
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO audit_log (user_id, action, resource_type, resource_id, phi_sensitivity, detail)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (user["user_id"], "EXPORT_AUDIT_REPORT", "audit_log", "csv_export",
                      "anonymized",
                      f'{{"from": "{since.date()}", "to": "{until.date()}", "rows": {len(rows)}}}'))
            conn.commit()

        except Exception as e:
            st.error(f"Export failed: {e}")
            conn.close()
            return

        conn.close()

        if not rows:
            st.info("No records in the selected date range.")
            return

        df = pd.DataFrame(rows)
        df["logged_at"] = pd.to_datetime(df["logged_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        csv = df.to_csv(index=False)
        st.download_button(
            label=f"⬇️ Download CSV ({len(df)} records)",
            data=csv,
            file_name=f"neuroguard_audit_{export_from}_{export_to}.csv",
            mime="text/csv",
            use_container_width=True
        )
        st.success(f"Report ready: {len(df)} records from {export_from} to {export_to}")
