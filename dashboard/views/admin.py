"""
NeuroGuard — Admin View
Appointment management and analytics dashboard.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import streamlit as st
import pandas as pd
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


# =============================================================================
# APPOINTMENTS VIEW
# =============================================================================

def show_appointments(user: dict):
    st.title("Appointments")

    conn = get_conn()

    # ── Summary metrics ───────────────────────────────────────────────────────
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'scheduled')   AS scheduled,
                    COUNT(*) FILTER (WHERE status = 'completed')   AS completed,
                    COUNT(*) FILTER (WHERE status = 'no_show')     AS no_shows,
                    COUNT(*) FILTER (WHERE status = 'cancelled')   AS cancelled,
                    COUNT(*) AS total
                FROM appointments
                WHERE scheduled_at >= NOW() - INTERVAL '30 days'
            """)
            metrics = dict(cur.fetchone())
    except Exception as e:
        st.error(f"Could not load metrics: {e}")
        conn.close()
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Scheduled",  metrics["scheduled"])
    c2.metric("Completed",  metrics["completed"])
    c3.metric("No-Shows",   metrics["no_shows"])
    c4.metric("Cancelled",  metrics["cancelled"])

    no_show_rate = (
        round(metrics["no_shows"] / metrics["total"] * 100, 1)
        if metrics["total"] else 0
    )
    st.caption(f"Last 30 days · No-show rate: **{no_show_rate}%**")
    st.divider()

    # ── Filters ───────────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        status_filter = st.selectbox("Status", ["All", "scheduled", "completed", "no_show", "cancelled"])
    with col2:
        type_filter = st.selectbox("Type", ["All", "initial_consult", "mri_scan", "follow_up",
                                             "treatment_review", "discharge"])
    with col3:
        physician_filter = st.selectbox("Physician", ["All",
            "Dr. Sarah Chen", "Dr. James Okafor", "Dr. Priya Nair",
            "Dr. Marcus Webb", "Dr. Elena Vasquez"])

    # ── Appointment list ──────────────────────────────────────────────────────
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT
                    a.appointment_id, a.scheduled_at, a.appointment_type,
                    a.physician, a.status, a.notes,
                    p.first_name, p.last_name, p.patient_id
                FROM appointments a
                JOIN patients p ON a.token_id = p.token_id
                WHERE p.is_deleted = FALSE
            """
            params = []

            if status_filter != "All":
                query += " AND a.status = %s"
                params.append(status_filter)
            if type_filter != "All":
                query += " AND a.appointment_type = %s"
                params.append(type_filter)
            if physician_filter != "All":
                query += " AND a.physician = %s"
                params.append(physician_filter)

            query += " ORDER BY a.scheduled_at DESC LIMIT 200"
            cur.execute(query, params)
            appointments = cur.fetchall()

    except Exception as e:
        st.error(f"Query error: {e}")
        conn.close()
        return

    conn.close()

    if not appointments:
        st.info("No appointments match the selected filters.")
        return

    st.caption(f"{len(appointments)} appointment(s)")

    df = pd.DataFrame(appointments)
    df["Patient"] = df["last_name"] + ", " + df["first_name"]
    df["Scheduled"] = pd.to_datetime(df["scheduled_at"]).dt.strftime("%Y-%m-%d %H:%M")
    df["Type"] = df["appointment_type"].str.replace("_", " ").str.title()
    df["Status"] = df["status"].str.title()

    st.dataframe(
        df[["appointment_id", "Patient", "Scheduled", "Type", "physician", "Status"]].rename(columns={
            "appointment_id": "ID", "physician": "Physician"
        }),
        use_container_width=True,
        hide_index=True
    )

    # ── Schedule new appointment ──────────────────────────────────────────────
    with st.expander("➕ Schedule New Appointment"):
        _schedule_appointment_form(user)


def _schedule_appointment_form(user: dict):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT patient_id, token_id, first_name, last_name FROM patients WHERE is_deleted = FALSE ORDER BY last_name LIMIT 500")
            patients = cur.fetchall()
    except Exception:
        st.error("Could not load patients.")
        conn.close()
        return
    conn.close()

    patient_map = {f"{p['last_name']}, {p['first_name']} (ID {p['patient_id']})": p for p in patients}

    with st.form("new_appt_form"):
        selected = st.selectbox("Patient", list(patient_map.keys()))
        col1, col2 = st.columns(2)
        with col1:
            appt_date = st.date_input("Date")
            appt_time = st.time_input("Time")
        with col2:
            appt_type = st.selectbox("Type", ["initial_consult", "mri_scan", "follow_up",
                                               "treatment_review", "discharge"])
            physician = st.selectbox("Physician", [
                "Dr. Sarah Chen", "Dr. James Okafor", "Dr. Priya Nair",
                "Dr. Marcus Webb", "Dr. Elena Vasquez"])
        notes = st.text_input("Notes (optional)")
        submitted = st.form_submit_button("Schedule", use_container_width=True)

        if submitted:
            from datetime import datetime
            patient = patient_map[selected]
            scheduled_at = datetime.combine(appt_date, appt_time)
            try:
                conn = get_conn()
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO appointments (token_id, scheduled_at, appointment_type, physician, status, notes)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (str(patient["token_id"]), scheduled_at, appt_type, physician, "scheduled", notes or None))
                    cur.execute("""
                        INSERT INTO audit_log (user_id, action, resource_type, resource_id, token_id, phi_sensitivity, detail)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (user["user_id"], "SCHEDULE_APPOINTMENT", "appointment", str(patient["patient_id"]),
                          str(patient["token_id"]), "identified", f'{{"type": "{appt_type}"}}'))
                conn.commit()
                conn.close()
                st.success(f"Appointment scheduled for {selected} on {scheduled_at.strftime('%Y-%m-%d %H:%M')}")
            except Exception as e:
                st.error(f"Failed to schedule: {e}")


# =============================================================================
# ANALYTICS VIEW
# =============================================================================

def show_analytics(user: dict):
    import plotly.express as px
    import plotly.graph_objects as go

    st.title("Analytics Dashboard")
    st.caption("De-identified data only · Analytics path")

    conn = get_conn()

    tab1, tab2, tab3, tab4 = st.tabs(["📊 Descriptive", "🔍 Diagnostic", "📈 Predictive", "🤖 Ask AI"])

    # ── Tab 1: Descriptive ────────────────────────────────────────────────────
    with tab1:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                # Appointment volume by type
                cur.execute("""
                    SELECT appointment_type, COUNT(*) AS cnt
                    FROM appointments GROUP BY appointment_type ORDER BY cnt DESC
                """)
                appt_types = cur.fetchall()

                # Appointment status breakdown
                cur.execute("""
                    SELECT status, COUNT(*) AS cnt
                    FROM appointments GROUP BY status
                """)
                appt_status = cur.fetchall()

                # Tumor detection breakdown
                cur.execute("""
                    SELECT tumor_status, COUNT(*) AS cnt
                    FROM mri_scans GROUP BY tumor_status
                """)
                tumor_dist = cur.fetchall()

                # Monthly scan volume
                cur.execute("""
                    SELECT TO_CHAR(scan_date, 'YYYY-MM') AS month, COUNT(*) AS scans
                    FROM mri_scans
                    WHERE scan_date >= NOW() - INTERVAL '12 months'
                    GROUP BY month ORDER BY month
                """)
                scan_volume = cur.fetchall()

        except Exception as e:
            st.error(f"Analytics query error: {e}")
            conn.close()
            return

        col1, col2 = st.columns(2)

        with col1:
            if appt_types:
                df = pd.DataFrame(appt_types)
                fig = px.bar(df, x="cnt", y="appointment_type", orientation="h",
                             title="Appointments by Type",
                             labels={"cnt": "Count", "appointment_type": "Type"},
                             color_discrete_sequence=["#818cf8"])
                fig.update_layout(paper_bgcolor="#1e2130", plot_bgcolor="#1e2130",
                                  font_color="#e2e8f0", showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            if tumor_dist:
                df = pd.DataFrame(tumor_dist)
                df["tumor_status"] = df["tumor_status"].fillna("unknown")
                fig = px.pie(df, names="tumor_status", values="cnt",
                             title="MRI Tumor Detection Distribution",
                             color_discrete_sequence=["#ff4b4b", "#21c55d", "#fbbf24", "#94a3b8"])
                fig.update_layout(paper_bgcolor="#1e2130", font_color="#e2e8f0")
                st.plotly_chart(fig, use_container_width=True)

        if scan_volume:
            df = pd.DataFrame(scan_volume)
            fig = px.line(df, x="month", y="scans", title="Monthly MRI Scan Volume",
                          markers=True, color_discrete_sequence=["#818cf8"])
            fig.update_layout(paper_bgcolor="#1e2130", plot_bgcolor="#1e2130", font_color="#e2e8f0")
            st.plotly_chart(fig, use_container_width=True)

    # ── Tab 2: Diagnostic ─────────────────────────────────────────────────────
    with tab2:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                # No-show rate by appointment type
                cur.execute("""
                    SELECT appointment_type,
                           ROUND(100.0 * SUM(CASE WHEN status = 'no_show' THEN 1 ELSE 0 END) / COUNT(*), 1) AS no_show_pct,
                           COUNT(*) AS total
                    FROM appointments
                    GROUP BY appointment_type
                    ORDER BY no_show_pct DESC
                """)
                no_show = cur.fetchall()

                # Confidence score distribution
                cur.execute("""
                    SELECT tumor_status, ROUND(AVG(confidence_score)::numeric, 3) AS avg_conf,
                           COUNT(*) AS cnt
                    FROM mri_scans
                    WHERE confidence_score IS NOT NULL
                    GROUP BY tumor_status
                """)
                confidence_by_status = cur.fetchall()

                # Risk score distribution
                cur.execute("""
                    SELECT urgency_level, COUNT(*) AS cnt
                    FROM risk_scores GROUP BY urgency_level
                """)
                risk_dist = cur.fetchall()

        except Exception as e:
            st.error(f"Diagnostic query error: {e}")
            conn.close()
            return

        col1, col2 = st.columns(2)

        with col1:
            if no_show:
                df = pd.DataFrame(no_show)
                fig = px.bar(df, x="appointment_type", y="no_show_pct",
                             title="No-Show Rate by Appointment Type (%)",
                             color_discrete_sequence=["#fbbf24"],
                             labels={"appointment_type": "Type", "no_show_pct": "No-Show %"})
                fig.update_layout(paper_bgcolor="#1e2130", plot_bgcolor="#1e2130", font_color="#e2e8f0")
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            if risk_dist:
                df = pd.DataFrame(risk_dist)
                fig = px.bar(df, x="urgency_level", y="cnt",
                             title="Risk Score Distribution",
                             color="urgency_level",
                             color_discrete_map={"low": "#21c55d", "medium": "#fbbf24", "high": "#ff4b4b"},
                             labels={"urgency_level": "Urgency", "cnt": "Patients"})
                fig.update_layout(paper_bgcolor="#1e2130", plot_bgcolor="#1e2130",
                                  font_color="#e2e8f0", showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        if confidence_by_status:
            df = pd.DataFrame(confidence_by_status)
            st.dataframe(
                df.rename(columns={
                    "tumor_status": "Tumor Status",
                    "avg_conf": "Avg Confidence",
                    "cnt": "Scan Count"
                }),
                use_container_width=True,
                hide_index=True
            )

    # ── Tab 3: Predictive ─────────────────────────────────────────────────────
    with tab3:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # High-risk patients pending review
                cur.execute("""
                    SELECT
                        pd.age_group, pd.gender, pd.region,
                        r.urgency_level, r.raw_score,
                        m.tumor_status, m.confidence_score,
                        r.scored_at
                    FROM risk_scores r
                    JOIN patients_deidentified pd ON r.token_id = pd.token_id
                    JOIN mri_scans m ON r.scan_id = m.scan_id
                    WHERE r.urgency_level = 'high'
                    ORDER BY r.raw_score DESC
                    LIMIT 50
                """)
                high_risk = cur.fetchall()

                # Urgency by age group (de-identified)
                cur.execute("""
                    SELECT pd.age_group, r.urgency_level, COUNT(*) AS cnt
                    FROM risk_scores r
                    JOIN patients_deidentified pd ON r.token_id = pd.token_id
                    GROUP BY pd.age_group, r.urgency_level
                    ORDER BY pd.age_group, r.urgency_level
                """)
                urgency_by_age = cur.fetchall()

        except Exception as e:
            st.error(f"Predictive query error: {e}")
            conn.close()
            return

        st.subheader("High-Risk Patients (De-identified)")
        st.caption("XGBoost urgency score > 0.70 · No PHI displayed")

        if high_risk:
            df = pd.DataFrame(high_risk)
            df["raw_score"] = df["raw_score"].round(3)
            df["confidence_score"] = df["confidence_score"].round(3)
            st.dataframe(
                df.rename(columns={
                    "age_group": "Age Group", "gender": "Gender", "region": "Region",
                    "urgency_level": "Urgency", "raw_score": "Risk Score",
                    "tumor_status": "Tumor Status", "confidence_score": "CNN Confidence",
                    "scored_at": "Scored At"
                }),
                use_container_width=True,
                hide_index=True
            )

        if urgency_by_age:
            df = pd.DataFrame(urgency_by_age)
            fig = px.bar(df, x="age_group", y="cnt", color="urgency_level",
                         title="Urgency Level by Age Group",
                         barmode="group",
                         color_discrete_map={"low": "#21c55d", "medium": "#fbbf24", "high": "#ff4b4b"},
                         labels={"age_group": "Age Group", "cnt": "Count", "urgency_level": "Urgency"})
            fig.update_layout(paper_bgcolor="#1e2130", plot_bgcolor="#1e2130", font_color="#e2e8f0")
            st.plotly_chart(fig, use_container_width=True)

    # ── Tab 4: Ask AI ─────────────────────────────────────────────────────────
    with tab4:
        from views.nl_analytics import show_nl_analytics
        show_nl_analytics(user)

    conn.close()
