"""
NeuroGuard — Natural Language Analytics
Text-to-SQL powered by Claude API.
Plugs into the Admin analytics view as a new tab.

Only queries de-identified tables — no PHI exposure.
"""

import os
import re
import json
import logging

import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# =============================================================================
# SCHEMA CONTEXT — de-identified tables only, fed to the LLM
# =============================================================================

ALLOWED_TABLES = {
    "patients_deidentified",
    "mri_scans",
    "risk_scores",
    "appointments",
    "data_quality_log",
    "data_lineage",
    "audit_log",
}

# PHI tables the LLM must never touch
BLOCKED_TABLES = {"patients", "master_patient_index", "users", "fhir_transfers"}

SCHEMA_CONTEXT = """
You are a SQL expert for NeuroGuard, a brain tumor clinic management system.
Generate READ-ONLY PostgreSQL SELECT queries against the following de-identified schema.

ALLOWED TABLES ONLY (never query patients, master_patient_index, users, or fhir_transfers):

patients_deidentified (
    deident_id      SERIAL PRIMARY KEY,
    token_id        UUID,
    age_group       VARCHAR,   -- e.g. '30-39', '40-49', '90+'
    gender          VARCHAR,   -- 'Male', 'Female', 'Non-binary', 'Prefer not to say'
    region          VARCHAR,   -- 'Southeast', 'Northeast', 'Midwest', 'Southwest', 'West', 'Mid-Atlantic'
    blood_type      VARCHAR,   -- e.g. 'A+', 'O-'
    deident_method  VARCHAR,
    deident_date    TIMESTAMP
)

mri_scans (
    scan_id          SERIAL PRIMARY KEY,
    token_id         UUID,
    scan_date        TIMESTAMP,
    modality         VARCHAR,   -- 'MRI', 'CT'
    image_format     VARCHAR,
    tumor_status     VARCHAR,   -- 'tumor', 'no_tumor', 'pending', 'inconclusive'
    confidence_score FLOAT,     -- 0.0 to 1.0
    model_version    VARCHAR,
    clinician_review VARCHAR,   -- 'confirmed', 'overridden', 'pending'
    inference_at     TIMESTAMP
)

risk_scores (
    score_id      SERIAL PRIMARY KEY,
    token_id      UUID,
    scan_id       INT,
    urgency_level VARCHAR,   -- 'low', 'medium', 'high'
    raw_score     FLOAT,     -- 0.0 to 1.0
    model_version VARCHAR,
    scored_at     TIMESTAMP
)

appointments (
    appointment_id   SERIAL PRIMARY KEY,
    token_id         UUID,
    scheduled_at     TIMESTAMP,
    appointment_type VARCHAR,   -- 'initial_consult', 'mri_scan', 'follow_up', 'treatment_review', 'discharge'
    physician        VARCHAR,
    status           VARCHAR,   -- 'scheduled', 'completed', 'no_show', 'cancelled'
    created_at       TIMESTAMP
)

data_quality_log (
    quality_id  SERIAL PRIMARY KEY,
    table_name  VARCHAR,
    check_type  VARCHAR,   -- 'completeness', 'validity', 'dedup', 'referential_integrity', 'phi_presence'
    passed      BOOLEAN,
    score       FLOAT,
    checked_at  TIMESTAMP
)

audit_log (
    log_id          BIGSERIAL PRIMARY KEY,
    action          VARCHAR,
    resource_type   VARCHAR,
    phi_sensitivity VARCHAR,   -- 'identified', 'deidentified', 'anonymized'
    logged_at       TIMESTAMP
)

RULES:
1. Only generate SELECT statements. Never INSERT, UPDATE, DELETE, DROP, or ALTER.
2. Never reference: patients, master_patient_index, users, fhir_transfers.
3. Always use table aliases for clarity.
4. Limit results to 500 rows maximum using LIMIT.
5. For date/time comparisons use NOW() and INTERVAL syntax.
6. Return ONLY the SQL query — no explanation, no markdown, no backticks.
"""

EXAMPLE_QUERIES = [
    "How many MRI scans were performed each month this year?",
    "What is the no-show rate by appointment type?",
    "Show the distribution of tumor status by age group",
    "Which regions have the highest proportion of high urgency risk scores?",
    "What is the average model confidence score for tumor vs no-tumor predictions?",
    "How many patients have been scored as high urgency in the last 30 days?",
    "Show data quality pass rates by table",
    "What is the breakdown of appointments by physician?",
]

# =============================================================================
# SQL SAFETY GUARD
# =============================================================================

def _is_safe_query(sql: str) -> tuple[bool, str]:
    """
    Validates that the generated SQL is a safe SELECT-only query
    that doesn't touch blocked tables.
    """
    sql_upper = sql.upper().strip()

    # Must start with SELECT
    if not sql_upper.startswith("SELECT"):
        return False, "Query must be a SELECT statement."

    # Block dangerous keywords
    dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
                 "TRUNCATE", "CREATE", "GRANT", "REVOKE", "EXEC"]
    for kw in dangerous:
        if re.search(rf"\b{kw}\b", sql_upper):
            return False, f"Blocked keyword detected: {kw}"

    # Block PHI tables
    for table in BLOCKED_TABLES:
        if re.search(rf"\b{table}\b", sql.lower()):
            return False, f"Access to '{table}' is not permitted."

    # Must have LIMIT
    if "LIMIT" not in sql_upper:
        return False, "Query must include a LIMIT clause."

    return True, "OK"


# =============================================================================
# LLM — TEXT TO SQL
# =============================================================================

def _call_claude(natural_language: str) -> str:
    """
    Calls Claude API to translate natural language to SQL.
    Returns raw SQL string.
    """
    import urllib.request

    payload = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1000,
        "system": SCHEMA_CONTEXT,
        "messages": [
            {"role": "user", "content": natural_language}
        ]
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
        },
        method="POST"
    )

    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read())

    # Extract text content
    for block in data.get("content", []):
        if block.get("type") == "text":
            sql = block["text"].strip()
            # Strip markdown fences if model added them anyway
            sql = re.sub(r"^```sql\s*", "", sql, flags=re.IGNORECASE)
            sql = re.sub(r"^```\s*", "", sql)
            sql = re.sub(r"\s*```$", "", sql)
            return sql.strip()

    raise ValueError("No text content returned from Claude API")


# =============================================================================
# QUERY EXECUTION
# =============================================================================

def _run_query(sql: str) -> pd.DataFrame:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "neuroguard"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    finally:
        conn.close()


# =============================================================================
# STREAMLIT UI
# =============================================================================

def show_nl_analytics(user: dict):
    st.subheader("🤖 Natural Language Analytics")
    st.caption(
        "Ask questions in plain English — Claude translates them to SQL and runs them against "
        "de-identified data. No PHI is accessible through this interface."
    )

    # ── Example queries ───────────────────────────────────────────────────────
    with st.expander("💡 Example questions you can ask"):
        for ex in EXAMPLE_QUERIES:
            if st.button(ex, key=f"ex_{ex[:20]}", use_container_width=True):
                st.session_state["nl_query_input"] = ex

    # ── Query input ───────────────────────────────────────────────────────────
    query_input = st.text_area(
        "Your question",
        value=st.session_state.get("nl_query_input", ""),
        placeholder="e.g. What is the tumor detection rate by age group over the last 6 months?",
        height=80,
        key="nl_query_text"
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        run = st.button("▶ Run Query", type="primary", use_container_width=True)
    with col2:
        st.caption("Queries run against de-identified tables only · Read-only · Max 500 rows")

    if not run or not query_input.strip():
        return

    # ── LLM translation ───────────────────────────────────────────────────────
    with st.spinner("Translating to SQL..."):
        try:
            sql = _call_claude(query_input.strip())
        except Exception as e:
            st.error(f"Claude API error: {e}")
            return

    # ── Safety check ──────────────────────────────────────────────────────────
    safe, reason = _is_safe_query(sql)

    st.markdown("**Generated SQL**")
    st.code(sql, language="sql")

    if not safe:
        st.error(f"Query blocked by safety guard: {reason}")
        _log_nl_query(user, query_input, sql, blocked=True, reason=reason)
        return

    # ── Execute ───────────────────────────────────────────────────────────────
    with st.spinner("Running query..."):
        try:
            df = _run_query(sql)
        except Exception as e:
            st.error(f"Query execution failed: {e}")
            _log_nl_query(user, query_input, sql, blocked=False, error=str(e))
            return

    # ── Results ───────────────────────────────────────────────────────────────
    if df.empty:
        st.info("Query returned no results.")
        return

    st.markdown(f"**Results** · {len(df)} row(s)")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Auto-chart if result has 2 columns and one is numeric
    if len(df.columns) == 2:
        col_names = list(df.columns)
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        if numeric_cols:
            try:
                import plotly.express as px
                x_col = [c for c in col_names if c not in numeric_cols][0]
                y_col = numeric_cols[0]
                fig = px.bar(
                    df, x=x_col, y=y_col,
                    color_discrete_sequence=["#818cf8"]
                )
                fig.update_layout(
                    paper_bgcolor="#1e2130", plot_bgcolor="#1e2130",
                    font_color="#e2e8f0"
                )
                st.plotly_chart(fig, use_container_width=True)
            except Exception:
                pass

    # CSV download
    st.download_button(
        "⬇️ Download CSV",
        data=df.to_csv(index=False),
        file_name="neuroguard_query_result.csv",
        mime="text/csv"
    )

    _log_nl_query(user, query_input, sql, blocked=False)


def _log_nl_query(user: dict, nl_query: str, sql: str,
                  blocked: bool, reason: str = "", error: str = ""):
    """Audit logs every NL query attempt."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            dbname=os.getenv("DB_NAME", "neuroguard"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres")
        )
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit_log
                    (user_id, action, resource_type, resource_id, phi_sensitivity, detail)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                user["user_id"],
                "NL_QUERY_BLOCKED" if blocked else "NL_QUERY_RUN",
                "analytics",
                "nl_sql",
                "deidentified",
                json.dumps({
                    "nl_query": nl_query[:500],
                    "sql": sql[:1000],
                    "blocked": blocked,
                    "reason": reason,
                    "error": error
                })
            ))
        conn.commit()
        conn.close()
    except Exception:
        pass
