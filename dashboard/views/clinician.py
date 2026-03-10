"""
NeuroGuard — Clinician View
Full patient records, MRI upload, AI inference results, Grad-CAM display.
"""

import os
import sys
import hashlib
import logging
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

MRI_DIR = Path(os.getenv("MRI_DIR", "data/mri"))


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "neuroguard"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )


def _log_audit(user_id, action, resource_id, token_id=None, detail=None):
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit_log (user_id, action, resource_type, resource_id, token_id, phi_sensitivity, detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (user_id, action, "patient", str(resource_id), token_id, "identified",
                  str(detail or {})))
        conn.commit()
        conn.close()
    except Exception:
        pass


# =============================================================================
# PATIENT RECORDS VIEW
# =============================================================================

def show_patient_records(user: dict):
    st.title("Patient Records")

    conn = get_conn()

    # ── Search bar ────────────────────────────────────────────────────────────
    col1, col2 = st.columns([3, 1])
    with col1:
        search = st.text_input("Search by last name or patient ID", placeholder="e.g. Smith")
    with col2:
        tumor_filter = st.selectbox("Tumor Status", ["All", "tumor", "no_tumor", "pending", "inconclusive"])

    # ── Patient list ──────────────────────────────────────────────────────────
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT
                    p.patient_id, p.token_id, p.first_name, p.last_name,
                    p.date_of_birth, p.gender, p.primary_physician,
                    p.insurance_provider, p.created_at,
                    m.scan_id, m.scan_date, m.tumor_status,
                    m.confidence_score, m.gradcam_path,
                    r.urgency_level
                FROM patients p
                LEFT JOIN LATERAL (
                    SELECT * FROM mri_scans WHERE token_id = p.token_id
                    ORDER BY scan_date DESC LIMIT 1
                ) m ON TRUE
                LEFT JOIN LATERAL (
                    SELECT * FROM risk_scores WHERE token_id = p.token_id
                    ORDER BY scored_at DESC LIMIT 1
                ) r ON TRUE
                WHERE p.is_deleted = FALSE
            """
            params = []

            if search:
                query += " AND (LOWER(p.last_name) LIKE %s OR CAST(p.patient_id AS TEXT) = %s)"
                params += [f"%{search.lower()}%", search]

            if tumor_filter != "All":
                query += " AND m.tumor_status = %s"
                params.append(tumor_filter)

            query += " ORDER BY p.last_name, p.first_name LIMIT 100"
            cur.execute(query, params)
            patients = cur.fetchall()

    except Exception as e:
        st.error(f"Database error: {e}")
        conn.close()
        return

    conn.close()

    if not patients:
        st.info("No patients found.")
        return

    st.caption(f"{len(patients)} patient(s) found")

    # ── Patient table ─────────────────────────────────────────────────────────
    for p in patients:
        status = p["tumor_status"] or "—"
        urgency = p["urgency_level"] or "—"
        status_color = {
            "tumor": "🔴", "no_tumor": "🟢",
            "pending": "🟡", "inconclusive": "🟠"
        }.get(status, "⚪")
        urgency_color = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(urgency, "⚪")

        with st.expander(
            f"{status_color} {p['last_name']}, {p['first_name']} "
            f"· ID {p['patient_id']} · {urgency_color} {urgency.upper()} risk"
        ):
            _log_audit(user["user_id"], "VIEW_PATIENT", p["patient_id"], str(p["token_id"]))

            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown("**Demographics**")
                st.write(f"DOB: {p['date_of_birth']}")
                st.write(f"Gender: {p['gender'] or '—'}")
                st.write(f"Physician: {p['primary_physician'] or '—'}")
                st.write(f"Insurance: {p['insurance_provider'] or '—'}")

            with col2:
                st.markdown("**Latest MRI**")
                if p["scan_id"]:
                    st.write(f"Date: {str(p['scan_date'])[:10] if p['scan_date'] else '—'}")
                    st.write(f"Result: **{status.replace('_', ' ').title()}**")
                    if p["confidence_score"]:
                        st.progress(float(p["confidence_score"]),
                                    text=f"Confidence: {p['confidence_score']:.1%}")
                else:
                    st.write("No MRI on record")

            with col3:
                st.markdown("**Risk Score**")
                urgency_map = {"high": "🔴 High", "medium": "🟡 Medium", "low": "🟢 Low"}
                st.write(urgency_map.get(urgency, "⚪ Not scored"))

                if p["scan_id"] and p["gradcam_path"]:
                    grad_path = Path(p["gradcam_path"])
                    if grad_path.exists():
                        st.image(str(grad_path), caption="Grad-CAM Heatmap", width=200)
                    else:
                        st.caption("Grad-CAM: file not found")

            # Medical history
            st.markdown("**Medical History**")
            try:
                conn2 = get_conn()
                with conn2.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT condition, diagnosed_date, resolved_date
                        FROM medical_history WHERE token_id = %s
                        ORDER BY diagnosed_date DESC
                    """, (str(p["token_id"]),))
                    history = cur.fetchall()
                conn2.close()

                if history:
                    df_hist = pd.DataFrame(history)
                    df_hist.columns = ["Condition", "Diagnosed", "Resolved"]
                    st.dataframe(df_hist, use_container_width=True, hide_index=True)
                else:
                    st.caption("No medical history on record.")

            except Exception as e:
                st.caption(f"Could not load history: {e}")


# =============================================================================
# MRI UPLOAD VIEW
# =============================================================================

def show_mri_upload(user: dict):
    st.title("MRI Upload & AI Analysis")
    st.caption("Upload a patient MRI scan to run tumor detection and generate a Grad-CAM heatmap.")

    # ── Patient selector ──────────────────────────────────────────────────────
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT patient_id, token_id, first_name, last_name
                FROM patients WHERE is_deleted = FALSE
                ORDER BY last_name LIMIT 500
            """)
            patients = cur.fetchall()
    except Exception as e:
        st.error(f"Could not load patients: {e}")
        conn.close()
        return
    conn.close()

    patient_options = {
        f"{p['last_name']}, {p['first_name']} (ID {p['patient_id']})": p
        for p in patients
    }

    selected_label = st.selectbox("Select Patient", list(patient_options.keys()))
    selected_patient = patient_options[selected_label]

    # ── File upload ───────────────────────────────────────────────────────────
    uploaded_file = st.file_uploader(
        "Upload MRI Image", type=["jpg", "jpeg", "png"],
        help="JPG or PNG MRI scan image. Target size: 224×224 for best results."
    )

    if uploaded_file:
        col1, col2 = st.columns(2)
        with col1:
            st.image(uploaded_file, caption="Uploaded MRI Scan", use_container_width=True)

        with col2:
            st.markdown("**Scan Details**")
            scan_date = st.date_input("Scan Date", value=datetime.today())
            modality  = st.selectbox("Modality", ["MRI", "CT", "PET"])
            notes     = st.text_area("Clinical Notes", height=100)

            run_inference = st.button("🔬 Run AI Analysis", use_container_width=True, type="primary")

        if run_inference:
            with st.spinner("Running inference..."):
                result = _run_inference_pipeline(
                    uploaded_file, selected_patient, scan_date,
                    modality, notes, user
                )

            if result["success"]:
                st.success("Analysis complete.")
                _show_inference_result(result)
            else:
                st.error(f"Inference failed: {result.get('error')}")


def _run_inference_pipeline(uploaded_file, patient: dict, scan_date,
                             modality: str, notes: str, user: dict) -> dict:
    """Saves the file, registers in DB, attempts CNN inference if model available."""
    import uuid

    # Save file to MRI store
    MRI_RAW_DIR = MRI_DIR / "raw"
    MRI_RAW_DIR.mkdir(parents=True, exist_ok=True)
    scan_filename = f"scan_{uuid.uuid4().hex[:12]}.jpg"
    file_path = MRI_RAW_DIR / scan_filename

    file_bytes = uploaded_file.read()
    file_hash  = hashlib.sha256(file_bytes).hexdigest()

    with open(file_path, "wb") as f:
        f.write(file_bytes)

    # Try CNN inference
    tumor_status    = "pending"
    confidence      = None
    gradcam_path    = None
    model_version   = None

    try:
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
        from models.cnn.predict import predict_scan
        pred = predict_scan(str(file_path))
        tumor_status  = pred["tumor_status"]
        confidence    = pred["confidence"]
        gradcam_path  = pred.get("gradcam_path")
        model_version = pred.get("model_version", "vgg16_v1.0")
    except ImportError:
        pass  # Model not yet trained — store as pending
    except Exception as e:
        logger.warning(f"Inference error: {e}")

    # Register in DB
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO mri_scans (
                    token_id, file_path, file_hash, scan_date,
                    modality, image_format, tumor_status,
                    confidence_score, gradcam_path, model_version,
                    inference_at, clinician_notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING scan_id
            """, (
                str(patient["token_id"]),
                str(file_path), file_hash, scan_date,
                modality, "JPG", tumor_status,
                confidence, gradcam_path, model_version,
                datetime.utcnow() if confidence else None,
                notes or None
            ))
            scan_id = cur.fetchone()["scan_id"]

            # Audit log
            cur.execute("""
                INSERT INTO audit_log (user_id, action, resource_type, resource_id, token_id, phi_sensitivity, detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                user["user_id"], "UPLOAD_MRI", "mri_scan", str(scan_id),
                str(patient["token_id"]), "identified",
                f'{{"file": "{scan_filename}", "tumor_status": "{tumor_status}"}}'
            ))

        conn.commit()
        conn.close()

        return {
            "success":      True,
            "scan_id":      scan_id,
            "tumor_status": tumor_status,
            "confidence":   confidence,
            "gradcam_path": gradcam_path,
            "file_path":    str(file_path)
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


def _show_inference_result(result: dict):
    status = result["tumor_status"]
    confidence = result["confidence"]

    col1, col2 = st.columns(2)
    with col1:
        color = {"tumor": "🔴", "no_tumor": "🟢", "pending": "🟡"}.get(status, "⚪")
        st.metric("Tumor Detection", f"{color} {status.replace('_', ' ').title()}")
        if confidence:
            st.metric("Model Confidence", f"{confidence:.1%}")
        st.caption(f"Scan ID: {result['scan_id']}")

    with col2:
        if result.get("gradcam_path"):
            gp = Path(result["gradcam_path"])
            if gp.exists():
                st.image(str(gp), caption="Grad-CAM Activation Map", use_container_width=True)
        elif status == "pending":
            st.info("Model weights not loaded — scan registered as pending. Run inference after training the CNN.")
