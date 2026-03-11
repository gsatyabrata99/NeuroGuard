"""
NeuroGuard — Streamlit Dashboard
Main entry point. Routes to role-specific views after authentication.

Run: streamlit run dashboard/app.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
from auth import require_auth, current_user, logout

st.set_page_config(
    page_title="NeuroGuard",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stApp { background-color: #0f1117; }
    .block-container { padding-top: 1.5rem; }
    .metric-card {
        background: #1e2130;
        border-radius: 10px;
        padding: 1.2rem 1.5rem;
        border: 1px solid #2d3250;
        margin-bottom: 0.5rem;
    }
    .status-tumor    { color: #ff4b4b; font-weight: 600; }
    .status-no-tumor { color: #21c55d; font-weight: 600; }
    .status-pending  { color: #fbbf24; font-weight: 600; }
    .role-badge {
        background: #2d3250;
        color: #818cf8;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    div[data-testid="stSidebarNav"] { display: none; }
</style>
""", unsafe_allow_html=True)

# ── Authentication ────────────────────────────────────────────────────────────
require_auth()
user = current_user()
role = user.get("role", "")

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"### 🧠 NeuroGuard")
    st.markdown(f"**{user.get('full_name', user.get('username'))}**")
    st.markdown(f"<span class='role-badge'>{role.replace('_', ' ').title()}</span>",
                unsafe_allow_html=True)
    st.divider()

    ROLE_VIEWS = {
        "clinician":    ["Patient Records", "MRI Upload"],
        "admin":        ["Appointments", "Analytics"],
        "data_steward": ["Governance Panel"],
        "engineer":     ["Pipeline Status"],
        "auditor":      ["Audit Trail"],
    }

    views = ROLE_VIEWS.get(role, [])
    if views:
        selected = st.radio("Navigation", views, label_visibility="collapsed")
    else:
        selected = None
        st.warning("No views configured for this role.")

    st.divider()
    if st.button("🚪 Logout", use_container_width=True):
        logout()

# ── Route to views ────────────────────────────────────────────────────────────
if role == "clinician":
    if selected == "Patient Records":
        from views.clinician import show_patient_records
        show_patient_records(user)
    elif selected == "MRI Upload":
        from views.clinician import show_mri_upload
        show_mri_upload(user)

elif role == "admin":
    if selected == "Appointments":
        from views.admin import show_appointments
        show_appointments(user)
    elif selected == "Analytics":
        from views.admin import show_analytics
        show_analytics(user)

elif role == "data_steward":
    from views.steward import show_governance
    show_governance(user)

elif role == "engineer":
    from views.steward import show_pipeline_status
    show_pipeline_status(user)

elif role == "auditor":
    from views.auditor import show_audit_trail
    show_audit_trail(user)
