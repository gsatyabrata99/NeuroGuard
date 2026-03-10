"""
NeuroGuard — Dashboard: RBAC Session Management
Handles login, session state, and role-based access control for Streamlit.
"""

import os
import hashlib
import logging

import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

ROLE_PERMISSIONS = {
    "clinician": {
        "view_patient_phi":    True,
        "view_mri":            True,
        "upload_mri":          True,
        "add_note":            True,
        "view_analytics":      False,
        "view_governance":     False,
        "view_audit":          False,
        "manage_appointments": False,
    },
    "admin": {
        "view_patient_phi":    False,
        "view_mri":            False,
        "upload_mri":          False,
        "add_note":            False,
        "view_analytics":      True,
        "view_governance":     False,
        "view_audit":          False,
        "manage_appointments": True,
    },
    "data_steward": {
        "view_patient_phi":    False,
        "view_mri":            False,
        "upload_mri":          False,
        "add_note":            False,
        "view_analytics":      True,
        "view_governance":     True,
        "view_audit":          False,
        "manage_appointments": False,
    },
    "engineer": {
        "view_patient_phi":    False,
        "view_mri":            False,
        "upload_mri":          False,
        "add_note":            False,
        "view_analytics":      False,
        "view_governance":     False,
        "view_audit":          True,
        "manage_appointments": False,
    },
    "auditor": {
        "view_patient_phi":    False,
        "view_mri":            False,
        "upload_mri":          False,
        "add_note":            False,
        "view_analytics":      True,
        "view_governance":     True,
        "view_audit":          True,
        "manage_appointments": False,
    },
}

ROLE_COLORS = {
    "clinician":    "#2ECC71",
    "admin":        "#3498DB",
    "data_steward": "#9B59B6",
    "engineer":     "#E67E22",
    "auditor":      "#E74C3C",
}


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


def authenticate(username: str, password: str) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT user_id, username, role, full_name, email, is_active
                FROM users
                WHERE username = %s AND password_hash = %s AND is_active = TRUE
            """, (username.strip(), sha256(password)))
            user = cur.fetchone()
            if user:
                cur.execute("UPDATE users SET last_login = NOW() WHERE user_id = %s", (user["user_id"],))
                conn.commit()
                return dict(user)
            return None
    finally:
        conn.close()


def init_session():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "user" not in st.session_state:
        st.session_state.user = None
    if "session_id" not in st.session_state:
        import uuid
        st.session_state.session_id = str(uuid.uuid4())


def login(username: str, password: str) -> bool:
    user = authenticate(username, password)
    if user:
        st.session_state.authenticated = True
        st.session_state.user = user
        _write_audit("LOGIN", user["user_id"], True)
        return True
    _write_audit("LOGIN_FAILED", None, False, note=username)
    return False


def logout():
    if st.session_state.get("user"):
        _write_audit("LOGOUT", st.session_state.user["user_id"], True)
    st.session_state.authenticated = False
    st.session_state.user = None
    st.rerun()


def require_auth():
    init_session()
    if not st.session_state.authenticated:
        _show_login()
        st.stop()


def has_permission(permission: str) -> bool:
    user = st.session_state.get("user")
    if not user:
        return False
    return ROLE_PERMISSIONS.get(user["role"], {}).get(permission, False)


def require_permission(permission: str):
    if not has_permission(permission):
        role = st.session_state.get("user", {}).get("role", "unknown")
        st.error(f"🚫 Access denied. Role `{role}` does not have permission: `{permission}`")
        st.stop()


def current_user() -> dict | None:
    return st.session_state.get("user")


def current_role() -> str | None:
    user = current_user()
    return user["role"] if user else None


# =============================================================================
# LOGIN UI
# =============================================================================

def _show_login():
    st.markdown("""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:ital,wght@0,300;0,400;0,500;1,300&display=swap');
            html, body, [data-testid="stAppViewContainer"] {
                background: #080D18 !important;
                font-family: 'DM Sans', sans-serif;
            }
            .ng-login-header {
                text-align: center;
                padding: 60px 0 32px;
            }
            .ng-wordmark {
                font-family: 'DM Serif Display', serif;
                font-size: 2.4rem;
                color: #FFFFFF;
                letter-spacing: -0.5px;
            }
            .ng-tagline {
                font-size: 0.72rem;
                color: rgba(255,255,255,0.35);
                letter-spacing: 0.18em;
                text-transform: uppercase;
                margin-top: 6px;
            }
            .ng-disclaimer {
                font-size: 0.68rem;
                color: rgba(255,255,255,0.2);
                text-align: center;
                margin-top: 20px;
                line-height: 1.6;
            }
        </style>
        <div class="ng-login-header">
            <div class="ng-wordmark">🧠 NeuroGuard</div>
            <div class="ng-tagline">Brain Tumor Clinic &nbsp;·&nbsp; Clinical AI Platform</div>
        </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1.4, 1])
    with col2:
        with st.form("login_form"):
            st.text_input("Username", key="login_user", placeholder="username")
            st.text_input("Password", type="password", key="login_pass", placeholder="password")
            submitted = st.form_submit_button("Sign In", use_container_width=True)
            if submitted:
                if login(st.session_state.login_user, st.session_state.login_pass):
                    st.rerun()
                else:
                    st.error("Invalid username or password.")

        with st.expander("Demo credentials"):
            st.markdown("""
            | Role | Username | Password |
            |------|----------|----------|
            | Clinician | `dr_chen` | `changeme1` |
            | Admin | `admin_ops` | `changeme2` |
            | Data Steward | `steward_ng` | `changeme3` |
            | Engineer | `eng_pipeline` | `changeme4` |
            | Auditor | `auditor_ng` | `changeme5` |
            """)

        st.markdown("""
            <div class="ng-disclaimer">
            ⚠️ This system processes protected health information.<br>
            Unauthorized access is prohibited. All activity is logged.<br><br>
            <em>Academic prototype — not for clinical use.</em>
            </div>
        """, unsafe_allow_html=True)


# =============================================================================
# SIDEBAR
# =============================================================================

DARK_SIDEBAR_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500&display=swap');
    html, body, [data-testid="stAppViewContainer"] {
        background: #080D18;
        font-family: 'DM Sans', sans-serif;
        color: #E8EAF0;
    }
    [data-testid="stSidebar"] {
        background: #0C1221 !important;
        border-right: 1px solid rgba(255,255,255,0.06);
    }
    h1, h2, h3 { font-family: 'DM Serif Display', serif; color: #fff; }
    .stMetric { background: rgba(255,255,255,0.03); border-radius: 8px; padding: 12px; }
    [data-testid="stMetricValue"] { color: #fff; }
    [data-testid="stMetricLabel"] { color: rgba(255,255,255,0.5); }
</style>
"""


def show_sidebar():
    user = current_user()
    if not user:
        return

    role  = user["role"]
    color = ROLE_COLORS.get(role, "#888")

    st.markdown(DARK_SIDEBAR_CSS, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown(f"""
            <div style="padding:8px 0 20px;">
                <div style="font-family:'DM Serif Display',serif;font-size:1.25rem;color:#fff;">🧠 NeuroGuard</div>
                <div style="font-size:0.62rem;color:rgba(255,255,255,0.3);letter-spacing:0.12em;text-transform:uppercase;margin-top:2px;">Clinical AI Platform</div>
            </div>
            <div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.07);border-radius:10px;padding:14px 16px;margin-bottom:20px;">
                <div style="font-size:0.88rem;color:#fff;font-weight:500;">{user['full_name']}</div>
                <div style="font-size:0.7rem;color:rgba(255,255,255,0.35);margin-top:2px;">@{user['username']}</div>
                <div style="display:inline-block;margin-top:8px;background:{color}22;color:{color};border:1px solid {color}44;border-radius:4px;padding:2px 8px;font-size:0.62rem;text-transform:uppercase;letter-spacing:0.1em;">{role}</div>
            </div>
        """, unsafe_allow_html=True)

        nav = _nav_for_role(role)
        for item in nav:
            st.markdown(f"**{item['icon']} {item['label']}**")

        st.divider()
        if st.button("Sign Out", use_container_width=True):
            logout()


def _nav_for_role(role: str) -> list[dict]:
    pages = []
    if role == "clinician":
        pages = [
            {"icon": "🫁", "label": "Patients"},
            {"icon": "📅", "label": "Appointments"},
        ]
    elif role == "admin":
        pages = [
            {"icon": "📅", "label": "Appointments"},
            {"icon": "📊", "label": "Analytics"},
        ]
    elif role == "data_steward":
        pages = [
            {"icon": "🛡️", "label": "Governance"},
            {"icon": "📊", "label": "Analytics"},
            {"icon": "📋", "label": "Audit Trail"},
        ]
    elif role == "engineer":
        pages = [{"icon": "📋", "label": "Audit Trail"}]
    elif role == "auditor":
        pages = [
            {"icon": "📋", "label": "Audit Trail"},
            {"icon": "🛡️", "label": "Governance"},
            {"icon": "📊", "label": "Analytics"},
        ]
    return pages


def _write_audit(action: str, user_id, success: bool, note: str = ""):
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit_log (user_id, action, resource_type, resource_id, phi_sensitivity, detail)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (user_id, action, "auth", str(user_id or note), "anonymized",
                  f'{{"success":{str(success).lower()}}}'))
        conn.commit()
        conn.close()
    except Exception:
        pass
