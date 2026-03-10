"""
NeuroGuard — Database Seeder
CIS 8395 | Georgia State University | Spring 2026

Generates synthetic patient data and populates all tables.
Run after schema.sql has been applied.

Usage:
    python db/seed.py
    python db/seed.py --patients 200 --scans 150
"""

import os
import argparse
import hashlib
import random
import uuid
import json
from datetime import datetime, timedelta, date

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
fake = Faker()
random.seed(42)
Faker.seed(42)

# =============================================================================
# DB CONNECTION
# =============================================================================

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME", "neuroguard"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

# =============================================================================
# HELPERS
# =============================================================================

def sha256(val: str) -> str:
    return hashlib.sha256(val.encode()).hexdigest()

def random_date(start: date, end: date) -> date:
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def random_datetime(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

AGE_GROUPS = ["20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80+"]
REGIONS = ["Southeast", "Northeast", "Midwest", "Southwest", "West", "Mid-Atlantic"]
BLOOD_TYPES = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
GENDERS = ["Male", "Female", "Non-binary", "Prefer not to say"]
CONDITIONS = [
    "Hypertension", "Type 2 Diabetes", "Migraine", "Epilepsy",
    "Glioblastoma (prior)", "Meningioma (prior)", "Anxiety", "Depression",
    "Hypercholesterolemia", "Hypothyroidism", "Asthma", "COPD"
]
APPOINTMENT_TYPES = ["initial_consult", "mri_scan", "follow_up", "treatment_review", "discharge"]
PHYSICIANS = [
    "Dr. Sarah Chen", "Dr. James Okafor", "Dr. Priya Nair",
    "Dr. Marcus Webb", "Dr. Elena Vasquez"
]
INSURANCE_PROVIDERS = [
    "BlueCross BlueShield", "Aetna", "UnitedHealth", "Cigna",
    "Humana", "Kaiser Permanente", "Medicare", "Medicaid"
]
MRI_FAKE_PATHS = [
    "data/mri/raw/scan_{scan_id}.jpg",
]
MODEL_VERSION = "vgg16_v1.0"
RISK_MODEL_VERSION = "xgboost_v1.0"

# =============================================================================
# SEED FUNCTIONS
# =============================================================================

def seed_master_patient_index(cur, n: int) -> list[str]:
    print(f"  Seeding master_patient_index ({n} records)...")
    token_ids = []
    rows = []
    sources = ["neuroguard", "epic_fhir", "external_referral"]
    for _ in range(n):
        tid = str(uuid.uuid4())
        token_ids.append(tid)
        rows.append((
            tid,
            random.choice(sources),
            True,
            None
        ))
    execute_values(cur, """
        INSERT INTO master_patient_index (token_id, source_system, is_active, merged_into)
        VALUES %s
    """, rows)
    return token_ids


def seed_patients(cur, token_ids: list[str]) -> dict:
    """Returns mapping of token_id -> patient_id"""
    print(f"  Seeding patients ({len(token_ids)} records)...")
    rows = []
    for tid in token_ids:
        gender = random.choice(GENDERS)
        if gender == "Male":
            first = fake.first_name_male()
        elif gender == "Female":
            first = fake.first_name_female()
        else:
            first = fake.first_name()

        dob = random_date(date(1940, 1, 1), date(2000, 12, 31))
        source = random.choice(["manual_entry", "fhir_transfer", "etl_pipeline"])
        fhir_id = f"fhir-{uuid.uuid4().hex[:12]}" if source == "fhir_transfer" else None

        rows.append((
            tid,
            first,
            fake.last_name(),
            dob,
            gender,
            sha256(fake.ssn()),                     # SSN stored as hash only
            fake.phone_number()[:20],
            fake.email(),
            fake.street_address(),
            fake.city(),
            fake.state_abbr(),
            fake.zipcode()[:10],
            random.choice(BLOOD_TYPES),
            random.choice(PHYSICIANS),
            random.choice(INSURANCE_PROVIDERS),
            f"INS-{fake.bothify('??####')}",
            source,
            fhir_id
        ))

    execute_values(cur, """
        INSERT INTO patients (
            token_id, first_name, last_name, date_of_birth, gender,
            ssn_hash, phone, email, address_line1, address_city,
            address_state, address_zip, blood_type, primary_physician,
            insurance_provider, insurance_id, source, fhir_resource_id
        ) VALUES %s
        RETURNING token_id, patient_id
    """, rows)

    # Build token -> patient_id map
    cur.execute("SELECT token_id, patient_id FROM patients")
    return {str(r["token_id"]): r["patient_id"] for r in cur.fetchall()}


def seed_deidentified(cur, token_ids: list[str]):
    print(f"  Seeding patients_deidentified ({len(token_ids)} records)...")
    # Pull DOB and gender from identified table to compute age_group
    cur.execute("SELECT token_id, date_of_birth, gender FROM patients")
    patient_map = {str(r["token_id"]): r for r in cur.fetchall()}

    rows = []
    for tid in token_ids:
        p = patient_map.get(tid)
        if not p:
            continue
        dob = p["date_of_birth"]
        age = (date.today() - dob).days // 365
        if age < 30:
            age_group = "20-29"
        elif age < 40:
            age_group = "30-39"
        elif age < 50:
            age_group = "40-49"
        elif age < 60:
            age_group = "50-59"
        elif age < 70:
            age_group = "60-69"
        elif age < 80:
            age_group = "70-79"
        else:
            age_group = "80+"

        rows.append((
            tid,
            age_group,
            p["gender"],
            random.choice(REGIONS),         # city/zip replaced with region
            None                            # blood type omitted for extra caution
        ))

    execute_values(cur, """
        INSERT INTO patients_deidentified (token_id, age_group, gender, region, blood_type)
        VALUES %s
    """, rows)


def seed_medical_history(cur, token_ids: list[str]):
    print(f"  Seeding medical_history...")
    rows = []
    for tid in token_ids:
        n_conditions = random.randint(0, 4)
        conditions = random.sample(CONDITIONS, n_conditions)
        for cond in conditions:
            diagnosed = random_date(date(2000, 1, 1), date(2023, 1, 1))
            resolved = None
            if random.random() < 0.3:
                resolved = random_date(diagnosed, date(2024, 1, 1))
            rows.append((
                tid,
                cond,
                diagnosed,
                resolved,
                fake.sentence() if random.random() < 0.4 else None,
                random.choice(["manual_entry", "fhir_transfer"])
            ))

    if rows:
        execute_values(cur, """
            INSERT INTO medical_history (token_id, condition, diagnosed_date, resolved_date, notes, source)
            VALUES %s
        """, rows)


def seed_appointments(cur, token_ids: list[str]) -> dict:
    """Returns mapping of token_id -> list of appointment_ids"""
    print(f"  Seeding appointments...")
    rows = []
    now = datetime.now()
    for tid in token_ids:
        n_appts = random.randint(1, 5)
        for _ in range(n_appts):
            scheduled = random_datetime(
                now - timedelta(days=365),
                now + timedelta(days=90)
            )
            is_past = scheduled < now
            status = random.choice(
                ["completed", "completed", "no_show", "cancelled"] if is_past
                else ["scheduled", "scheduled", "cancelled"]
            )
            rows.append((
                tid,
                scheduled,
                random.choice(APPOINTMENT_TYPES),
                random.choice(PHYSICIANS),
                status,
                fake.sentence() if random.random() < 0.3 else None
            ))

    execute_values(cur, """
        INSERT INTO appointments (token_id, scheduled_at, appointment_type, physician, status, notes)
        VALUES %s
    """, rows)

    cur.execute("SELECT token_id, appointment_id FROM appointments")
    appt_map: dict[str, list[int]] = {}
    for r in cur.fetchall():
        key = str(r["token_id"])
        appt_map.setdefault(key, []).append(r["appointment_id"])
    return appt_map


def seed_mri_scans(cur, token_ids: list[str], appt_map: dict, n_scans: int) -> list[int]:
    """Seeds MRI scans for a subset of patients. Returns list of scan_ids."""
    print(f"  Seeding mri_scans ({n_scans} records)...")
    scan_token_ids = random.sample(token_ids, min(n_scans, len(token_ids)))

    # Get user_id for clinician reviewer
    cur.execute("SELECT user_id FROM users WHERE role = 'clinician' LIMIT 1")
    clinician = cur.fetchone()
    clinician_id = clinician["user_id"] if clinician else None

    rows = []
    now = datetime.now()
    for tid in scan_token_ids:
        appts = appt_map.get(tid, [])
        appt_id = random.choice(appts) if appts else None
        scan_date = random_datetime(now - timedelta(days=300), now - timedelta(days=1))

        tumor = random.choices(
            ["tumor", "no_tumor", "inconclusive"],
            weights=[0.35, 0.55, 0.10]
        )[0]
        confidence = round(random.uniform(0.72, 0.99), 4)
        scan_id_placeholder = len(rows) + 1
        gradcam = f"data/mri/gradcam/gradcam_{scan_id_placeholder}.jpg" if tumor == "tumor" else None

        clinician_review = random.choice(["confirmed", "confirmed", "overridden", "pending"])
        reviewed_at = scan_date + timedelta(hours=random.randint(1, 48)) if clinician_review != "pending" else None

        rows.append((
            tid,
            appt_id,
            f"data/mri/raw/scan_{scan_id_placeholder}.jpg",
            sha256(f"scan_{scan_id_placeholder}"),
            scan_date,
            "MRI",
            "JPG",
            tumor,
            confidence,
            gradcam,
            MODEL_VERSION,
            scan_date + timedelta(minutes=random.randint(2, 10)),
            clinician_review,
            clinician_id,
            reviewed_at,
            fake.sentence() if clinician_review == "overridden" else None
        ))

    execute_values(cur, """
        INSERT INTO mri_scans (
            token_id, appointment_id, file_path, file_hash, scan_date,
            modality, image_format, tumor_status, confidence_score,
            gradcam_path, model_version, inference_at,
            clinician_review, reviewed_by, reviewed_at, clinician_notes
        ) VALUES %s
    """, rows)

    cur.execute("SELECT scan_id, token_id FROM mri_scans")
    results = cur.fetchall()
    return [(r["scan_id"], str(r["token_id"])) for r in results]


def seed_risk_scores(cur, scan_pairs: list[tuple]):
    print(f"  Seeding risk_scores ({len(scan_pairs)} records)...")
    rows = []
    for scan_id, token_id in scan_pairs:
        raw = round(random.uniform(0.05, 0.95), 4)
        if raw < 0.35:
            urgency = "low"
        elif raw < 0.70:
            urgency = "medium"
        else:
            urgency = "high"

        features = {
            "age_group": random.choice(AGE_GROUPS),
            "prior_tumor_history": random.choice([True, False]),
            "num_conditions": random.randint(0, 5),
            "scan_confidence": round(random.uniform(0.7, 0.99), 4),
            "days_since_last_scan": random.randint(30, 730)
        }

        rows.append((
            token_id,
            scan_id,
            urgency,
            raw,
            RISK_MODEL_VERSION,
            json.dumps(features)
        ))

    execute_values(cur, """
        INSERT INTO risk_scores (token_id, scan_id, urgency_level, raw_score, model_version, feature_snapshot)
        VALUES %s
    """, rows)


def seed_fhir_transfers(cur, token_ids: list[str]):
    fhir_tokens = random.sample(token_ids, max(1, len(token_ids) // 4))
    print(f"  Seeding fhir_transfers ({len(fhir_tokens)} records)...")
    resource_types = ["Patient", "DiagnosticReport", "ImagingStudy", "Observation", "Encounter"]
    rows = []
    now = datetime.now()

    for tid in fhir_tokens:
        resource_type = random.choice(resource_types)
        fhir_id = f"fhir-{uuid.uuid4().hex[:12]}"
        bundle = {
            "resourceType": "Bundle",
            "id": str(uuid.uuid4()),
            "type": "transaction",
            "entry": [{
                "resource": {
                    "resourceType": resource_type,
                    "id": fhir_id,
                    "subject": {"reference": f"Patient/{tid}"},
                    "status": "final",
                    "meta": {"source": "epic_demo", "versionId": "1"}
                }
            }]
        }
        received = random_datetime(now - timedelta(days=180), now)
        processed = random.random() < 0.85
        rows.append((
            tid,
            resource_type,
            fhir_id,
            "epic_demo",
            json.dumps(bundle),
            received,
            processed,
            received + timedelta(minutes=5) if processed else None
        ))

    execute_values(cur, """
        INSERT INTO fhir_transfers (
            token_id, fhir_resource_type, fhir_resource_id, source_system,
            raw_bundle, received_at, processed, processed_at
        ) VALUES %s
    """, rows)


def seed_audit_log(cur, token_ids: list[str]):
    print(f"  Seeding audit_log (sample entries)...")
    cur.execute("SELECT user_id, role FROM users")
    users = cur.fetchall()

    actions = {
        "clinician":    ["VIEW_PATIENT", "UPLOAD_MRI", "VIEW_MRI", "ADD_NOTE"],
        "admin":        ["VIEW_APPOINTMENTS", "SCHEDULE_APPOINTMENT", "VIEW_ANALYTICS"],
        "data_steward": ["VIEW_LINEAGE", "RUN_QUALITY_CHECK", "VIEW_GOVERNANCE"],
        "engineer":     ["VIEW_AUDIT_LOG", "RUN_ETL", "VIEW_PIPELINE_STATUS"],
        "auditor":      ["VIEW_AUDIT_LOG", "EXPORT_AUDIT_REPORT", "VIEW_LINEAGE"]
    }

    rows = []
    now = datetime.now()
    sample_tokens = random.sample(token_ids, min(50, len(token_ids)))

    for _ in range(200):
        user = random.choice(users)
        uid = user["user_id"]
        role = user["role"]
        action = random.choice(actions.get(role, ["VIEW_RECORD"]))
        token = random.choice(sample_tokens) if random.random() < 0.7 else None
        logged = random_datetime(now - timedelta(days=90), now)

        rows.append((
            uid,
            action,
            "patient" if "PATIENT" in action or "MRI" in action else "system",
            str(random.randint(1, 500)),
            token,
            "identified" if role == "clinician" else "deidentified",
            fake.ipv4(),
            str(uuid.uuid4()),
            json.dumps({"note": fake.sentence()}),
            logged
        ))

    execute_values(cur, """
        INSERT INTO audit_log (
            user_id, action, resource_type, resource_id, token_id,
            phi_sensitivity, ip_address, session_id, detail, logged_at
        ) VALUES %s
    """, rows)


def seed_lineage(cur, token_ids: list[str]):
    print(f"  Seeding data_lineage...")
    ingestion_methods = ["manual", "fhir_transfer", "etl_batch"]
    destinations = ["patients", "patients_deidentified", "mri_scans", "fhir_transfers"]
    rows = []

    for tid in random.sample(token_ids, min(100, len(token_ids))):
        rows.append((
            "patient",
            tid,
            tid,
            random.choice(["neuroguard_intake", "epic_demo", "external_csv"]),
            random.choice(ingestion_methods),
            json.dumps([
                "ingestion",
                "null_check",
                "standardize_phone",
                "safe_harbor_deident",
                "mpi_token_assign"
            ]),
            random.choice(destinations),
            "etl_v1.0"
        ))

    execute_values(cur, """
        INSERT INTO data_lineage (
            record_type, record_id, token_id, source_system,
            ingestion_method, transformations, destination, pipeline_version
        ) VALUES %s
    """, rows)


def seed_quality_log(cur):
    print(f"  Seeding data_quality_log...")
    tables = ["patients", "mri_scans", "appointments", "fhir_transfers"]
    check_types = ["completeness", "validity", "dedup", "referential_integrity"]
    rows = []

    for _ in range(150):
        passed = random.random() < 0.88
        rows.append((
            random.choice(tables),
            str(random.randint(1, 500)),
            random.choice(check_types),
            passed,
            round(random.uniform(0.6, 1.0) if passed else random.uniform(0.0, 0.6), 4),
            json.dumps({"field": random.choice(["phone", "email", "date_of_birth", "file_hash"])})
        ))

    execute_values(cur, """
        INSERT INTO data_quality_log (table_name, record_id, check_type, passed, score, details)
        VALUES %s
    """, rows)


# =============================================================================
# MAIN
# =============================================================================

def main(n_patients: int = 100, n_scans: int = 80):
    print(f"\n🧠 NeuroGuard DB Seeder")
    print(f"   Patients: {n_patients} | MRI Scans: {n_scans}\n")

    conn = get_conn()
    conn.autocommit = False

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            print("▶ Phase 1: Identity & Patients")
            token_ids = seed_master_patient_index(cur, n_patients)
            seed_patients(cur, token_ids)
            seed_deidentified(cur, token_ids)
            seed_medical_history(cur, token_ids)

            print("\n▶ Phase 2: Clinical Activity")
            appt_map = seed_appointments(cur, token_ids)
            scan_pairs = seed_mri_scans(cur, token_ids, appt_map, n_scans)
            seed_risk_scores(cur, scan_pairs)
            seed_fhir_transfers(cur, token_ids)

            print("\n▶ Phase 3: Governance")
            seed_audit_log(cur, token_ids)
            seed_lineage(cur, token_ids)
            seed_quality_log(cur)

        conn.commit()
        print(f"\n✅ Seeding complete.")
        print(f"   {n_patients} patients | {n_scans} MRI scans | 200 audit entries")
        print(f"   Run: streamlit run dashboard/app.py\n")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Seeding failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed NeuroGuard database")
    parser.add_argument("--patients", type=int, default=100, help="Number of patients to generate")
    parser.add_argument("--scans", type=int, default=80, help="Number of MRI scans to generate")
    args = parser.parse_args()
    main(n_patients=args.patients, n_scans=args.scans)
