-- =============================================================================
-- NeuroGuard — PostgreSQL Schema
-- CIS 8395 | Georgia State University | Spring 2026
-- =============================================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =============================================================================
-- ENUMS
-- =============================================================================

CREATE TYPE user_role AS ENUM ('clinician', 'admin', 'data_steward', 'engineer', 'auditor');
CREATE TYPE tumor_status AS ENUM ('tumor', 'no_tumor', 'pending', 'inconclusive');
CREATE TYPE urgency_level AS ENUM ('low', 'medium', 'high');
CREATE TYPE appointment_status AS ENUM ('scheduled', 'completed', 'cancelled', 'no_show');
CREATE TYPE data_source AS ENUM ('manual_entry', 'fhir_transfer', 'mri_upload', 'etl_pipeline');
CREATE TYPE phi_sensitivity AS ENUM ('identified', 'deidentified', 'anonymized');

-- =============================================================================
-- CORE IDENTITY LAYER — Master Patient Index (MDM/MPI)
-- =============================================================================

CREATE TABLE master_patient_index (
    token_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mpi_created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    mpi_updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(100) NOT NULL,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    merged_into         UUID REFERENCES master_patient_index(token_id)
);

COMMENT ON TABLE master_patient_index IS
    'Single patient identity anchor. token_id is the only ID used downstream. Real identifiers never leave this table.';

-- =============================================================================
-- USERS & RBAC (defined before mri_scans which references it)
-- =============================================================================

CREATE TABLE users (
    user_id             SERIAL PRIMARY KEY,
    username            VARCHAR(100) NOT NULL UNIQUE,
    password_hash       CHAR(64) NOT NULL,
    role                user_role NOT NULL,
    full_name           VARCHAR(255),
    email               VARCHAR(255),
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    last_login          TIMESTAMP,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- PATIENT DATA — Identified Path (Clinical Views Only)
-- =============================================================================

CREATE TABLE patients (
    patient_id          SERIAL PRIMARY KEY,
    token_id            UUID NOT NULL UNIQUE REFERENCES master_patient_index(token_id),

    -- PHI — identified path only
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100) NOT NULL,
    date_of_birth       DATE NOT NULL,
    gender              VARCHAR(20),
    ssn_hash            CHAR(64),
    phone               VARCHAR(20),
    email               VARCHAR(255),
    address_line1       VARCHAR(255),
    address_city        VARCHAR(100),
    address_state       CHAR(2),
    address_zip         VARCHAR(10),

    -- Clinical metadata
    blood_type          VARCHAR(5),
    primary_physician   VARCHAR(255),
    insurance_provider  VARCHAR(255),
    insurance_id        VARCHAR(100),

    -- Record management
    source              data_source NOT NULL DEFAULT 'manual_entry',
    fhir_resource_id    VARCHAR(255),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    is_deleted          BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_patients_token_id ON patients(token_id);
CREATE INDEX idx_patients_last_name ON patients(last_name);

-- =============================================================================
-- DE-IDENTIFIED PATIENT RECORDS — Analytics Path
-- =============================================================================

CREATE TABLE patients_deidentified (
    deident_id          SERIAL PRIMARY KEY,
    token_id            UUID NOT NULL UNIQUE REFERENCES master_patient_index(token_id),

    -- Safe Harbor de-identified fields (18 PHI identifiers removed/generalized)
    age_group           VARCHAR(20),
    gender              VARCHAR(20),
    region              VARCHAR(50),
    blood_type          VARCHAR(5),

    -- De-identification metadata
    deident_method      VARCHAR(50) NOT NULL DEFAULT 'HIPAA_Safe_Harbor',
    deident_date        TIMESTAMP NOT NULL DEFAULT NOW(),
    deident_version     VARCHAR(20) NOT NULL DEFAULT '1.0'
);

CREATE INDEX idx_deident_token_id ON patients_deidentified(token_id);

-- =============================================================================
-- MEDICAL HISTORY
-- =============================================================================

CREATE TABLE medical_history (
    history_id          SERIAL PRIMARY KEY,
    token_id            UUID NOT NULL REFERENCES master_patient_index(token_id),
    condition           VARCHAR(255) NOT NULL,
    diagnosed_date      DATE,
    resolved_date       DATE,
    notes               TEXT,
    source              data_source NOT NULL DEFAULT 'manual_entry',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_medical_history_token ON medical_history(token_id);

-- =============================================================================
-- APPOINTMENTS
-- =============================================================================

CREATE TABLE appointments (
    appointment_id      SERIAL PRIMARY KEY,
    token_id            UUID NOT NULL REFERENCES master_patient_index(token_id),
    scheduled_at        TIMESTAMP NOT NULL,
    appointment_type    VARCHAR(100) NOT NULL,
    physician           VARCHAR(255),
    status              appointment_status NOT NULL DEFAULT 'scheduled',
    notes               TEXT,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_appointments_token ON appointments(token_id);
CREATE INDEX idx_appointments_scheduled ON appointments(scheduled_at);
CREATE INDEX idx_appointments_status ON appointments(status);

-- =============================================================================
-- MRI SCANS & AI RESULTS
-- =============================================================================

CREATE TABLE mri_scans (
    scan_id             SERIAL PRIMARY KEY,
    token_id            UUID NOT NULL REFERENCES master_patient_index(token_id),
    appointment_id      INT REFERENCES appointments(appointment_id),

    -- File reference
    file_path           VARCHAR(500) NOT NULL,
    file_hash           CHAR(64),
    scan_date           TIMESTAMP NOT NULL,
    modality            VARCHAR(50) DEFAULT 'MRI',
    image_format        VARCHAR(20) DEFAULT 'JPG',

    -- AI inference results
    tumor_status        tumor_status NOT NULL DEFAULT 'pending',
    confidence_score    FLOAT CHECK (confidence_score BETWEEN 0 AND 1),
    gradcam_path        VARCHAR(500),
    model_version       VARCHAR(50),
    inference_at        TIMESTAMP,

    -- Clinician review
    clinician_review    VARCHAR(20) DEFAULT 'pending',
    reviewed_by         INT REFERENCES users(user_id),
    reviewed_at         TIMESTAMP,
    clinician_notes     TEXT,

    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_mri_token ON mri_scans(token_id);
CREATE INDEX idx_mri_tumor_status ON mri_scans(tumor_status);
CREATE INDEX idx_mri_scan_date ON mri_scans(scan_date);

-- =============================================================================
-- RISK SCORES (XGBoost Output)
-- =============================================================================

CREATE TABLE risk_scores (
    score_id            SERIAL PRIMARY KEY,
    token_id            UUID NOT NULL REFERENCES master_patient_index(token_id),
    scan_id             INT REFERENCES mri_scans(scan_id),

    urgency_level       urgency_level NOT NULL,
    raw_score           FLOAT NOT NULL CHECK (raw_score BETWEEN 0 AND 1),
    model_version       VARCHAR(50),
    feature_snapshot    JSONB,
    scored_at           TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_risk_token ON risk_scores(token_id);
CREATE INDEX idx_risk_urgency ON risk_scores(urgency_level);

-- =============================================================================
-- FHIR TRANSFER RECORDS
-- =============================================================================

CREATE TABLE fhir_transfers (
    transfer_id         SERIAL PRIMARY KEY,
    token_id            UUID NOT NULL REFERENCES master_patient_index(token_id),
    fhir_resource_type  VARCHAR(100) NOT NULL,
    fhir_resource_id    VARCHAR(255) NOT NULL,
    source_system       VARCHAR(255) NOT NULL,
    raw_bundle          JSONB NOT NULL,
    received_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    processed           BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at        TIMESTAMP
);

CREATE INDEX idx_fhir_token ON fhir_transfers(token_id);
CREATE INDEX idx_fhir_resource_type ON fhir_transfers(fhir_resource_type);

-- =============================================================================
-- DATA GOVERNANCE — Audit Log
-- =============================================================================

CREATE TABLE audit_log (
    log_id              BIGSERIAL PRIMARY KEY,
    user_id             INT REFERENCES users(user_id),
    action              VARCHAR(100) NOT NULL,
    resource_type       VARCHAR(100) NOT NULL,
    resource_id         VARCHAR(100),
    token_id            UUID REFERENCES master_patient_index(token_id),
    phi_sensitivity     phi_sensitivity,
    ip_address          VARCHAR(45),
    session_id          VARCHAR(255),
    detail              JSONB,
    logged_at           TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_user ON audit_log(user_id);
CREATE INDEX idx_audit_action ON audit_log(action);
CREATE INDEX idx_audit_token ON audit_log(token_id);
CREATE INDEX idx_audit_logged_at ON audit_log(logged_at);

COMMENT ON TABLE audit_log IS
    'Immutable audit trail. Never UPDATE or DELETE rows from this table.';

-- =============================================================================
-- DATA GOVERNANCE — Lineage Tracker
-- =============================================================================

CREATE TABLE data_lineage (
    lineage_id          SERIAL PRIMARY KEY,
    record_type         VARCHAR(100) NOT NULL,
    record_id           VARCHAR(100) NOT NULL,
    token_id            UUID REFERENCES master_patient_index(token_id),
    source_system       VARCHAR(100) NOT NULL,
    ingestion_method    VARCHAR(100) NOT NULL,
    transformations     JSONB,
    destination         VARCHAR(100) NOT NULL,
    pipeline_version    VARCHAR(50),
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_lineage_record ON data_lineage(record_type, record_id);
CREATE INDEX idx_lineage_token ON data_lineage(token_id);

-- =============================================================================
-- DATA GOVERNANCE — Quality Scores
-- =============================================================================

CREATE TABLE data_quality_log (
    quality_id          SERIAL PRIMARY KEY,
    table_name          VARCHAR(100) NOT NULL,
    record_id           VARCHAR(100) NOT NULL,
    check_type          VARCHAR(100) NOT NULL,
    passed              BOOLEAN NOT NULL,
    score               FLOAT CHECK (score BETWEEN 0 AND 1),
    details             JSONB,
    checked_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_quality_table ON data_quality_log(table_name);
CREATE INDEX idx_quality_passed ON data_quality_log(passed);

-- =============================================================================
-- VIEWS — Enforce Access Paths
-- =============================================================================

-- Clinician view: full identified patient data + latest scan + risk score
CREATE VIEW v_clinician_patient AS
SELECT
    p.patient_id,
    p.token_id,
    p.first_name,
    p.last_name,
    p.date_of_birth,
    p.gender,
    p.phone,
    p.email,
    p.primary_physician,
    p.insurance_provider,
    m.scan_id,
    m.scan_date,
    m.tumor_status,
    m.confidence_score,
    m.gradcam_path,
    r.urgency_level
FROM patients p
LEFT JOIN LATERAL (
    SELECT * FROM mri_scans
    WHERE token_id = p.token_id
    ORDER BY scan_date DESC LIMIT 1
) m ON TRUE
LEFT JOIN LATERAL (
    SELECT * FROM risk_scores
    WHERE token_id = p.token_id
    ORDER BY scored_at DESC LIMIT 1
) r ON TRUE
WHERE p.is_deleted = FALSE;

-- Analytics view: de-identified only
CREATE VIEW v_analytics_deident AS
SELECT
    pd.token_id,
    pd.age_group,
    pd.gender,
    pd.region,
    pd.blood_type,
    m.tumor_status,
    m.confidence_score,
    r.urgency_level,
    a.appointment_type,
    a.status AS appointment_status
FROM patients_deidentified pd
LEFT JOIN LATERAL (
    SELECT tumor_status, confidence_score FROM mri_scans
    WHERE token_id = pd.token_id
    ORDER BY scan_date DESC LIMIT 1
) m ON TRUE
LEFT JOIN LATERAL (
    SELECT urgency_level FROM risk_scores
    WHERE token_id = pd.token_id
    ORDER BY scored_at DESC LIMIT 1
) r ON TRUE
LEFT JOIN LATERAL (
    SELECT appointment_type, status FROM appointments
    WHERE token_id = pd.token_id
    ORDER BY scheduled_at DESC LIMIT 1
) a ON TRUE;

-- =============================================================================
-- SEED USERS (change passwords before any deployment)
-- =============================================================================

INSERT INTO users (username, password_hash, role, full_name, email) VALUES
('dr_chen',      encode(sha256('changeme1'::bytea), 'hex'), 'clinician',    'Dr. Sarah Chen',       'schen@neuroguard.local'),
('admin_ops',    encode(sha256('changeme2'::bytea), 'hex'), 'admin',        'Clinic Admin',         'admin@neuroguard.local'),
('steward_ng',   encode(sha256('changeme3'::bytea), 'hex'), 'data_steward', 'Data Steward',         'steward@neuroguard.local'),
('eng_pipeline', encode(sha256('changeme4'::bytea), 'hex'), 'engineer',     'Pipeline Engineer',    'eng@neuroguard.local'),
('auditor_ng',   encode(sha256('changeme5'::bytea), 'hex'), 'auditor',      'Compliance Auditor',   'auditor@neuroguard.local');
