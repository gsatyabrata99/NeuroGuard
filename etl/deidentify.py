"""
NeuroGuard — ETL De-identification Layer
Applies HIPAA Safe Harbor method to patient records.

Safe Harbor removes or generalizes all 18 PHI identifiers:
  1.  Names
  2.  Geographic data smaller than state
  3.  Dates (except year) for individuals over 89
  4.  Phone numbers
  5.  Fax numbers
  6.  Email addresses
  7.  SSN
  8.  Medical record numbers
  9.  Health plan beneficiary numbers
  10. Account numbers
  11. Certificate/license numbers
  12. Vehicle identifiers
  13. Device identifiers
  14. URLs
  15. IP addresses
  16. Biometric identifiers
  17. Full-face photographs
  18. Any other unique identifier

Reference: 45 CFR §164.514(b)
"""

import logging
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)

DEIDENT_METHOD  = "HIPAA_Safe_Harbor"
DEIDENT_VERSION = "1.0"

# =============================================================================
# AGE GROUP BUCKETING
# =============================================================================

def _dob_to_age_group(dob_str: str | None) -> str:
    """
    Convert date of birth to age group.
    Patients 90+ are grouped as '90+' per Safe Harbor rule 3
    (exact age/DOB for individuals over 89 must be suppressed).
    """
    if not dob_str:
        return "Unknown"
    try:
        dob = date.fromisoformat(str(dob_str))
        age = (date.today() - dob).days // 365
        if age < 18:
            return "<18"
        elif age < 30:
            return "18-29"
        elif age < 40:
            return "30-39"
        elif age < 50:
            return "40-49"
        elif age < 60:
            return "50-59"
        elif age < 70:
            return "60-69"
        elif age < 80:
            return "70-79"
        elif age < 90:
            return "80-89"
        else:
            return "90+"     # Safe Harbor: suppress exact age for 90+
    except (ValueError, TypeError):
        return "Unknown"


def _state_to_region(state: str | None) -> str:
    """
    Map US state abbreviation to region.
    Zip codes and cities are suppressed entirely — only state/region retained.

    Safe Harbor allows state-level geography unless population < 20,000.
    We generalize to region for extra caution.
    """
    if not state or pd.isna(state):
        return "Unknown"

    REGION_MAP = {
        # Southeast
        "AL": "Southeast", "AR": "Southeast", "FL": "Southeast",
        "GA": "Southeast", "KY": "Southeast", "LA": "Southeast",
        "MS": "Southeast", "NC": "Southeast", "SC": "Southeast",
        "TN": "Southeast", "VA": "Southeast", "WV": "Southeast",
        # Northeast
        "CT": "Northeast", "DE": "Northeast", "MA": "Northeast",
        "MD": "Northeast", "ME": "Northeast", "NH": "Northeast",
        "NJ": "Northeast", "NY": "Northeast", "PA": "Northeast",
        "RI": "Northeast", "VT": "Northeast",
        # Midwest
        "IA": "Midwest", "IL": "Midwest", "IN": "Midwest",
        "KS": "Midwest", "MI": "Midwest", "MN": "Midwest",
        "MO": "Midwest", "ND": "Midwest", "NE": "Midwest",
        "OH": "Midwest", "SD": "Midwest", "WI": "Midwest",
        # Southwest
        "AZ": "Southwest", "NM": "Southwest", "OK": "Southwest", "TX": "Southwest",
        # West
        "AK": "West", "CA": "West", "CO": "West", "HI": "West",
        "ID": "West", "MT": "West", "NV": "West", "OR": "West",
        "UT": "West", "WA": "West", "WY": "West",
        # Mid-Atlantic / DC
        "DC": "Mid-Atlantic",
    }
    return REGION_MAP.get(str(state).strip().upper(), "Unknown")


# =============================================================================
# CORE DE-IDENTIFICATION
# =============================================================================

PHI_COLUMNS_TO_DROP = [
    # Rule 1: Names
    "first_name", "last_name",
    # Rule 4: Phone
    "phone",
    # Rule 6: Email
    "email",
    # Rule 7: SSN / SSN hash
    "ssn", "ssn_hash",
    # Rule 2: Sub-state geography
    "address_line1", "address_city", "address_zip",
    # Rule 8-11: IDs
    "insurance_id", "fhir_resource_id",
    # Misc PII
    "primary_physician",
]


def deidentify_patients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply HIPAA Safe Harbor de-identification to patient DataFrame.

    Input:  Transformed patient DataFrame (identified)
    Output: De-identified DataFrame with:
              - token_id preserved (pseudonym only)
              - PHI columns dropped
              - DOB → age_group
              - city/zip/address → dropped
              - state → region
              - blood_type retained (not PHI)
              - gender retained (not PHI when not combined with other identifiers)
    """
    if df.empty:
        logger.warning("deidentify_patients: empty DataFrame received")
        return df

    logger.info(f"De-identifying {len(df)} patient records (HIPAA Safe Harbor)...")

    deident = df.copy()

    # Derive safe fields before dropping source columns
    deident["age_group"] = deident.get("date_of_birth", pd.Series()).apply(_dob_to_age_group)
    deident["region"]    = deident.get("address_state", pd.Series()).apply(_state_to_region)

    # Drop all PHI columns + date_of_birth (replaced by age_group)
    cols_to_drop = PHI_COLUMNS_TO_DROP + ["date_of_birth", "address_state"]
    cols_to_drop = [c for c in cols_to_drop if c in deident.columns]
    deident = deident.drop(columns=cols_to_drop)

    # Retain only safe fields
    safe_cols = [
        "token_id",
        "age_group",
        "gender",
        "region",
        "blood_type",
        "_source_file",
        "_ingested_at",
        "_transform_quality"
    ]
    retain = [c for c in safe_cols if c in deident.columns]
    deident = deident[retain]

    # Add de-identification metadata
    deident["deident_method"]  = DEIDENT_METHOD
    deident["deident_version"] = DEIDENT_VERSION

    logger.info(f"De-identification complete: {len(deident)} records, {len(deident.columns)} columns retained")
    logger.info(f"  Retained columns: {list(deident.columns)}")
    return deident.reset_index(drop=True)


def deidentify_mri_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    De-identify MRI manifest for analytics path.
    Retains: token_id, age_group (derived), tumor_status, scan_date (year only).
    Drops:   file_path (re-identifiable via file name), file_hash.
    """
    if df.empty:
        return df

    logger.info(f"De-identifying {len(df)} MRI metadata records...")
    deident = df.copy()

    # Retain scan year only — not full date
    if "scan_date" in deident.columns:
        deident["scan_year"] = pd.to_datetime(
            deident["scan_date"], errors="coerce"
        ).dt.year.astype("Int64")

    drop_cols = ["file_path", "file_hash", "scan_date", "_file_hash", "patient_ref"]
    deident = deident.drop(columns=[c for c in drop_cols if c in deident.columns])

    deident["deident_method"]  = DEIDENT_METHOD
    deident["deident_version"] = DEIDENT_VERSION

    return deident.reset_index(drop=True)


# =============================================================================
# VALIDATION — PHI Leak Check
# =============================================================================

PHI_SIGNAL_COLS = [
    "first_name", "last_name", "ssn", "ssn_hash",
    "phone", "email", "address_line1", "address_city", "address_zip",
    "date_of_birth", "insurance_id"
]

def validate_no_phi(df: pd.DataFrame, label: str = "dataframe") -> bool:
    """
    Checks that no known PHI column names remain in the de-identified DataFrame.
    Logs a warning for each detected column.
    Returns True if clean, False if PHI detected.
    """
    found = [c for c in PHI_SIGNAL_COLS if c in df.columns]
    if found:
        logger.error(f"PHI LEAK DETECTED in {label}: {found}")
        return False
    logger.info(f"PHI check passed for {label} — no PHI columns detected")
    return True


# =============================================================================
# ENTRY POINT
# =============================================================================

def run_deidentification(transformed: dict) -> dict:
    """
    Accepts output from transform.run_transform() and de-identifies
    records destined for the analytics path.

    Returns:
        {
            "patients_deident":    DataFrame,   # safe for analytics layer
            "mri_deident":         DataFrame,
            "phi_check_passed":    bool
        }
    """
    logger.info("=" * 60)
    logger.info("DE-IDENTIFICATION LAYER — START")
    logger.info("=" * 60)

    patients_deident = deidentify_patients(transformed.get("patients", pd.DataFrame()))
    mri_deident      = deidentify_mri_metadata(transformed.get("mri_manifest", pd.DataFrame()))

    phi_ok_patients = validate_no_phi(patients_deident, "patients_deident")
    phi_ok_mri      = validate_no_phi(mri_deident, "mri_deident")

    logger.info("DE-IDENTIFICATION LAYER — COMPLETE")
    return {
        "patients_deident":  patients_deident,
        "mri_deident":       mri_deident,
        "phi_check_passed":  phi_ok_patients and phi_ok_mri
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    from ingestion import run_ingestion
    from transform import run_transform
    raw         = run_ingestion()
    transformed = run_transform(raw)
    deident     = run_deidentification(transformed)
    print(f"\n  Patients de-identified: {len(deident['patients_deident'])}")
    print(f"  MRI de-identified:      {len(deident['mri_deident'])}")
    print(f"  PHI check passed:       {deident['phi_check_passed']}")
