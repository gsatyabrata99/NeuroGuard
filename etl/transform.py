"""
NeuroGuard — ETL Transform Layer
Cleans, standardizes, and deduplicates ingested data.
Operates on raw DataFrames from ingestion.py.
"""

import re
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# =============================================================================
# PATIENT CSV TRANSFORMS
# =============================================================================

GENDER_MAP = {
    "m": "Male", "male": "Male", "man": "Male",
    "f": "Female", "female": "Female", "woman": "Female",
    "nb": "Non-binary", "non-binary": "Non-binary", "nonbinary": "Non-binary",
    "other": "Prefer not to say", "unknown": "Prefer not to say", "": "Prefer not to say"
}

BLOOD_TYPE_VALID = {"A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"}

STATE_ABBRS = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC"
}


def _clean_phone(phone: str) -> str | None:
    if pd.isna(phone):
        return None
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == "1":
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return str(phone)[:20]  # return as-is but truncate


def _clean_email(email: str) -> str | None:
    if pd.isna(email):
        return None
    email = str(email).strip().lower()
    return email if re.match(r"[^@]+@[^@]+\.[^@]+", email) else None


def _clean_date(val: str) -> str | None:
    if pd.isna(val):
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(val).strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    logger.warning(f"  Could not parse date: {val}")
    return None


def _normalize_gender(val: str) -> str:
    if pd.isna(val):
        return "Prefer not to say"
    return GENDER_MAP.get(str(val).strip().lower(), "Prefer not to say")


def _normalize_blood_type(val: str) -> str | None:
    if pd.isna(val):
        return None
    val = str(val).strip().upper()
    return val if val in BLOOD_TYPE_VALID else None


def _normalize_state(val: str) -> str | None:
    if pd.isna(val):
        return None
    val = str(val).strip().upper()
    return val if val in STATE_ABBRS else None


def _normalize_name(val: str) -> str | None:
    if pd.isna(val):
        return None
    return str(val).strip().title()


def transform_patients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and standardizes patient CSV data.
    Returns a cleaned DataFrame ready for de-identification and loading.
    """
    if df.empty:
        return df

    logger.info(f"Transforming {len(df)} patient rows...")
    original_count = len(df)

    # Drop rows missing required fields
    required = ["first_name", "last_name", "date_of_birth"]
    df = df.dropna(subset=[c for c in required if c in df.columns])
    dropped = original_count - len(df)
    if dropped:
        logger.warning(f"  Dropped {dropped} rows missing required fields")

    # Normalize names
    df["first_name"] = df["first_name"].apply(_normalize_name)
    df["last_name"]  = df["last_name"].apply(_normalize_name)

    # Date of birth
    df["date_of_birth"] = df["date_of_birth"].apply(_clean_date)
    df = df.dropna(subset=["date_of_birth"])

    # Contact
    if "phone" in df.columns:
        df["phone"] = df["phone"].apply(_clean_phone)
    if "email" in df.columns:
        df["email"] = df["email"].apply(_clean_email)

    # Demographics
    if "gender" in df.columns:
        df["gender"] = df["gender"].apply(_normalize_gender)
    if "blood_type" in df.columns:
        df["blood_type"] = df["blood_type"].apply(_normalize_blood_type)
    if "address_state" in df.columns:
        df["address_state"] = df["address_state"].apply(_normalize_state)
    if "address_zip" in df.columns:
        df["address_zip"] = df["address_zip"].apply(
            lambda z: str(z).strip()[:10] if not pd.isna(z) else None
        )

    # Dedup — same first name + last name + DOB = likely same person
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["first_name", "last_name", "date_of_birth"], keep="first")
    deduped = before_dedup - len(df)
    if deduped:
        logger.info(f"  Removed {deduped} duplicate patient rows")

    # Quality flag
    df["_transform_quality"] = df.apply(_score_completeness, axis=1)

    logger.info(f"Patient transform complete: {len(df)} clean rows")
    return df.reset_index(drop=True)


def _score_completeness(row: pd.Series) -> float:
    """Simple completeness score: fraction of key fields that are non-null."""
    key_fields = [
        "first_name", "last_name", "date_of_birth", "gender",
        "phone", "email", "address_city", "address_state",
        "blood_type", "insurance_provider"
    ]
    present = sum(1 for f in key_fields if f in row and pd.notna(row[f]))
    return round(present / len(key_fields), 2)


# =============================================================================
# FHIR TRANSFORMS
# =============================================================================

def transform_fhir_patients(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize FHIR Patient records to match patient schema."""
    if df.empty:
        return df

    logger.info(f"Transforming {len(df)} FHIR Patient records...")

    if "date_of_birth" in df.columns:
        df["date_of_birth"] = df["date_of_birth"].apply(_clean_date)
    if "gender" in df.columns:
        df["gender"] = df["gender"].apply(_normalize_gender)
    if "phone" in df.columns:
        df["phone"] = df["phone"].apply(_clean_phone)
    if "email" in df.columns:
        df["email"] = df["email"].apply(_clean_email)
    if "first_name" in df.columns:
        df["first_name"] = df["first_name"].apply(_normalize_name)
    if "last_name" in df.columns:
        df["last_name"] = df["last_name"].apply(_normalize_name)

    df = df.dropna(subset=["first_name", "last_name", "date_of_birth"])
    df["_transform_quality"] = df.apply(_score_completeness, axis=1)

    logger.info(f"FHIR Patient transform complete: {len(df)} rows")
    return df.reset_index(drop=True)


def transform_fhir_diagnostic_reports(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    logger.info(f"Transforming {len(df)} FHIR DiagnosticReport records...")
    if "issued" in df.columns:
        df["issued"] = df["issued"].apply(_clean_date)
    df = df.dropna(subset=["fhir_resource_id"])
    return df.reset_index(drop=True)


def transform_fhir_observations(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    logger.info(f"Transforming {len(df)} FHIR Observation records...")
    if "effective_datetime" in df.columns:
        df["effective_datetime"] = df["effective_datetime"].apply(_clean_date)
    df = df.dropna(subset=["fhir_resource_id"])
    return df.reset_index(drop=True)


# =============================================================================
# MRI MANIFEST TRANSFORMS
# =============================================================================

def transform_mri_manifest(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize MRI manifest entries."""
    if df.empty:
        return df

    logger.info(f"Transforming {len(df)} MRI manifest entries...")

    if "scan_date" in df.columns:
        df["scan_date"] = df["scan_date"].apply(_clean_date)

    if "modality" in df.columns:
        df["modality"] = df["modality"].str.strip().str.upper().fillna("MRI")

    if "image_format" in df.columns:
        df["image_format"] = df["image_format"].str.strip().str.upper().fillna("JPG")

    # Drop entries with no file path
    df = df.dropna(subset=["file_path"])
    df["file_path"] = df["file_path"].str.strip()

    # Remove duplicate file paths
    before = len(df)
    df = df.drop_duplicates(subset=["file_path"], keep="first")
    if len(df) < before:
        logger.info(f"  Removed {before - len(df)} duplicate MRI file entries")

    logger.info(f"MRI manifest transform complete: {len(df)} entries")
    return df.reset_index(drop=True)


# =============================================================================
# COMBINED TRANSFORM ENTRY POINT
# =============================================================================

def run_transform(ingested: dict) -> dict:
    """
    Accepts output from ingestion.run_ingestion() and transforms all sources.

    Returns:
        {
            "patients":              DataFrame,   # from CSV
            "fhir_patients":         DataFrame,
            "fhir_diagnostic":       DataFrame,
            "fhir_observations":     DataFrame,
            "mri_manifest":          DataFrame,
        }
    """
    logger.info("=" * 60)
    logger.info("TRANSFORM LAYER — START")
    logger.info("=" * 60)

    fhir = ingested.get("fhir", {})

    result = {
        "patients":           transform_patients(ingested.get("patients_csv", pd.DataFrame())),
        "fhir_patients":      transform_fhir_patients(fhir.get("Patient", pd.DataFrame())),
        "fhir_diagnostic":    transform_fhir_diagnostic_reports(fhir.get("DiagnosticReport", pd.DataFrame())),
        "fhir_observations":  transform_fhir_observations(fhir.get("Observation", pd.DataFrame())),
        "mri_manifest":       transform_mri_manifest(ingested.get("mri_manifest", pd.DataFrame())),
    }

    logger.info("TRANSFORM LAYER — COMPLETE")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    from ingestion import run_ingestion
    raw = run_ingestion()
    transformed = run_transform(raw)
    for k, df in transformed.items():
        print(f"  {k}: {len(df)} rows")
