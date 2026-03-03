# 🧠 NeuroGuard
### AI-Powered Brain Tumor Clinic Management System

> CIS 8395 — AI for Data-Driven Experience | Georgia State University | Spring 2026

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![PyTorch](https://img.shields.io/badge/PyTorch-2.x-orange.svg)](https://pytorch.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.x-red.svg)](https://streamlit.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://postgresql.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Overview

NeuroGuard is a prototype clinical data management system designed for a brain tumor specialty clinic. It integrates a full data pipeline, enterprise-grade data governance, PHI-compliant architecture, and an AI-assisted MRI tumor detection model — built as a capstone project demonstrating end-to-end big data analytics in a healthcare context.

**This is a prototype for academic purposes. Not intended for clinical use.**

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│  Synthetic Patient Data  |  Kaggle Brain MRI  |  Simulated FHIR R4  │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ETL / DATA PIPELINE                           │
│      Ingestion  →  Transformation  →  De-identification  →  Storage  │
│            PostgreSQL (AES-256)  +  MRI File Store                   │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
         Identified path                De-identified path
         (clinical views)               (analytics only)
                    │                           │
                    ▼                           ▼
┌───────────────────────────┐   ┌───────────────────────────────────┐
│   DATA GOVERNANCE LAYER   │   │         ANALYTICS LAYER           │
│  MDM / MPI  |  Lineage    │   │  Descriptive  |  Diagnostic       │
│  Quality    |  RBAC       │   │  Predictive (feeds ML layer)      │
│  Audit Log  |  HIPAA      │   └───────────────────────────────────┘
└───────────────────────────┘                   │
                                                ▼
                                ┌───────────────────────────────────┐
                                │           AI / ML LAYER           │
                                │  CNN (VGG16 + Grad-CAM)           │
                                │  XGBoost Risk Scorer              │
                                └───────────────┬───────────────────┘
                                                │
                                                ▼
                                ┌───────────────────────────────────┐
                                │       PRESENTATION LAYER          │
                                │   Streamlit  |  RBAC-enforced     │
                                └───────────────────────────────────┘
```

---

## Features

- **Patient Portal** — intake, demographics, appointment scheduling, medical history
- **Epic FHIR Integration** — simulated HL7 FHIR R4 patient transfer records
- **ETL Pipeline** — ingestion, cleaning, standardization, and de-identification
- **Data Governance** — MDM/MPI, data lineage tracking, quality dashboard, audit logging
- **RBAC** — 5 role tiers: Clinician, Admin, Data Steward, Engineer, Auditor
- **PHI Compliance** — Safe Harbor de-identification, AES-256 at rest, TLS in transit
- **CNN Tumor Detection** — VGG16 transfer learning with Grad-CAM heatmap overlay
- **Risk Scoring** — XGBoost treatment urgency prediction
- **Analytics Dashboard** — descriptive, diagnostic, and predictive layers

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Generation | Python / Faker |
| FHIR Simulation | fhir.resources (HL7 FHIR R4) |
| ETL Pipeline | Python / pandas |
| Storage | PostgreSQL |
| CNN Model | PyTorch + torchvision (VGG16) |
| Explainability | pytorch-grad-cam |
| Risk Scoring | XGBoost / scikit-learn |
| Dashboard | Streamlit |
| Training Environment | Kaggle Notebooks (P100 GPU) + Apple M4 (MPS) |

---

## Project Structure

```
NeuroGuard/
│
├── data/
│   ├── raw/                    # Raw synthetic patient CSVs
│   ├── processed/              # Cleaned, transformed data
│   ├── mri/                    # MRI image store (mapped to patient IDs)
│   └── fhir/                   # Simulated FHIR R4 JSON bundles
│
├── etl/
│   ├── ingestion.py            # Data ingestion scripts
│   ├── transform.py            # Cleaning, standardization, dedup
│   ├── deidentify.py           # Safe Harbor PHI de-identification
│   └── load.py                 # PostgreSQL loader
│
├── governance/
│   ├── mpi.py                  # Master Patient Index logic
│   ├── lineage.py              # Data lineage tracker
│   ├── quality.py              # Data quality checks and scoring
│   └── audit.py                # Audit log writer
│
├── models/
│   ├── cnn/
│   │   ├── train.py            # VGG16 transfer learning training script
│   │   ├── predict.py          # Inference on new MRI scans
│   │   └── gradcam.py          # Grad-CAM heatmap generation
│   └── risk/
│       ├── train.py            # XGBoost risk scorer training
│       └── predict.py          # Risk score inference
│
├── analytics/
│   ├── descriptive.py          # Volume trends, no-show rates, scan rates
│   ├── diagnostic.py           # Correlation analysis, quality scoring
│   └── predictive.py           # Feeds from ML layer outputs
│
├── dashboard/
│   ├── app.py                  # Main Streamlit app entry point
│   ├── auth.py                 # RBAC session management
│   ├── views/
│   │   ├── clinician.py        # Full patient record + MRI upload view
│   │   ├── admin.py            # Appointments + analytics view
│   │   ├── steward.py          # Governance panel view
│   │   └── auditor.py          # Audit trail view
│   └── components/
│       ├── patient_card.py     # Patient profile component
│       ├── mri_viewer.py       # MRI upload + CNN result display
│       └── gradcam_display.py  # Grad-CAM heatmap renderer
│
├── db/
│   ├── schema.sql              # PostgreSQL schema (all tables)
│   └── seed.py                 # Seed DB with synthetic data
│
├── notebooks/
│   └── cnn_training.ipynb      # Kaggle-ready VGG16 training notebook
│
├── docs/
│   ├── architecture.md         # Full architecture documentation
│   ├── governance.md           # MDM, lineage, RBAC, PHI design
│   └── phi_compliance.md       # HIPAA Safe Harbor checklist
│
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Dataset

| Attribute | Detail |
|---|---|
| Source | [Kaggle — Brain MRI Images for Brain Tumor Detection](https://www.kaggle.com/datasets/navoneel/brain-mri-images-for-brain-tumor-detection) |
| Task | Binary classification — Tumor vs. No Tumor |
| Prototype subset | ~500 images (stratified, balanced classes) |
| Format | JPG (no DICOM preprocessing required) |
| License | Public / CC |

> Full dataset (~3,000 images) recommended for production. Prototype uses a stratified 500-image subset for feasibility within academic timeline.

---

## Model Details

### CNN — Tumor Detection
- **Architecture:** VGG16 with frozen ImageNet weights, fine-tuned classification head
- **Framework:** PyTorch with MPS backend (Apple M4) / Kaggle P100 for full training
- **Input:** 224×224 RGB MRI scan image
- **Output:** Binary prediction (Tumor / No Tumor) + confidence score
- **Explainability:** Grad-CAM heatmap highlighting regions of activation
- **Target accuracy:** 85–92% on prototype subset

### XGBoost — Risk Scorer
- **Input features:** Patient demographics, diagnosis history, prior treatments
- **Output:** Treatment urgency score — Low / Medium / High
- **Library:** XGBoost + scikit-learn pipeline

---

## Data Governance Design

### Master Patient Index (MDM)
Single patient identity maintained across all hospital transfers. Each patient receives an internal token ID — real identifiers are never used downstream.

### Data Lineage
Every record is tagged with its source system, transformation steps, and destination. Queryable via the governance dashboard.

### RBAC — Role Matrix

| Role | Patient Data | MRI / AI | Governance | Analytics |
|---|---|---|---|---|
| Clinician | Full | Full | None | None |
| Admin | Limited | None | None | Full |
| Data Steward | None | None | Full | Anon. |
| Engineer | None | None | Logs | None |
| Auditor | None | None | Read | Anon. |

### PHI Compliance
- **De-identification method:** HIPAA Safe Harbor (18 identifiers removed/generalized)
- **Data fork:** Identified data → clinical views only; de-identified → analytics layer
- **Encryption:** AES-256 at rest (PostgreSQL), TLS in transit (FHIR feed)
- **Audit log:** Every record access, MRI upload, and role change is logged

---

## Setup

### Prerequisites
- Python 3.10+
- PostgreSQL 15+
- Node.js (optional, for tooling)

### Installation

```bash
# Clone the repository
git clone https://github.com/gsatyabrata99/NeuroGuard.git
cd NeuroGuard

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# Initialize database
psql -U postgres -f db/schema.sql

# Seed with synthetic patient data
python db/seed.py

# Run the dashboard
streamlit run dashboard/app.py
```

### Training the CNN (optional — pre-trained weights included)

```bash
# Local training (Apple M4 MPS)
python models/cnn/train.py --device mps --epochs 20 --subset 500

# Or open the Kaggle notebook for cloud training
# notebooks/cnn_training.ipynb
```

---

## Team

| Name | Role | Responsibilities |
|---|---|---|
| Ganesh Satyabrata |  |  |
| Janish Pranesh Kumar |  |  |
| Robert Max |  |  |

---

## Deliverables

- [ ] ETL pipeline with full audit logging
- [ ] PostgreSQL schema with MPI and audit_log tables
- [ ] De-identification pipeline (Safe Harbor)
- [ ] VGG16 CNN — trained on Kaggle, deployed locally
- [ ] Grad-CAM heatmap visualization
- [ ] XGBoost risk scorer
- [ ] Streamlit dashboard with RBAC-enforced views
- [ ] Kaggle training notebook (reproducible)
- [ ] Paper 1 — Data source, ETL, storage (due before Apr 13)
- [ ] Final paper — Full system report (due Apr 27)

---

## Academic Context

| Item | Detail |
|---|---|
| Course | CIS 8395 — AI for Data-Driven Experience |
| Institution | Georgia State University |
| Semester | Spring 2026 |
| Concentration | MS Information Systems — AI & Data-Driven Analytics |

---

## Disclaimer

This project is built for academic purposes using synthetic patient data and publicly available, de-identified MRI datasets. It is not intended for clinical diagnosis or deployment in any healthcare setting. All architectural decisions referencing HIPAA and PHI compliance are for educational demonstration only.

---

*NeuroGuard — Georgia State University, CIS 8395, Spring 2026*
