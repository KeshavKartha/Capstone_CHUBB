"""
Synthetic Census Raw-File Generator (unpartitioned Parquet parts + manifest)
Produces N unpartitioned Parquet files that together contain the full dataset.
Each part intentionally includes small schema or content variations to simulate
real-world heterogeneous source artifacts that your Airflow/Databricks ingestion must reconcile.

Requirements: pandas, numpy, pyarrow
Run: python generate_raw_parquet_parts.py
"""

import os
import json
import uuid
import random
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# ====== CONFIG ======
OUT_DIR = "synthetic_raw_files"     # directory to write raw Parquet files + manifest
TOTAL_ROWS = 100_000                # total rows across all parts (adjust downward for small repos)
N_PARTS = 4                         # number of unpartitioned Parquet files to create (3-4 recommended)
SEED = 2025
YEARS = [2010, 2015, 2020]          # census snapshot years to include in the raw data
NUM_REGIONS = 120                   # number of canonical regions (geoid values will be 1..NUM_REGIONS)
MANIFEST_NAME = "manifest.json"
# ====================

random.seed(SEED)
np.random.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)

# Helper functions for realistic distributions and anomalies
def sample_age(rng):
    """Sample an age using a three-component mixture to resemble demographic pyramids."""
    p = rng.random()
    if p < 0.22:
        return int(rng.integers(0, 15))
    elif p < 0.78:
        return int(rng.integers(15, 55))
    else:
        return int(rng.integers(55, 95))

def introduce_typo(s, rng):
    """Introduce a small typo or abbreviation with low probability."""
    if s is None or len(s) < 4:
        return s
    if rng.random() < 0.06:
        i = rng.integers(0, len(s))
        # randomly delete or replace
        if rng.random() < 0.5:
            return s[:i] + s[i+1:]
        else:
            repl = chr(97 + (rng.integers(0, 26)))
            return s[:i] + repl + s[i+1:]
    # occasionally return an abbreviation or alternate form
    if rng.random() < 0.03:
        parts = s.split("-")
        return parts[-1] if parts else s
    return s

# Plan rows per year with simple weighting (more recent year heavier)
year_weights = np.array([0.2, 0.3, 0.5])
year_weights = year_weights / year_weights.sum()
rows_per_year = (year_weights * TOTAL_ROWS).astype(int)
# adjust to exact total
rows_per_year[-1] += TOTAL_ROWS - rows_per_year.sum()

# Build the full dataset in memory in streaming batches then split to parts
all_rows = []
rng = np.random.default_rng(SEED)

education_choices = ["NoSchool", "Primary", "Secondary", "Tertiary", "Postgraduate"]
sex_choices = ["Male", "Female", "Other"]
ethnicity_codes = [f"ETH{c:02d}" for c in range(1, 21)]
employment_status_choices = ["Employed", "Unemployed", "Student", "Retired", "Inactive"]
employment_type_choices = ["Formal", "Informal", "Self-employed", "Casual"]

row_counter = 0
for yi, year in enumerate(YEARS):
    n = rows_per_year[yi]
    for _ in range(n):
        row_counter += 1
        geoid = int(rng.integers(1, NUM_REGIONS + 1))
        # person_id deterministic seeded uuid to ensure reproducibility
        person_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{SEED}-{row_counter}"))
        household_id = f"H-{geoid}-{rng.integers(1, max(2, n//20))}"
        dob_age = sample_age(rng)
        # approximate DOB day/month randomness; ensure valid date strings but as ISO strings
        birth_year = year - dob_age
        birth_month = int(rng.integers(1,13))
        birth_day = int(min(28, rng.integers(1,29)))
        date_of_birth = f"{birth_year:04d}-{birth_month:02d}-{birth_day:02d}"
        sex = rng.choice(sex_choices)
        ethnicity_code = rng.choice(ethnicity_codes)
        # education correlated with age
        if dob_age < 6:
            education = "NoSchool"
        else:
            eprob = rng.random()
            if eprob < 0.25:
                education = "Primary"
            elif eprob < 0.7:
                education = "Secondary"
            elif eprob < 0.92:
                education = "Tertiary"
            else:
                education = "Postgraduate"
        literacy = True if education in ("Secondary", "Tertiary", "Postgraduate") or rng.random() < 0.6 else False
        # employment status heuristic
        if dob_age < 16:
            employment_status = "Student" if rng.random() < 0.7 else "Inactive"
        elif dob_age > 65:
            employment_status = "Retired" if rng.random() < 0.8 else "Inactive"
        else:
            employment_status = rng.choice(employment_status_choices, p=[0.55, 0.12, 0.06, 0.02, 0.25])
        annual_income = 0.0
        employment_type = None
        industry_code = None
        if employment_status == "Employed":
            # log-normal-ish distribution, then scaled by region urbanity heuristic
            base_income = float(np.exp(rng.normal(10.2, 0.95)))
            urban_premium = 1.25 if (geoid % 5 == 0) else 0.85
            annual_income = float(base_income * urban_premium)
            # occasional top-coding
            if rng.random() < 0.01:
                annual_income = min(annual_income, 200000.0)
            employment_type = rng.choice(employment_type_choices)
            industry_code = f"IND{int(rng.integers(1,300)):03d}"
        marital_status = rng.choice(["Single", "Married", "Divorced", "Widowed"])
        migration_status = rng.choice(["Native", "Internal Migrant", "International Migrant"], p=[0.9, 0.085, 0.015])
        arrival_year = None
        if migration_status != "Native" and rng.random() < 0.85:
            arrival_year = int(rng.integers(year-20, year+1))
        is_head_of_household = bool(rng.random() < 0.12)
        record_confidence_score = round(float(rng.random()), 3)
        enumeration_source = rng.choice(["AdminRegister", "SurveySample", "FieldEnumeration", "MobileUpdate"], p=[0.45,0.15,0.35,0.05])
        last_updated = datetime(year, int(rng.integers(1,13)), int(min(28, rng.integers(1,29)))).isoformat()

        canonical_region_name = f"Region-{geoid:04d}"
        reported_region_name = introduce_typo(canonical_region_name, rng)  # inject realistic typos/variants

        # small fraction missingness patterns (e.g., income missing more for informal)
        if employment_status != "Employed" and rng.random() < 0.92:
            annual_income = 0.0
        if rng.random() < 0.01:
            # MCAR: drop ethnicity randomly
            ethnicity_code = None

        # assemble canonical row dictionary (this will be our "base" schema)
        row = {
            "person_id": person_id,
            "household_id": household_id,
            "geoid": geoid,
            "region_code_legacy": f"LEG-{geoid}-{int(rng.integers(1,9999))}",
            "region_name_reported": reported_region_name,
            "census_year": year,
            "date_of_birth": date_of_birth,
            "age": dob_age,
            "sex": sex,
            "ethnicity_code": ethnicity_code,
            "education_level": education,
            "literacy": literacy,
            "employment_status": employment_status,
            "employment_type": employment_type,
            "industry_code": industry_code,
            "annual_income_local": float(round(annual_income, 2)),
            "marital_status": marital_status,
            "migration_status": migration_status,
            "arrival_year": arrival_year,
            "is_head_of_household": is_head_of_household,
            "record_confidence_score": record_confidence_score,
            "enumeration_source": enumeration_source,
            "last_updated": last_updated
        }
        all_rows.append(row)

# Convert to DataFrame
df_full = pd.DataFrame(all_rows)

# Shuffle then split into N parts (disjoint person_id ranges)
df_full = df_full.sample(frac=1, random_state=SEED).reset_index(drop=True)
part_size = int(np.ceil(TOTAL_ROWS / N_PARTS))

manifest = {
    "generated_at": datetime.utcnow().isoformat() + "Z",
    "seed": SEED,
    "total_rows": int(TOTAL_ROWS),
    "n_parts": N_PARTS,
    "parts": []
}

# To make ingestion interesting: slightly vary the schema across parts
# strategies: rename 'sex'->'gender' in one part, add 'national_id' to another, change 'annual_income_local' name in one part
for part_idx in range(N_PARTS):
    start = part_idx * part_size
    end = min((part_idx + 1) * part_size, TOTAL_ROWS)
    df_part = df_full.iloc[start:end].copy()
    # apply schema variant per part
    if part_idx == 0:
        # canonical part: keep base schema
        filename = f"raw_part_{part_idx+1:02d}.parquet"
        df_to_write = df_part
    elif part_idx == 1:
        # rename 'sex' -> 'gender' and drop 'record_confidence_score' to simulate a drift
        df_to_write = df_part.rename(columns={"sex": "gender"})
        df_to_write = df_to_write.drop(columns=["record_confidence_score"])
        filename = f"raw_part_{part_idx+1:02d}_gender.drift.parquet"
    elif part_idx == 2:
        # add a 'national_id' column with partial coverage (simulate admin register with national ID)
        df_to_write = df_part.copy()
        # introduce national_id for ~70% of rows in this part
        df_to_write["national_id"] = [str(uuid.uuid5(uuid.NAMESPACE_DNS, f"nid-{SEED}-{i}")) if rng.random() < 0.7 else None for i in range(len(df_to_write))]
        filename = f"raw_part_{part_idx+1:02d}_admin.parquet"
    else:
        # change income column name to 'income' and cast to integer for this part (simulates different type coding)
        df_to_write = df_part.copy()
        df_to_write = df_to_write.rename(columns={"annual_income_local": "income"})
        # integer cast with top-coding applied
        df_to_write["income"] = df_to_write["income"].fillna(0.0).astype(float).round().astype("Int64")
        filename = f"raw_part_{part_idx+1:02d}_income_int.parquet"

    out_path = os.path.join(OUT_DIR, filename)
    # Ensure we write Parquet as unpartitioned single-file per part for clarity (pandas/pyarrow will create a single file)
    df_to_write.to_parquet(out_path, index=False, engine="pyarrow")
    # record metadata for manifest
    part_info = {
        "filename": filename,
        "rows": int(len(df_to_write)),
        "generated_from_row_range": [int(start), int(end)],
        "schema_columns": list(df_to_write.columns),
        "notes": ""
    }
    if part_idx == 1:
        part_info["notes"] = "schema drift: 'sex' renamed to 'gender'; missing record_confidence_score"
    if part_idx == 2:
        part_info["notes"] = "admin-like file: includes 'national_id' for ~70% rows"
    if part_idx == 3:
        part_info["notes"] = "income column named 'income' and stored as integers (top-coded); type drift"
    manifest["parts"].append(part_info)

# Write manifest
with open(os.path.join(OUT_DIR, MANIFEST_NAME), "w", encoding="utf-8") as fh:
    json.dump(manifest, fh, indent=2)

print(f"Raw parts written to {OUT_DIR}. Manifest: {MANIFEST_NAME}")

