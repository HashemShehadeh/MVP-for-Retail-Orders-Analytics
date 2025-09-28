import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# ==============================
# === CONFIG PATHS ============
# ==============================
with open("paths.yaml", "r") as f:
    import yaml
    paths_cfg = yaml.safe_load(f)

OUTPUT_DIR = paths_cfg["folders"]["mdm"]
AUDIT_DIR = paths_cfg["folders"]["audit"]
PRD_FILE = os.path.join(paths_cfg["folders"]["prd"], "prd_orders.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(AUDIT_DIR, exist_ok=True)

LAST_INGESTION_FILE = os.path.join(AUDIT_DIR, "mdm_last_ingestion.csv")
AUDIT_LOG_FILE = os.path.join(AUDIT_DIR, "mdm_audit_log.csv")

# ==============================
# === POSTGRES CONNECTION =====
# ==============================
POSTGRES_CONN_STRING = "postgresql+psycopg2://user:password@host:port/dbname"
engine = create_engine(POSTGRES_CONN_STRING)

# ==============================
# === AUDIT HELPERS ===========
# ==============================
def load_last_ingestion():
    if os.path.exists(LAST_INGESTION_FILE):
        return pd.read_csv(LAST_INGESTION_FILE)
    return pd.DataFrame(columns=["entity", "last_ingestion_at"])

def update_last_ingestion(entity, ts):
    df = load_last_ingestion()
    df = df[df["entity"] != entity]
    df = pd.concat([df, pd.DataFrame([{"entity": entity, "last_ingestion_at": ts}])])
    df.to_csv(LAST_INGESTION_FILE, index=False)

def log_audit(entity, key_id, operation, notes):
    ts = datetime.utcnow().isoformat()
    entry = {
        "entity": entity,
        "key_id": key_id,
        "operation": operation,
        "changed_at": ts,
        "notes": notes
    }
    if os.path.exists(AUDIT_LOG_FILE):
        pd.DataFrame([entry]).to_csv(AUDIT_LOG_FILE, mode='a', header=False, index=False)
    else:
        pd.DataFrame([entry]).to_csv(AUDIT_LOG_FILE, index=False)

# ==============================
# === LOAD INCREMENTAL DATA ====
# ==============================
def load_incremental_data(df_raw, entity, key_col):
    last_ingestion_df = load_last_ingestion()
    last_ts = last_ingestion_df[last_ingestion_df["entity"] == entity]["last_ingestion_at"]
    if last_ts.empty:
        return df_raw[df_raw["changetype"].isin(["I", "U", "D"])], None
    else:
        ts = pd.to_datetime(last_ts.values[0])
        df_raw["last_modified_at"] = pd.to_datetime(df_raw["last_modified_at"])
        df_inc = df_raw[
            (df_raw["last_modified_at"] > ts) &
            (df_raw["changetype"].isin(["I", "U", "D"]))
        ]
        return df_inc, ts

# ==============================
# === MDM PROCESSORS ==========
# ==============================
def generate_surrogate_keys(df, key_name, existing_df=None):
    df = df.copy()
    sk_col = f"{key_name}_sk"
    if sk_col in df.columns:
        df.drop(columns=[sk_col], inplace=True)

    if existing_df is not None and sk_col in existing_df.columns and not existing_df.empty:
        max_sk = existing_df[sk_col].max()
    else:
        max_sk = 0

    df.insert(0, sk_col, range(max_sk + 1, max_sk + 1 + len(df)))
    return df

def consolidate_entity(df_inc, key_col, important_fields, output_file, entity_name):
    df_inc = df_inc[df_inc[key_col].notna()]
    golden_path = os.path.join(OUTPUT_DIR, output_file)

    if os.path.exists(golden_path):
        df_existing = pd.read_csv(golden_path)
    else:
        df_existing = pd.DataFrame(columns=[key_col] + important_fields + ["source_list", "golden_score", "last_updated"])

    for key, group in df_inc.groupby(key_col):
        changetype = group["changetype"].iloc[-1]
        is_deleted = group["is_deleted"].iloc[-1]

        if changetype == "D" or is_deleted:
            df_existing = df_existing[df_existing[key_col] != key]
            log_audit(entity_name, key, "DELETE", "Marked as deleted")
            continue

        group = group.fillna("")
        group["score"] = group[important_fields].apply(lambda row: sum([1 for v in row if str(v).strip() != ""]), axis=1)
        best = group.sort_values(by="score", ascending=False).iloc[0]

        record = {key_col: key}
        for f in important_fields:
            record[f] = best[f]
        record["source_list"] = ",".join(group["Row ID"].astype(str).unique())
        record["golden_score"] = best["score"]
        record["last_updated"] = datetime.utcnow().isoformat()

        df_existing = df_existing[df_existing[key_col] != key]
        df_existing = pd.concat([df_existing, pd.DataFrame([record])])

        log_audit(entity_name, key, "UPSERT", "Inserted or updated")

    df_existing = generate_surrogate_keys(
        df_existing.sort_values(key_col),
        entity_name[:-1],
        existing_df=pd.read_csv(golden_path) if os.path.exists(golden_path) else None
    )
    df_existing.to_csv(golden_path, index=False)
    update_last_ingestion(entity_name, datetime.utcnow().isoformat())
    print(f"✅ MDM {entity_name.capitalize()} updated at {golden_path}")

# ==============================
# === MAIN EXECUTION ==========
# ==============================
if __name__ == "__main__":
    # Load PRD Orders from CSV or Postgres
    if os.path.exists(PRD_FILE):
        df_raw = pd.read_csv(PRD_FILE)
    else:
        with engine.connect() as conn:
            df_raw = pd.read_sql("SELECT * FROM prd_orders", conn)

    # --------- Customers ------------
    customer_fields = ["Customer Name", "Segment"]
    df_cust, _ = load_incremental_data(df_raw, "customers", "Customer ID")
    consolidate_entity(df_cust, "Customer ID", customer_fields, "customers.csv", "customers")

    # --------- Products ------------
    product_fields = ["Product Name", "Category", "Sub-Category"]
    df_prod, _ = load_incremental_data(df_raw, "products", "Product ID")
    consolidate_entity(df_prod, "Product ID", product_fields, "products.csv", "products")

    # --------- Countries/Locations ------------
    location_keys = ["Country", "Region", "State", "City", "Postal Code"]
    df_loc, _ = load_incremental_data(df_raw, "countries", "Country")
    df_loc[location_keys] = df_loc[location_keys].fillna("")
    df_loc["full_key"] = df_loc[location_keys].astype(str).agg("|".join, axis=1)
    consolidate_entity(df_loc, "full_key", location_keys, "countries.csv", "countries")
