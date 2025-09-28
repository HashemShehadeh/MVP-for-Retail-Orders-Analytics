import os
import pandas as pd
import hashlib
import datetime
import zipfile
from sqlalchemy import create_engine, text
import yaml

# ==========================
# === LOAD CONFIG FILES ===
# ==========================

# Paths
with open("paths.yaml", "r") as f:
    paths_cfg = yaml.safe_load(f)

BASE_DIR = paths_cfg['base']
LZ_DIR = paths_cfg['folders']['raw']
STG_DIR = paths_cfg['folders']['staged']
RAW_PROCESSED_DIR = paths_cfg['folders']['processed']
ARCHIVE_DIR = paths_cfg['folders']['archived']
AUDIT_PATH = paths_cfg['files']['audit_log']
MERGE_AUDIT_PATH = paths_cfg['files']['merge_audit_log']

os.makedirs(STG_DIR, exist_ok=True)
os.makedirs(RAW_PROCESSED_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# Schema
with open("./orders/schema/schema.yaml", "r") as f:
    schema_cfg = yaml.safe_load(f)

schema_columns = schema_cfg["columns"]  # { "Order ID": "VARCHAR", ... }

# Postgres connection
POSTGRES_CONN_STRING = "postgresql+psycopg2://user:password@host:port/dbname"
engine = create_engine(POSTGRES_CONN_STRING)

# ==========================
# === HELPERS ============
# ==========================

def parse_filename_meta(filename):
    name = os.path.basename(filename).replace(".csv","")
    parts = name.split("_")
    try:
        sourcemonthyear_key = parts[0]
        dt = datetime.datetime(*map(int, parts[2:]))  # YYYY, MM, DD, HH, MM, SS
        return sourcemonthyear_key, dt
    except Exception as e:
        print(f"[!] Failed to parse {filename}: {e}")
        return None, None

def row_hash(row):
    vals = [str(v).strip().upper() if pd.notna(v) and str(v).strip()!="" else "<NULL>" for v in row]
    payload = "||".join(vals)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def zip_and_archive_file(original_path):
    if not os.path.exists(original_path):
        return
    base = os.path.basename(original_path)
    zip_path = os.path.join(ARCHIVE_DIR, f"{os.path.splitext(base)[0]}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(original_path, arcname=base)
    os.remove(original_path)
    print(f"📦 Archived {original_path} → {zip_path}")

def cast_df_to_schema(df, schema):
    for col, dtype in schema.items():
        if col in df.columns:
            if dtype == "INTEGER":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "NUMERIC":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "DATE":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:  # VARCHAR
                df[col] = df[col].astype(str)
    return df

# ==========================
# === LOAD AUDIT LOGS ===
# ==========================

audit_df = pd.read_csv(AUDIT_PATH) if os.path.exists(AUDIT_PATH) else pd.DataFrame()
merge_audit_df = pd.read_csv(MERGE_AUDIT_PATH) if os.path.exists(MERGE_AUDIT_PATH) else pd.DataFrame()

# ==========================
# === STAGING PIPELINE ===
# ==========================

for fname in os.listdir(LZ_DIR):
    fpath = os.path.join(LZ_DIR, fname)
    if not fname.lower().endswith(".csv") or not os.path.isfile(fpath):
        continue

    print(f"\n📄 Processing file: {fname}")
    sourcemonth, sourcefiledate = parse_filename_meta(fname)
    ingested_at = datetime.datetime.now(datetime.timezone.utc)

    try:
        df_raw = pd.read_csv(fpath, sep="|")
    except Exception as e:
        print(f"[!] Failed to read {fname}: {e}")
        continue

    rows_total = len(df_raw)
    df_raw = cast_df_to_schema(df_raw, schema_columns)

    # Enrich metadata
    df_raw["Row ID"] = range(1, rows_total+1)
    df_raw["sourcefilename"] = fname
    df_raw["sourcemonthyear_key"] = sourcemonth
    df_raw["sourcefiledate_key"] = sourcefiledate
    df_raw["ingested_at"] = ingested_at

    # Write to Postgres staging
    df_raw.to_sql("stg_orders", engine, if_exists="append", index=False)
    print(f"[✓] Loaded {rows_total} rows into Postgres staging: stg_orders")

    # Update audit log
    audit_record = {
        "filename": fname,
        "sourcemonthyear_key": sourcemonth,
        "sourcefiledate_key": sourcefiledate,
        "file_datetime": sourcefiledate,
        "file_size_bytes": os.path.getsize(fpath),
        "checksum": "",  # optional: compute hash of file
        "status": "staged",
        "ingested_at": ingested_at,
        "rows_read": rows_total,
        "rows_loaded": rows_total,
        "rows_failed": 0
    }
    audit_df = pd.concat([audit_df, pd.DataFrame([audit_record])], ignore_index=True)
    audit_df.to_csv(AUDIT_PATH, index=False)

    # Move raw file to processed
    processed_path = os.path.join(RAW_PROCESSED_DIR, fname)
    os.rename(fpath, processed_path)
    print(f"[→] Moved raw file to processed: {processed_path}")

# ==========================
# === MERGE TO PRODUCTION ===
# ==========================

with engine.begin() as conn:
    # Create PRD table if not exists (all schema columns + metadata)
    columns_sql = ", ".join([f'"{c}" {schema_columns[c]}' for c in schema_columns])
    extra_cols = """,
        hash_key VARCHAR,
        changetype CHAR(1),
        is_deleted BOOLEAN,
        last_modified_at TIMESTAMP
    """
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS prd_orders (
            {columns_sql}
            {extra_cols}
        )
    """))

    # Get distinct months from staging
    months = pd.read_sql("SELECT DISTINCT sourcemonthyear_key FROM stg_orders", conn)
    for month in months["sourcemonthyear_key"]:
        run_started = datetime.datetime.utcnow()
        df_stg = pd.read_sql(text("SELECT * FROM stg_orders WHERE sourcemonthyear_key=:month"), conn, params={"month": month})

        # Hash & metadata
        df_stg["hash_key"] = df_stg.apply(row_hash, axis=1)
        df_stg["changetype"] = "I"
        df_stg["is_deleted"] = False
        df_stg["last_modified_at"] = datetime.datetime.utcnow()

        # Fetch existing PRD for same month
        df_prd = pd.read_sql(text("SELECT * FROM prd_orders WHERE sourcemonthyear_key=:month"), conn, params={"month": month})
        existing_hash = df_prd.set_index("Row ID")["hash_key"].to_dict() if not df_prd.empty else {}

        insert_rows, update_rows = [], []
        seen = set()
        inserted = updated = deleted = 0

        for _, r in df_stg.iterrows():
            rid = r["Row ID"]
            seen.add(rid)
            if rid not in existing_hash:
                insert_rows.append(r)
                inserted += 1
            elif existing_hash[rid] != r["hash_key"]:
                update_rows.append(r)
                updated += 1

        # Mark deleted
        if not df_prd.empty:
            mask_to_delete = ~df_prd["Row ID"].isin(seen)
            deleted = mask_to_delete.sum()
            if deleted > 0:
                conn.execute(text("""
                    UPDATE prd_orders
                    SET is_deleted=true, changetype='D', last_modified_at=:now
                    WHERE sourcemonthyear_key=:month AND "Row ID" = ANY(:ids)
                """), {"now": datetime.datetime.utcnow(), "month": month, "ids": df_prd.loc[mask_to_delete, "Row ID"].tolist()})

        # Insert new rows
        if insert_rows:
            pd.DataFrame(insert_rows).to_sql("prd_orders", conn, if_exists="append", index=False)

        # Update changed rows
        if update_rows:
            for r in update_rows:
                update_sql = text("""
                UPDATE prd_orders SET
                    hash_key=:hash_key,
                    changetype='U',
                    is_deleted=false,
                    last_modified_at=:last_modified_at
                WHERE "Row ID"=:row_id AND sourcemonthyear_key=:month
                """)
                conn.execute(update_sql, {
                    "hash_key": r["hash_key"],
                    "last_modified_at": datetime.datetime.utcnow(),
                    "row_id": r["Row ID"],
                    "month": month
                })

        # Update merge audit
        run_finished = datetime.datetime.utcnow()
        merge_record = {
            "filename": f"month_{month}",
            "target_table": "prd_orders",
            "inserted_count": inserted,
            "updated_count": updated,
            "marked_deleted_count": deleted,
            "run_started": run_started,
            "run_finished": run_finished
        }
        merge_audit_df = pd.concat([merge_audit_df, pd.DataFrame([merge_record])], ignore_index=True)
        merge_audit_df.to_csv(MERGE_AUDIT_PATH, index=False)
        print(f"✅ Merge month {month} → Inserts={inserted}, Updates={updated}, Deletes={deleted}")

# ==========================
# === ARCHIVE PROCESSED FILES ===
# ==========================
for f in os.listdir(RAW_PROCESSED_DIR):
    zip_and_archive_file(os.path.join(RAW_PROCESSED_DIR, f))

print("\n🎯 Full Postgres staging → production cycle complete.")
