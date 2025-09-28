import os
import hashlib
import pandas as pd
from datetime import datetime, timezone
import yaml

# === Load paths from YAML ===
with open("paths.yaml", "r") as f:
    paths_cfg = yaml.safe_load(f)

DATA_FOLDER = paths_cfg['folders']['data']
RAW_FOLDER = paths_cfg['folders']['raw']
AUDIT_PATH = paths_cfg['files']['audit_log']

# Ensure folders exist
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(RAW_FOLDER, exist_ok=True)
os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)

print(f"Folders ready: DATA_FOLDER={DATA_FOLDER}, RAW_FOLDER={RAW_FOLDER}, AUDIT_PATH={AUDIT_PATH}")

# === Helper functions ===
def checksum_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def parse_filename_meta(fname):
    """Extract metadata from filename: format expected: MONTHYEAR_TABLENAME_YYYY_MM_DD_HH_MM_SS.csv"""
    fname_no_ext = os.path.splitext(fname)[0]
    parts = fname_no_ext.split("_")
    meta = {"filename": fname}
    try:
        meta["sourcemonthyear_key"] = parts[0]
        meta["tablename"] = parts[1]
        dtparts = list(map(int, parts[2:]))  # Convert remaining parts to integers
        dt = datetime(*dtparts)  # datetime(YYYY, MM, DD, HH, MM, SS)
        meta["sourcefiledate_key"] = dt
        meta["file_datetime"] = dt
    except Exception as e:
        print(f"[!] Failed to parse datetime from filename: {fname}. Error: {e}")
        meta["sourcemonthyear_key"] = None
        meta["sourcefiledate_key"] = None
        meta["file_datetime"] = None
    return meta

# === Process Files ===
audit_records = []

for fname in os.listdir(DATA_FOLDER):
    src_path = os.path.join(DATA_FOLDER, fname)

    # Skip directories
    if not os.path.isfile(src_path):
        continue

    dst_path = os.path.join(RAW_FOLDER, fname)

    # Read raw bytes
    with open(src_path, "rb") as f:
        raw_bytes = f.read()

    # Convert encoding to UTF-8
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = raw_bytes.decode("cp1252", errors="replace")
    converted_bytes = text.encode("utf-8")

    # Save to raw folder
    with open(dst_path, "wb") as f:
        f.write(converted_bytes)

    # Extract metadata
    meta = parse_filename_meta(fname)
    meta["file_size_bytes"] = os.path.getsize(dst_path)
    meta["checksum"] = checksum_bytes(converted_bytes)
    meta["status"] = "downloaded"
    meta["ingested_at"] = datetime.now(timezone.utc)

    # Try reading CSV to count rows
    try:
        df = pd.read_csv(dst_path, encoding="utf-8", sep="|")
        meta["rows_read"] = len(df)
        meta["rows_loaded"] = 0
        meta["rows_failed"] = 0  # Not loaded yet
    except Exception as e:
        print(f"[!] Failed to read CSV file {dst_path}: {e}")
        meta["rows_read"] = 0
        meta["rows_loaded"] = 0
        meta["rows_failed"] = 0

    audit_records.append(meta)

print("✅ Processing complete.")

# === Save Audit Log ===
df_audit = pd.DataFrame(audit_records)
pd.set_option('display.max_columns', None)
display(df_audit)

# Save to CSV
df_audit.to_csv(AUDIT_PATH, index=False)
print(f"✅ Audit log saved to: {AUDIT_PATH}")
