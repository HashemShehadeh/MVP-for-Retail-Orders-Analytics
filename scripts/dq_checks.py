import os
import pandas as pd
import datetime
from dateutil import parser
from sqlalchemy import create_engine, text
import yaml

# ==============================
# === CONFIGURATION & PATHS ===
# ==============================
with open("paths.yaml", "r") as f:
    paths_cfg = yaml.safe_load(f)

STG_DIR = paths_cfg['folders']['staged']
PRD_FILE = os.path.join(paths_cfg['folders']['prd'], "prd_orders.csv")
REPORT_DIR = paths_cfg['folders']['dq_report']
os.makedirs(REPORT_DIR, exist_ok=True)

timestamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
CSV_VIOLATIONS_PATH_STG = os.path.join(REPORT_DIR, f"dq_violations_stg_{timestamp}.csv")
EXCEL_REPORT_PATH_STG = os.path.join(REPORT_DIR, f"dq_report_stg_{timestamp}.xlsx")
CSV_VIOLATIONS_PATH_PRD = os.path.join(REPORT_DIR, f"dq_violations_prd_{timestamp}.csv")
EXCEL_REPORT_PATH_PRD = os.path.join(REPORT_DIR, f"dq_report_prd_{timestamp}.xlsx")

# Postgres connection
POSTGRES_CONN_STRING = "postgresql+psycopg2://user:password@host:port/dbname"
engine = create_engine(POSTGRES_CONN_STRING)

# ==============================
# === LOAD DQ CHECKS ==========
# ==============================
with open("./orders/dqchecks/checks.yaml", "r") as f:
    dq_checks_cfg = yaml.safe_load(f)

CHECKS = dq_checks_cfg["checks"]

# ==============================
# === HELPERS =================
# ==============================
def is_not_ddmmyyyy(val):
    if pd.isna(val) or str(val).strip() == '':
        return False
    try:
        dt = parser.parse(str(val), dayfirst=True)
        normalized = dt.strftime('%d-%m-%Y')
        return str(val).strip() != normalized
    except Exception:
        return True

def run_dq_on_df(df, target_name="STG"):
    all_violations = []
    all_summary = []

    df.columns = [c.lower().strip() for c in df.columns]

    for check in CHECKS:
        check_name, check_func_name = check['name'], check['func']
        if check_name.startswith("order_date") or check_name.startswith("ship_date"):
            condition = df[check_func_name].apply(is_not_ddmmyyyy)
        else:
            condition = eval(check_func_name)(df)  # Use lambda from config

        if condition is None or not isinstance(condition, pd.Series):
            continue

        violating_rows = df[condition].copy()
        count = len(violating_rows)

        if count > 0:
            violating_rows["violation_type"] = check_name
            all_violations.append(violating_rows)

        all_summary.append({
            "check": check_name,
            "violation_count": count
        })

    return all_violations, all_summary

def generate_dq_report(violations, summary, csv_path, excel_path):
    if violations:
        df_violations = pd.concat(violations, ignore_index=True)
        df_violations.to_csv(csv_path, index=False)
        print(f"[✓] Violations CSV written: {csv_path}")
    else:
        print("[✓] No violations found.")

    # Excel report
    import xlsxwriter
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        df_summary = pd.DataFrame(summary)
        if df_summary.empty:
            df_summary = pd.DataFrame(columns=["check", "violation_count"])
        df_summary.to_excel(writer, sheet_name="Inconsistencies_Summary", index=False)

        if violations:
            df_examples = pd.concat(violations, ignore_index=True).head(100)
            df_examples.to_excel(writer, sheet_name="Inconsistencies_Examples", index=False)
        else:
            pd.DataFrame(columns=["violation_type"]).to_excel(writer, sheet_name="Inconsistencies_Examples", index=False)

        # Quality Report
        quality_report = pd.DataFrame([{
            "file": "summary",
            "message": f"DQ run completed",
            "run_ts": datetime.datetime.utcnow()
        }])
        quality_report.to_excel(writer, sheet_name="Quality_Report", index=False)
    print(f"[✓] Excel DQ report written: {excel_path}")

# ==============================
# === RUN DQ FOR STG ==========
# ==============================
with engine.connect() as conn:
    df_stg = pd.read_sql("SELECT * FROM stg_orders", conn)
    violations, summary = run_dq_on_df(df_stg, "STG")
    generate_dq_report(violations, summary, CSV_VIOLATIONS_PATH_STG, EXCEL_REPORT_PATH_STG)

# ==============================
# === RUN DQ FOR PRD ==========
# ==============================
with engine.connect() as conn:
    df_prd = pd.read_sql("SELECT * FROM prd_orders", conn)
    violations, summary = run_dq_on_df(df_prd, "PRD")
    generate_dq_report(violations, summary, CSV_VIOLATIONS_PATH_PRD, EXCEL_REPORT_PATH_PRD)
