import pandas as pd
import hashlib
import datetime
from sqlalchemy import create_engine, text

# ============================
# Configuration
# ============================
PG_CONN_STR = "postgresql+psycopg2://username:password@host:port/dbname"
engine = create_engine(PG_CONN_STR)

STG_SCHEMA = "stg"
PRD_SCHEMA = "prd"
MDM_SCHEMA = "mdm"
DW_SCHEMA = "dw"

# ============================
# Helper Functions
# ============================
def row_hash(vals):
    """Generate consistent row hash for SCD2 detection."""
    normalized = ["<NULL>" if v is None or str(v).strip()=="" else str(v).strip().upper() for v in vals]
    return hashlib.sha256("||".join(normalized).encode("utf-8")).hexdigest()

def read_table(schema, table, columns=None):
    cols = "*" if columns is None else ", ".join(f'"{c}"' for c in columns)
    return pd.read_sql(f'SELECT {cols} FROM {schema}."{table}"', engine)

def upsert_dim(df, table, bkeys):
    """Upsert dimension table with SCD2 logic."""
    df = df.copy()
    now = datetime.datetime.utcnow()
    df["hash_key"] = df[bkeys].apply(row_hash, axis=1)
    df["effective_from"] = now
    df["effective_to"] = None
    df["current_flag"] = True
    df["source_info"] = f"{MDM_SCHEMA}.{table}"

    # Batch upsert using temp table
    temp_table = f"temp_{table}"
    df.to_sql(temp_table, engine, schema=DW_SCHEMA, if_exists="replace", index=False)

    bkeys_str = ", ".join(f'"{col}"' for col in bkeys)
    col_names = ", ".join(f'"{c}"' for c in df.columns)
    update_cols = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in df.columns if c not in bkeys + ["surrogate_key"])

    sql = f"""
    INSERT INTO {DW_SCHEMA}."{table}" ({col_names})
    SELECT * FROM {DW_SCHEMA}."{temp_table}"
    ON CONFLICT ({bkeys_str}) DO UPDATE SET {update_cols};
    DROP TABLE {DW_SCHEMA}."{temp_table}";
    """
    engine.execute(text(sql))

def upsert_fact(df_fact, table, pk_cols=["Order ID"]):
    """Upsert fact table using primary key(s)."""
    df_fact = df_fact.copy()
    temp_table = f"temp_{table}"
    df_fact.to_sql(temp_table, engine, schema=DW_SCHEMA, if_exists="replace", index=False)

    pk_str = ", ".join(f'"{c}"' for c in pk_cols)
    update_cols = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in df_fact.columns if c not in pk_cols)

    sql = f"""
    INSERT INTO {DW_SCHEMA}."{table}" ({', '.join(f'"{c}"' for c in df_fact.columns)})
    SELECT * FROM {DW_SCHEMA}."{temp_table}"
    ON CONFLICT ({pk_str}) DO UPDATE SET {update_cols};
    DROP TABLE {DW_SCHEMA}."{temp_table}";
    """
    engine.execute(text(sql))

def generate_dim_date(
    start_date="2018-01-01",
    end_date="2026-12-31",
    fiscal_year_start_month=1,
    holidays=None
):
    """Generate enriched date dimension."""
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    df = pd.DataFrame({"full_date": dates})
    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["day"] = df["full_date"].dt.day
    df["day_of_week"] = df["full_date"].dt.dayofweek
    df["day_name"] = df["full_date"].dt.day_name()
    df["week_of_year"] = df["full_date"].dt.isocalendar().week.astype(int)
    df["month"] = df["full_date"].dt.month
    df["month_name"] = df["full_date"].dt.month_name()
    df["quarter"] = df["full_date"].dt.quarter
    df["year"] = df["full_date"].dt.year
    df["year_month"] = df["full_date"].dt.strftime("%Y%m").astype(int)
    df["fiscal_year"] = df["year"].where(df["month"]>=fiscal_year_start_month, df["year"]-1)
    df["fiscal_month"] = ((df["month"] - fiscal_year_start_month) % 12) + 1
    df["fiscal_quarter"] = ((df["fiscal_month"]-1)//3)+1
    df["fiscal_year_month"] = df["fiscal_year"]*100 + df["fiscal_month"]
    df["is_weekend"] = df["day_of_week"].isin([5,6])
    holidays = pd.to_datetime(holidays or [])
    df["is_holiday"] = df["full_date"].isin(holidays)
    df["is_first_day_of_month"] = df["day"]==1
    df["is_last_day_of_month"] = df["full_date"] == df["full_date"] + pd.offsets.MonthEnd(0)
    return df

# ============================
# Main DW Load Pipeline
# ============================
def main():
    now = datetime.datetime.utcnow()
    print(f"🚀 Starting DW load at {now} UTC")

    # --- Load MDM Dimensions ---
    cust_bk = ["Customer ID","Customer Name","Segment"]
    prod_bk = ["Product ID","Product Name","Category","Sub-Category"]
    geo_bk  = ["Country","City","State","Postal Code","Region"]

    df_mdm_cust = read_table(MDM_SCHEMA,"customers")[cust_bk]
    df_mdm_prod = read_table(MDM_SCHEMA,"products")[prod_bk]
    df_mdm_geo  = read_table(MDM_SCHEMA,"countries")[geo_bk]

    upsert_dim(df_mdm_cust,"dim_customers",cust_bk)
    upsert_dim(df_mdm_prod,"dim_products",prod_bk)
    upsert_dim(df_mdm_geo,"dim_countries",geo_bk)

    # --- Load Date Dimension ---
    df_date = generate_dim_date()
    df_date["full_date"] = df_date["full_date"].dt.strftime("%d-%m-%Y")
    upsert_dim(df_date,"dim_date",["date_key"])

    # --- Load PRD Orders ---
    df_prd = read_table(PRD_SCHEMA,"prd_orders")

    # --- Join DW Dimensions ---
    df_fact = df_prd.merge(read_table(DW_SCHEMA,"dim_customers")[cust_bk+["surrogate_key"]],
                            on=cust_bk, how="left").rename(columns={"surrogate_key":"customer_sk"})

    df_fact = df_fact.merge(read_table(DW_SCHEMA,"dim_products")[prod_bk+["surrogate_key"]],
                            on=prod_bk, how="left").rename(columns={"surrogate_key":"product_sk"})

    df_fact = df_fact.merge(read_table(DW_SCHEMA,"dim_countries")[geo_bk+["surrogate_key"]],
                            on=geo_bk, how="left").rename(columns={"surrogate_key":"country_sk"})

    df_fact = df_fact.merge(read_table(DW_SCHEMA,"dim_date")[["date_key","full_date"]],
                            left_on="Order Date", right_on="full_date", how="left").rename(columns={"date_key":"order_date_sk"})
    df_fact.drop(columns=["full_date"], inplace=True)

    df_fact = df_fact.merge(read_table(DW_SCHEMA,"dim_date")[["date_key","full_date"]],
                            left_on="Ship Date", right_on="full_date", how="left").rename(columns={"date_key":"ship_date_sk"})
    df_fact.drop(columns=["full_date"], inplace=True)

    # --- Fact Columns ---
    fact_cols = ["Order ID","order_date_sk","ship_date_sk","customer_sk","product_sk","country_sk",
                 "Sales","Quantity","Discount","Profit","Ship Mode"]
    df_final_fact = df_fact[fact_cols].copy()

    # --- Upsert Fact Table ---
    upsert_fact(df_final_fact,"fact_orders_sales")

    print(f"✅ DW load completed with {len(df_final_fact)} fact rows at {datetime.datetime.utcnow()} UTC")

if __name__=="__main__":
    main()
