# Retail Orders ETL Pipeline

This project implements a **full Retail Orders ETL pipeline** from raw order ingestion via SFTP to a PostgreSQL Data Warehouse. The pipeline covers **staging, PRD, data quality (DQ), master data management (MDM), and dimensional modeling (DW)**, all orchestrated by **Apache Airflow**.

---

## Table of Contents

* [Overview](#overview)
* [Prerequisites](#prerequisites)
* [Folder Structure](#folder-structure)
* [Pipeline Steps](#pipeline-steps)
* [How to Run](#how-to-run)
* [Configuration](#configuration)
* [Functionality Details](#functionality-details)
* [Logging & Audit](#logging--audit)

---

## Overview

The ETL pipeline executes the following end-to-end process:

1. **Ingest** order data from an SFTP server into a local filesystem (landing zone).
2. **Stage** files in a structured file system and load into PostgreSQL STG/PRD schemas.
3. Apply **Data Quality (DQ) checks** to identify missing, invalid, or inconsistent records.
4. Perform **Master Data Management (MDM)** to consolidate entities like customers, products, and countries.
5. Load **dimensions** (Customer, Product, Country, Date) and **fact tables** into PostgreSQL DW schema.
6. Generate **audit logs** at each step to ensure traceability.

The pipeline uses YAML configuration files to define **schemas, DQ rules, and file paths**.

---

## Prerequisites

* Python >= 3.10
* PostgreSQL instance with schemas: `stg`, `prd`, `mdm`, `dw`
* Apache Airflow >= 2.7
* Access to SFTP server with order files

Python dependencies (install via `requirements.txt`):

```bash
pip install -r requirements.txt
```

---

## Folder Structure

```
retail_orders_etl/
│
├── dags/                  # Airflow DAGs
├── scripts/               # ETL Python modules
├── config/orders/         # Schema, DQ rules, paths
├── tests/                 # Unit / integration tests
├── README.md
├── requirements.txt
└── .gitignore
```

---

## Pipeline Steps

### 1️⃣ SFTP Ingestion

* `import_sftp.py` downloads order files from the remote SFTP server into the local landing directory.
* Ensures only new files are fetched to avoid duplicates.

### 2️⃣ Load to File System (FS)

* `load_to_fs.py` organizes raw files into a structured directory (landing → staged).
* Applies consistent naming conventions and folder partitioning by date.

### 3️⃣ Load to PostgreSQL STG/PRD

* `load_to_stg_prd.py` reads staged CSVs and inserts them into PostgreSQL staging and production schemas.
* Supports incremental loading by comparing last ingestion timestamps.

### 4️⃣ Data Quality Checks

* `dq_checks.py` executes configurable checks (missing values, invalid dates, negative or zero numeric values, type validations).
* Generates **CSV violations** and **Excel reports** for review.

### 5️⃣ Master Data Management (MDM)

* `mdm.py` consolidates entities (Customers, Products, Countries).
* Supports incremental updates and generates **surrogate keys**.
* Keeps **audit logs** for every insert/update/delete operation.

### 6️⃣ Load to DW

* `load_dw.py` implements **SCD Type 2** logic for dimension tables.
* Creates a **fact table** with surrogate keys from dimensions.
* Generates a **date dimension** enriched with fiscal year/month, holidays, and flags.

### 7️⃣ Airflow Orchestration

* `etl_retail_orders.py` orchestrates all steps as tasks with dependencies.
* Uses **PythonOperator** for each ETL module.
* Sends notifications and logs execution results.

---

## How to Run

1. **Configure paths and connections** in `config/orders/paths.yaml`

   * Include SFTP credentials, local landing/staging paths, and PostgreSQL connection info.
2. **Start Airflow**:

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow webserver --port 8080
airflow scheduler
```

3. **Deploy DAG**
   Copy `dags/etl_retail_orders.py` into the Airflow DAGs folder.

4. **Trigger DAG**

   * Manually via Airflow UI or CLI:

   ```bash
   airflow dags trigger etl_retail_orders
   ```

5. **Monitor Logs**

   * Airflow UI provides task-level logs for troubleshooting.

---

## Configuration

| File            | Purpose                                                                 |
| --------------- | ----------------------------------------------------------------------- |
| `schema.yml`    | Defines table schemas, primary keys, and important columns.             |
| `dq_checks.yml` | Lists DQ rules for nulls, invalid formats, negative values, etc.        |
| `paths.yaml`    | SFTP credentials, landing/staging paths, PostgreSQL connection details. |

---

## Functionality Details

* **Incremental Loading**: Only new or changed records are processed, improving efficiency.
* **SCD Type 2**: Historical changes in dimensions are tracked with `effective_from`, `effective_to`, and `current_flag`.
* **MDM Consolidation**: Golden records are determined using scoring on important fields.
* **Data Quality**: DQ checks are fully configurable via YAML. Violations are outputted as CSV and Excel reports.
* **Audit & Logging**: Every stage writes audit logs with timestamps, row counts, and operation type.

---

## Logging & Audit

* **Audit Tables** in PostgreSQL (`audit` schema) track:

  * Dimension/fact loads
  * DQ violations
  * MDM insert/update/delete actions
* **Filesystem logs** capture detailed DQ and ingestion results in `audit/` folder.

---

## Notes

* Designed to be **modular**, each Python script can be run independently or orchestrated via Airflow.
* Configuration-driven approach allows **easy adaptation** to new entities or sources.
* Supports **full load** or **incremental load** without modifying code.

