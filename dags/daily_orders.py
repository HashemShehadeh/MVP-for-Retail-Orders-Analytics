from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import os
import yaml

# ============================
# DAG Config
# ============================
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-team@example.com"],
}

dag = DAG(
    "retail_orders_etl_prod",
    default_args=default_args,
    description="Full ETL: SFTP -> FS -> STG/PRD -> DQ -> MDM -> DW",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
)

# ============================
# Load Config
# ============================
CONFIG_DIR = "/opt/airflow/config/orders"
with open(os.path.join(CONFIG_DIR, "paths.yaml")) as f:
    PATHS = yaml.safe_load(f)

with open(os.path.join(CONFIG_DIR, "schema.yml")) as f:
    SCHEMA = yaml.safe_load(f)

with open(os.path.join(CONFIG_DIR, "dq_checks.yml")) as f:
    DQ_CHECKS = yaml.safe_load(f)

# ============================
# Python Callables
# ============================
def run_import_sftp(**kwargs):
    from import_sftp import import_from_sftp
    import_from_sftp(PATHS["sftp"], PATHS["fs"]["landing"])

def run_load_to_fs(**kwargs):
    from load_to_fs import load_fs
    load_fs(PATHS["fs"]["landing"], PATHS["fs"]["staged"], SCHEMA)

def run_load_to_stg_prd(**kwargs):
    from load_to_stg_prd import load_stg_prd
    load_stg_prd(PATHS["fs"]["staged"], PATHS["postgres"]["stg"], PATHS["postgres"]["prd"])

def run_dq_checks(**kwargs):
    from dq_checks import run_dq
    run_dq(PATHS["postgres"]["stg"], DQ_CHECKS)

def run_mdm_entity(entity_name):
    def _run(**kwargs):
        from mdm import process_mdm
        process_mdm(PATHS["postgres"]["prd"], PATHS["postgres"]["mdm"], entity_name)
    return _run

def run_load_dw(**kwargs):
    from load_dw import load_dw_main
    load_dw_main(PATHS["postgres"]["mdm"], PATHS["postgres"]["dw"])

# ============================
# Failure Notification
# ============================
def notify_email(context):
    subject = f"DAG {context['dag'].dag_id} Failed"
    html_content = f"""
        DAG: {context['dag'].dag_id}<br>
        Task: {context['task_instance'].task_id}<br>
        Execution Time: {context['execution_date']}<br>
        Log URL: {context['task_instance'].log_url}<br>
    """
    send_email(to=default_args["email"], subject=subject, html_content=html_content)

# ============================
# Tasks
# ============================
t1_sftp = PythonOperator(
    task_id="import_sftp",
    python_callable=run_import_sftp,
    on_failure_callback=notify_email,
    dag=dag
)

t2_fs = PythonOperator(
    task_id="load_to_fs",
    python_callable=run_load_to_fs,
    on_failure_callback=notify_email,
    dag=dag
)

t3_stg_prd = PythonOperator(
    task_id="load_to_stg_prd",
    python_callable=run_load_to_stg_prd,
    on_failure_callback=notify_email,
    dag=dag
)

t4_dq = PythonOperator(
    task_id="dq_checks",
    python_callable=run_dq_checks,
    on_failure_callback=notify_email,
    dag=dag
)

# MDM entities parallelized
with TaskGroup("mdm_processing", tooltip="Process each MDM entity") as mdm_group:
    mdm_customers = PythonOperator(
        task_id="mdm_customers",
        python_callable=run_mdm_entity("customers"),
        on_failure_callback=notify_email
    )

    mdm_products = PythonOperator(
        task_id="mdm_products",
        python_callable=run_mdm_entity("products"),
        on_failure_callback=notify_email
    )

    mdm_countries = PythonOperator(
        task_id="mdm_countries",
        python_callable=run_mdm_entity("countries"),
        on_failure_callback=notify_email
    )

t5_dw = PythonOperator(
    task_id="load_dw",
    python_callable=run_load_dw,
    on_failure_callback=notify_email,
    dag=dag
)

# ============================
# Dependencies
# ============================
t1_sftp >> t2_fs >> t3_stg_prd >> t4_dq >> mdm_group >> t5_dw
