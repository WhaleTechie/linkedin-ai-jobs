from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import requests
import snowflake.connector

# --- Config ---
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASS = os.getenv("SNOWFLAKE_PASS")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

SEARCH_QUERY = 'site:linkedin.com/jobs "AI engineer" OR "data engineer"'

# --- Functions ---
def fetch_jobs_from_serpapi():
    params = {
        "engine": "bing",
        "q": SEARCH_QUERY,
        "api_key": SERPAPI_KEY
    }
    res = requests.get("https://serpapi.com/search", params=params)
    res.raise_for_status()
    return res.json().get("organic_results", [])

def load_jobs_to_snowflake(**context):
    records = context['ti'].xcom_pull(task_ids="fetch_jobs")
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASS,
        account=SNOWFLAKE_ACCOUNT,
        warehouse="COMPUTE_WH",
        database="LINKEDIN_DB",
        schema="RAW"
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS JOBS (
            query STRING,
            title STRING,
            link STRING,
            snippet STRING,
            date_crawled TIMESTAMP
        )
    """)
    for r in records:
        cur.execute("""
            INSERT INTO JOBS(query, title, link, snippet, date_crawled)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            SEARCH_QUERY,
            r.get("title"),
            r.get("link"),
            r.get("snippet"),
            datetime.utcnow()
        ))
    conn.commit()
    conn.close()

# --- DAG Definition ---
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "linkedin_jobs_pipeline",
    default_args=default_args,
    description="Fetch LinkedIn job postings via SerpAPI, store in Snowflake, transform with dbt",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["linkedin", "jobs", "serpapi", "snowflake", "dbt"]
) as dag:

    fetch_jobs = PythonOperator(
        task_id="fetch_jobs",
        python_callable=fetch_jobs_from_serpapi
    )

    load_jobs = PythonOperator(
        task_id="load_jobs",
        python_callable=load_jobs_to_snowflake,
        provide_context=True
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/airflow/dbt_project && dbt run"
    )

    fetch_jobs >> load_jobs >> run_dbt
