from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

# GCP Configurations
PROJECT_ID = "nimble-courier-449405"
REGION = "us-central1"
CLUSTER_NAME = "my-cluster"
GCS_PYSPARK_FILE = "gs://nimble-courier-449405-f7/main.py"
BIGQUERY_JAR = "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"

# Define gcloud command for submitting the job
GCLOUD_COMMAND = f"""
gcloud dataproc jobs submit pyspark {GCS_PYSPARK_FILE} \
    --cluster={CLUSTER_NAME} \
    --region={REGION} \
    --properties spark.executor.memory=4g,spark.executor.cores=2 \
    --jars {BIGQUERY_JAR}
"""

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id="dataproc_pyspark_gcloud_2min",
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # Runs every 2 minutes
    catchup=False,
    tags=["dataproc", "pyspark", "gcloud"],
) as dag:

    start = EmptyOperator(task_id="start")

    submit_pyspark = BashOperator(
        task_id="submit_pyspark_job",
        bash_command=GCLOUD_COMMAND,
    )

    end = EmptyOperator(task_id="end")

    # DAG Workflow
    start >> submit_pyspark >> end

