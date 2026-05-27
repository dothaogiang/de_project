from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.operators.bash import BashOperator

def upload_to_gcs(local_path, destination_blob_name):
    """Upload file lên GCS bucket"""
    bucket_name = Variable.get("gcs_bucket")
    
    # Đọc credentials từ file key
    client = storage.Client.from_service_account_json(
        "/opt/airflow/gcp_key.json"
    )
    
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_path)
    
    print(f"File {local_path} đã upload lên gs://{bucket_name}/{destination_blob_name}")

def upload_movie_review():
    upload_to_gcs(
        local_path="/opt/airflow/data/movie_review.csv",
        destination_blob_name="raw/movie_review.csv"
    )

def upload_user_purchase():
    upload_to_gcs(
        local_path="/opt/airflow/data/OnlineRetail.csv",
        destination_blob_name="raw/user_purchase.csv"
    )

with DAG(
    dag_id="gcs_upload_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # chạy thủ công
    catchup=False,
) as dag:

    task_upload_movie = PythonOperator(
        task_id="upload_movie_review",
        python_callable=upload_movie_review,
    )

    task_upload_purchase = PythonOperator(
        task_id="upload_user_purchase",
        python_callable=upload_user_purchase,
    )
    task_spark = BashOperator(
    task_id="run_spark_job",
    bash_command="""
        /opt/spark/bin/spark-submit \
        --master local[*] \
        --driver-class-path /opt/airflow/dags/gcs-connector-hadoop3-latest.jar \
        --jars /opt/airflow/dags/gcs-connector-hadoop3-latest.jar \
        --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
        --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/airflow/gcp_key.json \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
        /opt/airflow/dags/scripts/spark_job.py \
        de-project-giang-2026 \
        /opt/airflow/gcp_key.json
    """,
)
    task_duckdb = BashOperator(
        task_id="load_to_duckdb",
        bash_command="""
            python3 /opt/airflow/dags/scripts/load_duckdb.py \
            de-project-giang-2026 \
            /opt/airflow/gcp_key.json \
            /opt/airflow/temp/warehouse.duckdb
        """,
)

    # Cập nhật thứ tự
    [task_upload_movie, task_upload_purchase] >> task_spark >> task_duckdb
