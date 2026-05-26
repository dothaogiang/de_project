from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Hàm Python sẽ được task gọi
def say_hello():
    print("Hello từ Airflow!")
    print(f"Thời gian chạy: {datetime.now()}")

def count_data():
    # Đọc file CSV và đếm dòng
    with open("/opt/airflow/data/movie_review.csv", "r") as f:
        lines = f.readlines()
    print(f"File movie_review.csv có {len(lines) - 1} dòng dữ liệu")
    return len(lines) - 1

with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # chỉ chạy thủ công
    catchup=False,
) as dag:

    # Task 1: in hello
    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    # Task 2: đếm dòng dữ liệu
    task_count = PythonOperator(
        task_id="count_data",
        python_callable=count_data,
    )

    # Task 3: in thư mục hiện tại
    task_ls = BashOperator(
        task_id="list_data_folder",
        bash_command="ls -lh /opt/airflow/data/",
    )

    # Thứ tự chạy
    task_hello >> task_count >> task_ls