from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from etlProcess import extract_data_from_hive, Transform_data, Load_data_to_csv
from emailProcess import send_email
import pandas as pd

#  DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG
dag = DAG(
    dag_id="Loan_Request_Report",
    start_date=datetime(year=2024, month=7, day=1, hour=9, minute=0),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
)

#---------------------etl tasks------------------
def func():
    send_email()

today = date.today()
required_date = (today - timedelta(days=31)).isoformat()

EXTRACTED_MERGED_PATH = f"/tmp/merged_data_{required_date}.csv"
EXTRACTED_ORIGINAL_PATH = f"/tmp/original_data_{required_date}.csv"
TRANSFORMED_PATH = f"/tmp/transformed_data_{required_date}.csv"

def extract_task(**kwargs):
    merged_df, original_df = extract_data_from_hive(kwargs['required_date'])
    merged_df.to_csv(EXTRACTED_MERGED_PATH, index=False)
    original_df.to_csv(EXTRACTED_ORIGINAL_PATH, index=False)

def transform_task(**kwargs):
    merged_df = pd.read_csv(EXTRACTED_MERGED_PATH)
    transformed_df = Transform_data(merged_df)
    transformed_df.to_csv(TRANSFORMED_PATH, index=False)

def load_task(**kwargs):
    transformed_df = pd.read_csv(TRANSFORMED_PATH)
    original_df = pd.read_csv(EXTRACTED_ORIGINAL_PATH)
    Load_data_to_csv(transformed_df, kwargs['required_date'], original_df)


extract = PythonOperator(
    task_id='Extract',
    python_callable=extract_task,
    op_kwargs={'required_date': required_date},
    dag=dag,
)

transform = PythonOperator(
    task_id='Transform',
    python_callable=transform_task,
    dag=dag,
)

load = PythonOperator(
    task_id='Load',
    python_callable=load_task,
    op_kwargs={'required_date': required_date},
    dag=dag,
)

sendEmail = PythonOperator(
    task_id='SendEmail',
    python_callable=func,  
    dag=dag,
)
extract >> transform >> load >> sendEmail
