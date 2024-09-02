from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from etlProcess import extract_data_from_hive, Transform_data, Load_data_to_csv

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id="Loan_Request_Report",
    start_date=datetime(year=2024, month=7, day=1, hour=9, minute=0),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
)

#---------------------etl tasks------------------
def func():
    print("Emailed sent")

today = date.today()
required_date = (today - timedelta(days=30)).isoformat()

def extract_task(**kwargs):
    merged_df, original_df = extract_data_from_hive(kwargs['required_date'])
    kwargs['ti'].xcom_push(key='merged_df', value=merged_df)
    kwargs['ti'].xcom_push(key='original_df', value=original_df)

def transform_task(**kwargs):
    ti = kwargs['ti']
    merged_df = ti.xcom_pull(key='merged_df', task_ids='Extract')
    transformed_df = Transform_data(merged_df)
    ti.xcom_push(key='transformed_df', value=transformed_df)

def load_task(**kwargs):
    ti = kwargs['ti']
    transformed_df = ti.xcom_pull(key='transformed_df', task_ids='Transform')
    original_df = ti.xcom_pull(key='original_df', task_ids='Extract')
    Load_data_to_csv(transformed_df, kwargs['required_date'], original_df)

extract = PythonOperator(
    task_id='Extract',
    python_callable=extract_task,
    op_kwargs={'required_date': required_date},
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='Transform',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

load = PythonOperator(
    task_id='Load',
    python_callable=load_task,
    op_kwargs={'required_date': required_date},
    provide_context=True,
    dag=dag,
)

sendEmail = PythonOperator(
    task_id='SendEmail',
    python_callable=func,  
    dag=dag,
)

extract >> transform >> load >> sendEmail
