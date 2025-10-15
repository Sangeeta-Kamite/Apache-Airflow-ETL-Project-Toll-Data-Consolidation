from datetime import datetime, timedelta
from airflow import DAG

# Compatible import for BashOperator (Airflow 1.x or 2.x)
try:
    from airflow.operators.bash_operator import BashOperator
except Exception:
    from airflow.operators.bash import BashOperator

# ---- Default DAG arguments ----
default_args = {
    'owner': 'Sangeeta K',
    'start_date': datetime.today(),
    'email': ['srk@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ---- DAG definition ----
dag = DAG(
    dag_id='ETL_toll_data',
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)

# ---- Tasks ----

# 1️⃣ unzip_data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz '
                 '-C /home/project/airflow/dags/finalassignment/staging',
    dag=dag
)

# 2️⃣ extract_data_from_csv
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1,2,3,4 '
                 '/home/project/airflow/dags/finalassignment/staging/vehicle-data.csv '
                 '> /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag
)

# 3️⃣ extract_data_from_tsv
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -f5,6,7 "
                 "/home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv "
                 "| tr '\t' ',' "
                 "> /home/project/airflow/dags/finalassignment/staging/tsv_data.csv",
    dag=dag
)

# 4️⃣ extract_data_from_fixed_width
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="awk '{print substr($0, 59, 5) \",\" substr($0, 65, 5)}' "
                 "/home/project/airflow/dags/finalassignment/staging/payment-data.txt "
                 "> /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv",
    dag=dag
)

# 5️⃣ consolidate_data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d"," '
                 '/home/project/airflow/dags/finalassignment/staging/csv_data.csv '
                 '/home/project/airflow/dags/finalassignment/staging/tsv_data.csv '
                 '/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv '
                 '> /home/project/airflow/dags/finalassignment/staging/extracted_data.csv',
    dag=dag
)

# 6️⃣ transform_data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk -F"," \'{OFS=","; $4=toupper($4); print $0}\' '
                 '/home/project/airflow/dags/finalassignment/staging/extracted_data.csv '
                 '> /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    dag=dag
)

# ---- Task Pipeline ----
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
