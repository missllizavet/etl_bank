from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'bank_etl',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def run_export():
    import sys
    sys.path.append('/home/missllizavet/airflow/scripts')
    from export_f101 import export_f101_to_csv, show_sample_data
    output_file = export_f101_to_csv()
    show_sample_data(output_file)
    return output_file

def run_import():
    import sys
    sys.path.append('/home/missllizavet/airflow/scripts')
    from import_f101 import create_copy_table, modify_csv_values, import_csv_to_table
    import os
    DATA_DIR = '/home/missllizavet/airflow/data'
    csv_file = os.path.join(DATA_DIR, 'dm_f101_round_f_export.csv')
    create_copy_table()
    modified_file = modify_csv_values(csv_file)
    import_csv_to_table(modified_file, 'dm.dm_f101_round_f_v2')

dag = DAG(
    'export_import_f101',
    default_args=default_args,
    description='Экспорт и импорт данных 101 формы',
    schedule=None,
    catchup=False,
    tags=['f101', 'export', 'import'],
)

start = EmptyOperator(task_id='start', dag=dag)

export_task = PythonOperator(
    task_id='export_f101_to_csv',
    python_callable=run_export,
    dag=dag,
)

import_task = PythonOperator(
    task_id='import_f101_from_csv',
    python_callable=run_import,
    dag=dag,
)

end = EmptyOperator(task_id='end', dag=dag)

start >> export_task >> import_task >> end
