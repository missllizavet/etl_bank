from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import time
import io
import chardet

DATA_DIR = '/home/missllizavet/airflow/data'

default_args = {
    'owner': 'bank_etl',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def log_event(conn, table_name, status, rows_processed=0, error_message=None, log_id=None):
    with conn.cursor() as cur:
        if status == 'START':
            cur.execute(
                "INSERT INTO logs.etl_log (table_name, start_time, status) VALUES (%s, %s, %s) RETURNING log_id",
                (table_name, datetime.now(), 'IN PROGRESS')
            )
            new_log_id = cur.fetchone()[0]
            conn.commit()
            return new_log_id
        else:
            cur.execute(
                "UPDATE logs.etl_log SET end_time = %s, status = %s, rows_processed = %s, error_message = %s WHERE log_id = %s",
                (datetime.now(), status, rows_processed, error_message, log_id)
            )
            conn.commit()
            return log_id

def detect_date_columns(df):
    date_keywords = ['date', 'data']
    date_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in date_keywords):
            date_columns.append(col)
    return date_columns

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)
        result = chardet.detect(raw_data)
        return result['encoding']

def read_csv_with_date_parsing(file_path):
    encoding = detect_encoding(file_path)
    print(f"Определена кодировка: {encoding}")

    with open(file_path, 'r', encoding=encoding) as f:
        first_line = f.readline()

    if ';' in first_line:
        separator = ';'
    else:
        separator = ','

    print(f"Определен разделитель: '{separator}'")

    df = pd.read_csv(file_path, sep=separator, encoding=encoding, nrows=0)
    print(f"Колонки в {os.path.basename(file_path)}: {list(df.columns)}")

    date_columns = detect_date_columns(df)
    print(f"Обнаружены колонки с датами: {date_columns}")

    df = pd.read_csv(file_path, sep=separator, encoding=encoding)

    def detect_date_format(date_string):
        if pd.isna(date_string):
            return True
        date_string = str(date_string).strip()
        if len(date_string) == 10 and date_string[4] == '-' and date_string[7] == '-':
            return False
        elif len(date_string) == 10 and date_string[2] == '.' and date_string[5] == '.':
            return True
        return True

    for col in date_columns:
        if col in df.columns:
            try:
                sample_date = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                use_dayfirst = detect_date_format(sample_date)
                print(f"Колонка {col}: используется dayfirst={use_dayfirst} (пример: {sample_date})")
                df[col] = pd.to_datetime(df[col], dayfirst=use_dayfirst, errors='coerce')
                null_count = df[col].isna().sum()
                if null_count > 0:
                    print(f"Предупреждение: {null_count} пустых значений в {col}")
            except Exception as e:
                print(f"Ошибка при парсинге дат в колонке {col}: {e}")
                try:
                    df[col] = pd.to_datetime(df[col], dayfirst=not use_dayfirst, errors='coerce')
                except:
                    print(f"Предупреждение: не удалось распарсить даты в колонке {col}")

    df.columns = [col.lower() for col in df.columns]

    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].astype(str).str.replace(' ', '').str.replace(',', '.')
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass

    return df, separator

def update_table(conn, table_name, df, primary_keys, separator=','):
    with conn.cursor() as cur:
        if primary_keys:
            df = df.drop_duplicates(subset=primary_keys, keep='last')
            print(f"После удаления дубликатов: {len(df)} строк")

        columns = list(df.columns)
        temp_table = f"temp_{table_name.split('.')[-1]}"

        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")

        col_defs = ', '.join([f'"{col}" TEXT' for col in columns])
        cur.execute(f"CREATE TEMP TABLE {temp_table} ({col_defs})")

        
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep='', sep=separator)
        buffer.seek(0)
        cur.copy_from(buffer, temp_table, columns=columns, sep=separator, null='')
        short_table_name = table_name.split('.')[-1]

        type_casts = {
            'ft_balance_f': {
                'on_date': 'DATE',
                'account_rk': 'BIGINT',
                'currency_rk': 'BIGINT',
                'balance_out': 'DOUBLE PRECISION'
            },
            'ft_posting_f': {
                'oper_date': 'DATE',
                'credit_account_rk': 'BIGINT',
                'debet_account_rk': 'BIGINT',
                'credit_amount': 'DOUBLE PRECISION',
                'debet_amount': 'DOUBLE PRECISION'
            },
            'md_account_d': {
                'data_actual_date': 'DATE',
                'data_actual_end_date': 'DATE',
                'account_rk': 'BIGINT',
                'account_number': 'VARCHAR(20)',
                'char_type': 'VARCHAR(1)',
                'currency_rk': 'BIGINT',
                'currency_code': 'VARCHAR(3)'
            },
            'md_currency_d': {
                'currency_rk': 'BIGINT',
                'data_actual_date': 'DATE',
                'data_actual_end_date': 'DATE',
                'currency_code': 'VARCHAR(3)',
                'code_iso_char': 'VARCHAR(3)'
            },
            'md_exchange_rate_d': {
                'data_actual_date': 'DATE',
                'data_actual_end_date': 'DATE',
                'currency_rk': 'BIGINT',
                'reduced_cource': 'DOUBLE PRECISION',
                'code_iso_num': 'VARCHAR(3)'
            },
            'md_ledger_account_s': {
                'chapter': 'CHAR(1)',
                'chapter_name': 'VARCHAR(16)',
                'section_number': 'INTEGER',
                'section_name': 'VARCHAR(22)',
                'subsection_name': 'VARCHAR(21)',
                'ledger1_account': 'INTEGER',
                'ledger1_account_name': 'VARCHAR(47)',
                'ledger_account': 'INTEGER',
                'ledger_account_name': 'VARCHAR(153)',
                'characteristic': 'CHAR(1)',
                'is_resident': 'INTEGER',
                'is_reserve': 'INTEGER',
                'is_reserved': 'INTEGER',
                'is_loan': 'INTEGER',
                'is_reserved_assets': 'INTEGER',
                'is_overdue': 'INTEGER',
                'is_interest': 'INTEGER',
                'pair_account': 'VARCHAR(5)',
                'start_date': 'DATE',
                'end_date': 'DATE',
                'is_rub_only': 'INTEGER',
                'min_term': 'VARCHAR(1)',
                'min_term_measure': 'VARCHAR(1)',
                'max_term': 'VARCHAR(1)',
                'max_term_measure': 'VARCHAR(1)',
                'ledger_acc_full_name_translit': 'VARCHAR(1)',
                'is_revaluation': 'VARCHAR(1)',
                'is_correct': 'VARCHAR(1)'
            }
        }

        casts = type_casts.get(short_table_name, {})

        insert_columns = []
        select_columns = []
        for col in df.columns:
            insert_columns.append(f'"{col}"')
            if col in casts:
                select_columns.append(f'"{col}"::{casts[col]}')
            else:
                select_columns.append(f'"{col}"')

        if primary_keys:
            pk_columns = ', '.join([f'"{pk}"' for pk in primary_keys])
            update_set = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns])
            upsert_sql = f"""
            INSERT INTO {table_name} ({', '.join(insert_columns)})
            SELECT {', '.join(select_columns)} FROM {temp_table}
            ON CONFLICT ({pk_columns}) DO UPDATE SET {update_set}
            """
        else:
            upsert_sql = f"""
            INSERT INTO {table_name} ({', '.join(insert_columns)})
            SELECT {', '.join(select_columns)} FROM {temp_table}
            """

        try:
            cur.execute(upsert_sql)
            conn.commit()
            return cur.rowcount
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")

def process_table(table_name, file_name, primary_keys, **context):
    hook = PostgresHook(postgres_conn_id='bank_postgres')
    conn = hook.get_conn()
    
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f"[{datetime.now()}] Файл не найден: {file_path}")
        return

    log_id = log_event(conn, table_name, 'START')
    print(f"[{datetime.now()}] Запуск ETL для {table_name}...")

    time.sleep(5)

    try:
        df, separator = read_csv_with_date_parsing(file_path)  
        print(f"[{datetime.now()}] Загружено {len(df)} строк из {file_name}")

        if table_name == 'ds.ft_posting_f':
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table_name}")
                conn.commit()
            print(f"[{datetime.now()}] Очищена таблица {table_name}")

        rows_affected = update_table(conn, table_name, df, primary_keys, separator) 
        print(f"[{datetime.now()}] Обработано {rows_affected} строк для {table_name}")

        log_event(conn, table_name, 'COMPLETED', rows_processed=rows_affected, log_id=log_id)

    except Exception as e:
        print(f"[{datetime.now()}] Ошибка при обработке {table_name}: {str(e)}")
        log_event(conn, table_name, 'FAILED', error_message=str(e), log_id=log_id)
        raise
    finally:
        conn.close()
dag = DAG(
    'bank_etl_full',
    default_args=default_args,
    description='Полный ETL для банковских данных из CSV',
    schedule=None,
    catchup=False,
    tags=['bank', 'etl', 'full'],
)

start = EmptyOperator(task_id='start', dag=dag)

create_schemas = PostgresOperator(
    task_id='create_schemas',
    postgres_conn_id='bank_postgres',
    sql="CREATE SCHEMA IF NOT EXISTS ds; CREATE SCHEMA IF NOT EXISTS logs;",
    dag=dag,
)

def create_tables_func():
    hook = PostgresHook(postgres_conn_id='bank_postgres')
    with open('/home/missllizavet/airflow/sql/create_tables.sql', 'r') as f:
        sql = f.read()
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    conn.close()
    print("Таблицы созданы")

create_tables = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables_func,
    dag=dag,
)

load_balance = PythonOperator(
    task_id='load_ft_balance_f',
    python_callable=process_table,
    op_args=['ds.ft_balance_f', 'ft_balance_f.csv', ['on_date', 'account_rk']],
    dag=dag,
)

load_posting = PythonOperator(
    task_id='load_ft_posting_f',
    python_callable=process_table,
    op_args=['ds.ft_posting_f', 'ft_posting_f.csv', []],
    dag=dag,
)

load_account = PythonOperator(
    task_id='load_md_account_d',
    python_callable=process_table,
    op_args=['ds.md_account_d', 'md_account_d.csv', ['data_actual_date', 'account_rk']],
    dag=dag,
)

load_currency = PythonOperator(
    task_id='load_md_currency_d',
    python_callable=process_table,
    op_args=['ds.md_currency_d', 'md_currency_d.csv', ['currency_rk', 'data_actual_date']],
    dag=dag,
)

load_exchange = PythonOperator(
    task_id='load_md_exchange_rate_d',
    python_callable=process_table,
    op_args=['ds.md_exchange_rate_d', 'md_exchange_rate_d.csv', ['data_actual_date', 'currency_rk']],
    dag=dag,
)

load_ledger = PythonOperator(
    task_id='load_md_ledger_account_s',
    python_callable=process_table,
    op_args=['ds.md_ledger_account_s', 'md_ledger_account_s.csv', ['ledger_account', 'start_date']],
    dag=dag,
)

end = EmptyOperator(task_id='end', dag=dag)

start >> create_schemas >> create_tables
create_tables >> [load_balance, load_posting, load_account, load_currency, load_exchange, load_ledger]
[load_balance, load_posting, load_account, load_currency, load_exchange, load_ledger] >> end
