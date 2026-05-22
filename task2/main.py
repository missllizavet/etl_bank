import psycopg2
import pandas as pd
import os
import time
import io
import chardet
from datetime import datetime
from configurations import DB_CONFIG, DATA_DIR


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


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
    print(f"кодировка: {encoding}")

    with open(file_path, 'r', encoding=encoding) as f:
        first_line = f.readline()

    if ';' in first_line:
        separator = ';'
    else:
        separator = ','

    print(f"разделитель: '{separator}'")

    df = pd.read_csv(file_path, sep=separator, encoding=encoding, nrows=0)
    print(f"колонки в {os.path.basename(file_path)}: {list(df.columns)}")

    date_columns = detect_date_columns(df)
    print(f"колонки с датами: {date_columns}")

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
                print(f"колонка {col}: dayfirst={use_dayfirst}")
                df[col] = pd.to_datetime(df[col], dayfirst=use_dayfirst, errors='coerce')
            except Exception as e:
                print(f"ошибка парсинга дат в {col}: {e}")

    df.columns = [col.lower() for col in df.columns]

    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].astype(str).str.replace(' ', '').str.replace(',', '.')
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass

    return df


def update_table(conn, table_name, df, primary_keys):
    with conn.cursor() as cur:
        if primary_keys:
            df = df.drop_duplicates(subset=primary_keys, keep='last')
            print(f"после удаления дубликатов: {len(df)} строк")

        columns = list(df.columns)
        temp_table = f"temp_{table_name.split('.')[-1]}"

        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        col_defs = ', '.join([f'"{col}" TEXT' for col in columns])
        cur.execute(f"CREATE TEMP TABLE {temp_table} ({col_defs})")

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep='')
        buffer.seek(0)
        cur.copy_from(buffer, temp_table, columns=columns, sep=',', null='')

        short_table_name = table_name.split('.')[-1]

        type_casts = {
            'deal_info': {
                'deal_rk': 'BIGINT',
                'deal_num': 'TEXT',
                'deal_name': 'TEXT',
                'deal_sum': 'NUMERIC',
                'client_rk': 'BIGINT',
                'account_rk': 'BIGINT',
                'agreement_rk': 'BIGINT',
                'deal_start_date': 'DATE',
                'department_rk': 'BIGINT',
                'product_rk': 'BIGINT',
                'deal_type_cd': 'TEXT',
                'effective_from_date': 'DATE',
                'effective_to_date': 'DATE'
            },
            'product': {
                'product_rk': 'BIGINT',
                'product_name': 'TEXT',
                'effective_from_date': 'DATE',
                'effective_to_date': 'DATE'
            },
            'dict_currency': {
                'currency_cd': 'TEXT',
                'currency_name': 'TEXT',
                'effective_from_date': 'DATE',
                'effective_to_date': 'DATE'
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


def process_table(conn, table_name, file_name, primary_keys):
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f"[{datetime.now()}] файл не найден: {file_path}")
        return

    log_id = log_event(conn, table_name, 'START')
    print(f"[{datetime.now()}] загрузка {table_name}...")

    time.sleep(5)

    try:
        df = read_csv_with_date_parsing(file_path)
        print(f"[{datetime.now()}] загружено {len(df)} строк из {file_name}")

        rows_affected = update_table(conn, table_name, df, primary_keys)
        print(f"[{datetime.now()}] обработано {rows_affected} строк для {table_name}")

        log_event(conn, table_name, 'COMPLETED', rows_processed=rows_affected, log_id=log_id)

    except Exception as e:
        print(f"[{datetime.now()}] ошибка {table_name}: {str(e)}")
        log_event(conn, table_name, 'FAILED', error_message=str(e), log_id=log_id)
        raise


def main():
    tables_config_rd = [
        {
            'table_name': 'rd.deal_info',
            'file_name': 'deal_info.csv',
            'primary_keys': ['deal_rk', 'effective_from_date']
        },
        {
            'table_name': 'rd.product',
            'file_name': 'product_info.csv',
            'primary_keys': ['product_rk', 'effective_from_date']
        },
        {
            'table_name': 'dm.dict_currency',
            'file_name': 'dict_currency.csv',
            'primary_keys': ['currency_cd', 'effective_from_date']
        }
    ]

    conn = get_db_connection()

    try:
        for config in tables_config_rd:
            process_table(
                conn,
                config['table_name'],
                config['file_name'],
                config['primary_keys']
            )
        print(f"[{datetime.now()}] загрузка RD завершена!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()