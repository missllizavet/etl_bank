# task_2_2_solution.py
import psycopg2
import pandas as pd
import os
import time
import io
import chardet
from datetime import datetime
from configurations import DB_CONFIG, DATA_DIR, RD_TABLES_CONFIG


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
            except Exception as e:
                print(f"Ошибка при парсинге дат в колонке {col}: {e}")

    df.columns = [col.lower() for col in df.columns]

    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].astype(str).str.replace(' ', '').str.replace(',', '.')
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass

    if 'effective_to_date' in df.columns:
        df['effective_to_date'] = df['effective_to_date'].fillna('2099-12-31')
        print("Null значения в effective_to_date заменены на 2099-12-31")

    return df


def update_table(conn, table_name, df, primary_keys):
    with conn.cursor() as cur:
        if primary_keys:
            original_count = len(df)
            df = df.drop_duplicates(subset=primary_keys, keep='last')
            print(f"Удалено дубликатов: {original_count - len(df)}")
            print(f"После удаления дубликатов: {len(df)} строк")

        cur.execute(f"TRUNCATE TABLE {table_name}")
        print(f"Таблица {table_name} очищена")

        columns = list(df.columns)

        for _, row in df.iterrows():
            placeholders = ','.join(['%s'] * len(columns))
            values = []
            for col in columns:
                val = row[col]
                if pd.isna(val):
                    values.append(None)
                elif hasattr(val, 'strftime'):
                    values.append(val.strftime('%Y-%m-%d'))
                else:
                    values.append(val)

            insert_sql = f'INSERT INTO {table_name} ({",".join(columns)}) VALUES ({placeholders})'

            try:
                cur.execute(insert_sql, values)
            except Exception as e:
                print(f"Ошибка при вставке строки: {e}")
                print(f"Значения: {values}")
                raise

        conn.commit()
        return len(df)


def process_table(conn, table_name, file_name, primary_keys):
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f"[{datetime.now()}] Файл не найден: {file_path}")
        return

    log_id = log_event(conn, table_name, 'START')
    print(f"[{datetime.now()}] Запуск ETL для {table_name}...")

    time.sleep(2)

    try:
        df = read_csv_with_date_parsing(file_path)
        print(f"[{datetime.now()}] Загружено {len(df)} строк из {file_name}")

        rows_affected = update_table(conn, table_name, df, primary_keys)
        print(f"[{datetime.now()}] Обработано {rows_affected} строк для {table_name}")

        log_event(conn, table_name, 'COMPLETED', rows_processed=rows_affected, log_id=log_id)

    except Exception as e:
        print(f"[{datetime.now()}] Ошибка при обработке {table_name}: {str(e)}")
        log_event(conn, table_name, 'FAILED', error_message=str(e), log_id=log_id)
        raise


def analyze_data_gaps(conn):
    print("\n" + "=" * 60)
    print("АНАЛИЗ ВИТРИНЫ loan_holiday_info")
    print("=" * 60)

    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT MIN(effective_from_date), MAX(effective_from_date), 
                   COUNT(DISTINCT effective_from_date) as unique_dates
            FROM dm.loan_holiday_info
        """)
        result = cursor.fetchone()

        if result[0]:
            print(f"Диапазон дат в витрине: {result[0]} - {result[1]}")
            print(f"Количество уникальных дат: {result[2]}")

            cursor.execute("""
                WITH date_range AS (
                    SELECT generate_series('2023-01-01'::date, '2023-12-31'::date, '1 day'::interval)::date as date
                ) 
                SELECT COUNT(*) as missing_dates
                FROM date_range 
                WHERE date NOT IN (SELECT DISTINCT effective_from_date FROM dm.loan_holiday_info)
            """)
            missing_count = cursor.fetchone()[0]
            print(f"Количество пропущенных дат: {missing_count}")
        else:
            print("Витрина пуста!")

        cursor.execute("""
            SELECT 'deal_info' as table_name, 
                   MIN(effective_from_date) as min_date, 
                   MAX(effective_from_date) as max_date,
                   COUNT(DISTINCT effective_from_date) as unique_dates
            FROM rd.deal_info
            UNION ALL
            SELECT 'loan_holiday' as table_name, 
                   MIN(effective_from_date) as min_date, 
                   MAX(effective_from_date) as max_date,
                   COUNT(DISTINCT effective_from_date) as unique_dates
            FROM rd.loan_holiday
            UNION ALL
            SELECT 'product' as table_name, 
                   MIN(effective_from_date) as min_date, 
                   MAX(effective_from_date) as max_date,
                   COUNT(DISTINCT effective_from_date) as unique_dates
            FROM rd.product
        """)

        print("\nДанные в таблицах-источниках:")
        for row in cursor.fetchall():
            if row[0] == 'loan_holiday' and row[1] is None:
                print(f"  {row[0]}: НЕТ ДАННЫХ (таблица пуста)")
            else:
                print(f"  {row[0]}: {row[1]} - {row[2]} ({row[3]} уникальных дат)")

    finally:
        cursor.close()


def refresh_mart(conn):
    print("\n" + "=" * 60)
    print("ПЕРЕСЧЕТ ВИТРИНЫ")
    print("=" * 60)

    with conn.cursor() as cur:
        cur.execute("CALL refresh_loan_holiday_info()")
        conn.commit()
        print("Витрина loan_holiday_info успешно пересчитана")

        cur.execute("SELECT COUNT(*) FROM dm.loan_holiday_info")
        count = cur.fetchone()[0]
        print(f"Количество записей в витрине после пересчета: {count}")


def determine_load_strategy(conn):
    print("\n" + "=" * 60)
    print("ОПРЕДЕЛЕНИЕ СТРАТЕГИИ ЗАГРУЗКИ")
    print("=" * 60)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM rd.deal_info")
        deal_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM rd.product")
        product_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM rd.loan_holiday")
        loan_holiday_count = cur.fetchone()[0]

    print(f"\n  rd.deal_info: {deal_count} записей")
    print(f"  rd.product: {product_count} записей")
    print(f"  rd.loan_holiday: {loan_holiday_count} записей")



def main():
    print("\n" + "=" * 60)
    print("ЗАДАНИЕ 2.2: Восстановление данных витрины dm.loan_holiday_info")
    print("=" * 60)

    conn = get_db_connection()

    try:
        print("\n[1/4] Анализ текущего состояния...")
        analyze_data_gaps(conn)

        print("\n[2/4] Загрузка данных из CSV файлов...")
        for config in RD_TABLES_CONFIG:
            process_table(
                conn,
                config['table_name'],
                config['file_name'],
                config['primary_keys']
            )

        print("\n[3/4] Определение стратегии загрузки...")
        determine_load_strategy(conn)

        print("\n[4/4] Пересчет витрины...")
        refresh_mart(conn)

        print("\n" + "=" * 60)
        print("ФИНАЛЬНЫЙ АНАЛИЗ")
        print("=" * 60)
        analyze_data_gaps(conn)

        print("\n" + "=" * 60)
        print("ЗАДАНИЕ 2.2 ВЫПОЛНЕНО УСПЕШНО")
        print("=" * 60)

    except Exception as e:
        print(f"\nОШИБКА: {str(e)}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()