import psycopg2
import csv
import os
from datetime import datetime
from task1.configurations import DB_CONFIG


# Устанавливает соединение с базой данных
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


# Создает копию таблицы
def create_table_copy(conn, source_table, target_table):
    try:
        with conn.cursor() as cur:
            # Создаем новую таблицу как копию существующей
            cur.execute(f"""
                DROP TABLE IF EXISTS {target_table};
                CREATE TABLE {target_table} AS 
                SELECT * FROM {source_table} WHERE 1=0;

                -- Добавляем первичный ключ
                ALTER TABLE {target_table} 
                ADD CONSTRAINT pk_{target_table.split('.')[-1]} 
                PRIMARY KEY (from_date, to_date, ledger_account, characteristic);
            """)
            conn.commit()
            print(f"[{datetime.now()}] Создана таблица {target_table} как копия {source_table}")

    except Exception as e:
        conn.rollback()
        print(f"[{datetime.now()}] Ошибка при создании копии таблицы: {e}")
        raise


# Импортирует данные из CSV-файла в таблицу
def import_csv_to_table(conn, table_name, input_file):
    try:
        with conn.cursor() as cur:
            # Читаем CSV-файл
            with open(input_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile, delimiter=';')

                # Читаем заголовки
                headers = next(reader)
                print(f"[{datetime.now()}] Заголовки CSV: {headers}")

                # Подготавливаем данные для вставки
                rows = []
                for row in reader:
                    # Преобразуем пустые строки в None
                    formatted_row = []
                    for i, value in enumerate(row):
                        if value == '' or value.strip() == '':
                            formatted_row.append(None)
                        else:
                            # Определяем тип данных по названию колонки
                            col_name = headers[i].lower()
                            if 'date' in col_name:
                                # Пробуем распарсить дату
                                try:
                                    formatted_row.append(datetime.strptime(value, '%Y-%m-%d').date())
                                except:
                                    formatted_row.append(value)
                            elif any(type_name in col_name for type_name in ['amount', 'rub', 'val', 'total']):
                                try:
                                    formatted_row.append(float(value))
                                except:
                                    formatted_row.append(None)
                            else:
                                formatted_row.append(value)
                    rows.append(formatted_row)

                # Очищаем таблицу перед вставкой
                cur.execute(f"DELETE FROM {table_name}")

                # Вставляем данные
                placeholders = ', '.join(['%s'] * len(headers))
                columns = ', '.join([f'"{h}"' for h in headers])
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

                for row in rows:
                    try:
                        cur.execute(insert_sql, row)
                    except Exception as e:
                        print(f"[{datetime.now()}] Ошибка при вставке строки: {row[:5]}... - {e}")
                        raise

                conn.commit()
                print(f"[{datetime.now()}] Успешно импортировано {len(rows)} строк в {table_name}")

                # Логируем импорт
                cur.execute("""
                    INSERT INTO logs.etl_log (table_name, start_time, status, rows_processed)
                    VALUES (%s, %s, 'COMPLETED', %s)
                """, (f'import_{table_name}', datetime.now(), len(rows)))
                conn.commit()

                return len(rows)

    except Exception as e:
        conn.rollback()
        print(f"[{datetime.now()}] Ошибка при импорте данных: {e}")
        # Логируем ошибку
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs.etl_log (table_name, start_time, status, error_message)
                VALUES (%s, %s, 'FAILED', %s)
            """, (f'import_{table_name}', datetime.now(), str(e)))
            conn.commit()
        raise


# Сравнивает количество записей в двух таблицах
def compare_tables(conn, table1, table2):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table1}")
        count1 = cur.fetchone()[0]

        cur.execute(f"SELECT COUNT(*) FROM {table2}")
        count2 = cur.fetchone()[0]

        print(f"[{datetime.now()}] Сравнение:")
        print(f"  {table1}: {count1} строк")
        print(f"  {table2}: {count2} строк")

        if count1 == count2:
            print(f"   Количество строк совпадает!")
        else:
            print(f"   Количество строк отличается на {abs(count1 - count2)}")


def main():
    import_dir = '../export'
    input_file = os.path.join(import_dir, 'dm_f101_round_f.csv')

    if not os.path.exists(input_file):
        print(f"Ошибка: Файл {input_file} не найден!")
        print("Сначала запустите export_f101_to_csv.py")
        return

    source_table = 'dm.dm_f101_round_f'
    target_table = 'dm.dm_f101_round_f_v2'

    conn = get_db_connection()

    try:
        # Создаем копию таблицы
        print(f"[{datetime.now()}] Создание таблицы-копии {target_table}...")
        create_table_copy(conn, source_table, target_table)

        # Импортируем данные из CSV
        print(f"[{datetime.now()}] Импорт данных из {input_file}...")
        rows_count = import_csv_to_table(conn, target_table, input_file)

        print(f"[{datetime.now()}] Импорт завершен! {rows_count} строк импортировано в {target_table}")

        # Сравниваем таблицы
        compare_tables(conn, source_table, target_table)

        # Показываем пример данных из новой таблицы
        print(f"\n[{datetime.now()}] Пример данных из {target_table}:")
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {target_table} LIMIT 3")
            columns = [desc[0] for desc in cur.description]
            print(f"  Колонки: {columns}")
            for row in cur.fetchall():
                print(f"  {row}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()