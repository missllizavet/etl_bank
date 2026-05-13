import psycopg2
import csv
import os
from datetime import datetime
from configurations import DB_CONFIG


# Устанавливает соединение с базой данных
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


# Экспортирует таблицу в CSV-файл, первая строка - заголовки колонок
def export_table_to_csv(conn, table_name, output_file):
    try:
        with conn.cursor() as cur:
            # Получаем все данные из таблицы
            cur.execute(f"SELECT * FROM {table_name} ORDER BY from_date, ledger_account, characteristic")

            # Получаем заголовки колонок
            column_names = [desc[0] for desc in cur.description]

            # Получаем все строки
            rows = cur.fetchall()

            # Записываем в CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, delimiter=';')

                # Записываем заголовки
                writer.writerow(column_names)

                # Записываем данные
                for row in rows:
                    # Форматируем значения для корректного экспорта
                    formatted_row = []
                    for value in row:
                        if value is None:
                            formatted_row.append('')
                        elif isinstance(value, datetime):
                            formatted_row.append(value.strftime('%Y-%m-%d'))
                        else:
                            formatted_row.append(str(value))
                    writer.writerow(formatted_row)

            print(f"[{datetime.now()}] Успешно экспортировано {len(rows)} строк в {output_file}")

            # Логируем экспорт
            cur.execute("""
                INSERT INTO logs.etl_log (table_name, start_time, status, rows_processed)
                VALUES (%s, %s, 'COMPLETED', %s)
            """, (f'export_{table_name}', datetime.now(), len(rows)))
            conn.commit()

            return len(rows)

    except Exception as e:
        print(f"[{datetime.now()}] Ошибка при экспорте таблицы: {e}")
        # Логируем ошибку
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs.etl_log (table_name, start_time, status, error_message)
                VALUES (%s, %s, 'FAILED', %s)
            """, (f'export_{table_name}', datetime.now(), str(e)))
            conn.commit()
        raise


def main():
    # Создаем папку для экспорта, если её нет
    export_dir = 'export'
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)

    output_file = os.path.join(export_dir, 'dm_f101_round_f.csv')

    conn = get_db_connection()

    try:
        print(f"[{datetime.now()}] Начало экспорта таблицы dm.dm_f101_round_f...")

        rows_count = export_table_to_csv(conn, 'dm.dm_f101_round_f', output_file)

        print(f"[{datetime.now()}] Экспорт завершен! {rows_count} строк экспортировано в {output_file}")

        # Показываем первые 5 строк файла для проверки
        print(f"\n[{datetime.now()}] Предпросмотр экспортированного файла:")
        with open(output_file, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i < 6:  # Заголовок + 5 строк данных
                    print(f"  {line.strip()}")
                else:
                    break

    finally:
        conn.close()


if __name__ == "__main__":
    main()