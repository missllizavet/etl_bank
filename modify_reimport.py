import csv
import os
from datetime import datetime
import psycopg2
from configurations import DB_CONFIG


# Устанавливает соединение с базой данных
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


# Модифицирует несколько значений в CSV-файле
def modify_csv_file(input_file, output_file):
    print(f"[{datetime.now()}] Модификация CSV-файла...")

    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=';')
        headers = next(reader)
        rows = list(reader)

    # Модифицируем несколько значений
    modifications = []
    for i, row in enumerate(rows):
        # Меняем значения в первых 3 строках для демонстрации
        if i < 3:
            old_values = row.copy()
            # Меняем balance_out_rub (индекс 21)
            if len(row) > 21 and row[21]:
                try:
                    old_val = float(row[21])
                    row[21] = str(old_val * 1.1)  # Увеличиваем на 10%
                    modifications.append({
                        'row': i,
                        'column': 'balance_out_rub',
                        'old_value': old_val,
                        'new_value': row[21]
                    })
                except:
                    pass

            # Меняем turn_deb_total (индекс 15)
            if len(row) > 15 and row[15]:
                try:
                    old_val = float(row[15])
                    row[15] = str(old_val * 0.9)  # Уменьшаем на 10%
                    modifications.append({
                        'row': i,
                        'column': 'turn_deb_total',
                        'old_value': old_val,
                        'new_value': row[15]
                    })
                except:
                    pass

    # Записываем модифицированный файл
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile, delimiter=';')
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"[{datetime.now()}] Внесенные изменения:")
    for mod in modifications:
        print(f"  Строка {mod['row']}: {mod['column']} изменено с {mod['old_value']} на {mod['new_value']}")

    print(f"[{datetime.now()}] Модифицированный файл сохранен в {output_file}")


# Сравнивает данные в двух таблицах
def compare_tables(conn, table1, table2):
    print(f"\n[{datetime.now()}] Сравнение таблиц {table1} и {table2}...")

    with conn.cursor() as cur:
        # Находим различия
        cur.execute(f"""
            SELECT 
                COALESCE(t1.ledger_account, t2.ledger_account) as ledger_account,
                COALESCE(t1.characteristic, t2.characteristic) as characteristic,
                t1.balance_out_rub as orig_balance,
                t2.balance_out_rub as mod_balance,
                t1.turn_deb_total as orig_turn_deb,
                t2.turn_deb_total as mod_turn_deb
            FROM {table1} t1
            FULL OUTER JOIN {table2} t2 
                ON t1.from_date = t2.from_date 
                AND t1.to_date = t2.to_date 
                AND t1.ledger_account = t2.ledger_account 
                AND t1.characteristic = t2.characteristic
            WHERE t1.balance_out_rub != t2.balance_out_rub 
                OR t1.turn_deb_total != t2.turn_deb_total
                OR t1.balance_out_rub IS NULL 
                OR t2.balance_out_rub IS NULL
            LIMIT 10
        """)

        differences = cur.fetchall()

        if differences:
            print(f"[{datetime.now()}] Найдено различий: {len(differences)} (показаны первые 10):")
            for diff in differences:
                print(f"  Счет: {diff[0]}, Характеристика: {diff[1]}")
                print(f"    Остаток: {diff[2]} -> {diff[3]}")
                print(f"    Оборот по дебету: {diff[4]} -> {diff[5]}")
        else:
            print(f"[{datetime.now()}] Различий не найдено (таблицы идентичны)")


def main():
    import_dir = 'export'
    original_file = os.path.join(import_dir, 'dm_f101_round_f.csv')
    modified_file = os.path.join(import_dir, 'dm_f101_round_f_modified.csv')

    if not os.path.exists(original_file):
        print(f"Ошибка: Файл {original_file} не найден!")
        print("Сначала запустите export_f101_to_csv.py")
        return

    # Модифицируем CSV-файл
    modify_csv_file(original_file, modified_file)

    # Импортируем модифицированный файл в новую таблицу
    conn = get_db_connection()

    try:
        from import_f101_from_csv import create_table_copy, import_csv_to_table

        target_table = 'dm.dm_f101_round_f_modified'
        source_table = 'dm.dm_f101_round_f'

        # Создаем таблицу для модифицированных данных
        print(f"\n[{datetime.now()}] Создание таблицы для модифицированных данных...")
        create_table_copy(conn, source_table, target_table)

        # Импортируем модифицированные данные
        print(f"[{datetime.now()}] Импорт модифицированных данных...")
        rows_count = import_csv_to_table(conn, target_table, modified_file)

        print(f"[{datetime.now()}] Импорт завершен! {rows_count} строк импортировано в {target_table}")

        # Сравниваем оригинальную и модифицированную таблицы
        compare_tables(conn, source_table, target_table)

    finally:
        conn.close()


if __name__ == "__main__":
    main()