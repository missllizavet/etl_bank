import psycopg2
import pandas as pd
import os
from datetime import datetime

DB_CONFIG = {
    'host': '192.168.0.22',
    'database': 'bank_db_airflow',
    'user': 'postgres',
    'password': 'p@ssw0rd',
    'port': 5432
}

DATA_DIR = '/home/missllizavet/airflow/data'
LOG_FILE = '/home/missllizavet/airflow/logs/import_f101.log'

def write_log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{timestamp}] {message}"
    print(log_message)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_message + '\n')

def create_copy_table():
    write_log("Создание таблицы-копии dm.dm_f101_round_f_v2")
    
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS dm.dm_f101_round_f_v2;
            CREATE TABLE dm.dm_f101_round_f_v2 (LIKE dm.dm_f101_round_f INCLUDING ALL);
        """)
        conn.commit()
    conn.close()
    
    write_log("Таблица dm.dm_f101_round_f_v2 успешно создана")

def import_csv_to_table(csv_file, table_name):
    write_log(f"Начало импорта данных в {table_name}")
    
    try:
        write_log(f"Чтение файла: {csv_file}")
        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        write_log(f"Загружено {len(df)} строк из CSV")
        
        conn = psycopg2.connect(**DB_CONFIG)
        
        write_log(f"Очистка таблицы {table_name}")
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table_name};")
            conn.commit()
        
        write_log(f"Вставка данных в таблицу {table_name}")
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                sql = """
                    INSERT INTO dm.dm_f101_round_f_v2 (
                        from_date, to_date, chapter, ledger_account, characteristic,
                        balance_in_rub, r_balance_in_rub, balance_in_val, r_balance_in_val,
                        balance_in_total, r_balance_in_total, turn_deb_rub, r_turn_deb_rub,
                        turn_deb_val, r_turn_deb_val, turn_deb_total, r_turn_deb_total,
                        turn_cre_rub, r_turn_cre_rub, turn_cre_val, r_turn_cre_val,
                        turn_cre_total, r_turn_cre_total, balance_out_rub, r_balance_out_rub,
                        balance_out_val, r_balance_out_val, balance_out_total, r_balance_out_total
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    row['from_date'], row['to_date'], row['chapter'], row['ledger_account'], row['characteristic'],
                    row['balance_in_rub'], row['r_balance_in_rub'], row['balance_in_val'], row['r_balance_in_val'],
                    row['balance_in_total'], row['r_balance_in_total'], row['turn_deb_rub'], row['r_turn_deb_rub'],
                    row['turn_deb_val'], row['r_turn_deb_val'], row['turn_deb_total'], row['r_turn_deb_total'],
                    row['turn_cre_rub'], row['r_turn_cre_rub'], row['turn_cre_val'], row['r_turn_cre_val'],
                    row['turn_cre_total'], row['r_turn_cre_total'], row['balance_out_rub'], row['r_balance_out_rub'],
                    row['balance_out_val'], row['r_balance_out_val'], row['balance_out_total'], row['r_balance_out_total']
                )
                cur.execute(sql, values)
            conn.commit()
        
        write_log(f"Успешно импортировано {len(df)} строк в таблицу {table_name}")
        
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            write_log(f"Проверка: в таблице {table_name} теперь {count} строк")
        
        conn.close()
        write_log(f"Импорт в {table_name} успешно завершен\n")
        
    except Exception as e:
        write_log(f"Ошибка при импорте: {str(e)}")
        raise

def modify_csv_values(csv_file):
    write_log("\nИзменение данных в CSV файле")
    
    df = pd.read_csv(csv_file, encoding='utf-8-sig')
    write_log(f"Исходное количество строк: {len(df)}")
    
    if len(df) > 0:
        write_log("Изменяем значения первых строк для демонстрации")
        
        for i in range(min(3, len(df))):
            original = df.loc[i, 'balance_out_rub']
            df.loc[i, 'balance_out_rub'] = df.loc[i, 'balance_out_rub'] + 1000
            write_log(f"  Строка {i+1}: balance_out_rub изменен с {original} на {df.loc[i, 'balance_out_rub']}")
    
    modified_file = csv_file.replace('.csv', '_modified.csv')
    df.to_csv(modified_file, index=False, encoding='utf-8-sig')
    write_log(f"Измененный файл сохранен: {modified_file}")
    
    return modified_file

if __name__ == "__main__":
    csv_file = os.path.join(DATA_DIR, 'dm_f101_round_f_export.csv')
    create_copy_table()
    modified_file = modify_csv_values(csv_file)
    import_csv_to_table(modified_file, 'dm.dm_f101_round_f_v2')
