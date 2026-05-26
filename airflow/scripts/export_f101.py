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
LOG_FILE = '/home/missllizavet/airflow/logs/export_f101.log'

def write_log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{timestamp}] {message}"
    print(log_message)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_message + '\n')

def export_f101_to_csv():
    write_log("Начало экспорта данных 101 формы")
    
    try:
        write_log("Подключение к базе данных")
        conn = psycopg2.connect(**DB_CONFIG)
        
        write_log("Выполнение запроса к таблице dm.dm_f101_round_f")
        query = """
            SELECT 
                from_date,
                to_date,
                chapter,
                ledger_account,
                characteristic,
                balance_in_rub,
                r_balance_in_rub,
                balance_in_val,
                r_balance_in_val,
                balance_in_total,
                r_balance_in_total,
                turn_deb_rub,
                r_turn_deb_rub,
                turn_deb_val,
                r_turn_deb_val,
                turn_deb_total,
                r_turn_deb_total,
                turn_cre_rub,
                r_turn_cre_rub,
                turn_cre_val,
                r_turn_cre_val,
                turn_cre_total,
                r_turn_cre_total,
                balance_out_rub,
                r_balance_out_rub,
                balance_out_val,
                r_balance_out_val,
                balance_out_total,
                r_balance_out_total
            FROM dm.dm_f101_round_f
            ORDER BY ledger_account, characteristic
        """
        
        df = pd.read_sql(query, conn)
        write_log(f"Загружено {len(df)} строк из базы данных")
        
        output_file = os.path.join(DATA_DIR, 'dm_f101_round_f_export.csv')
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        write_log(f"Данные экспортированы в файл: {output_file}")
        write_log(f"Размер файла: {os.path.getsize(output_file)} байт")
        
        conn.close()
        write_log("Экспорт успешно завершен")
        return output_file
        
    except Exception as e:
        write_log(f"Ошибка при экспорте: {str(e)}")
        raise

def show_sample_data(file_path, n=5):
    write_log(f"\nПервые {n} строк экспортированного файла:")
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    write_log(str(df.head(n)))

if __name__ == "__main__":
    output_file = export_f101_to_csv()
    show_sample_data(output_file)
