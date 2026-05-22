import psycopg2
from datetime import datetime
from task1.configurations import DB_CONFIG


# Устанавливает соединение с базой данных
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


# Рассчитывает 101 форму на отчетную дату
def calculate_f101(conn, report_date):
    date_str = report_date.strftime('%Y-%m-%d')
    print(f"[{datetime.now()}] Расчет формы 101 за период, оканчивающийся {date_str}...")

    with conn.cursor() as cur:
        try:
            # Вызываем процедуру расчета 101 формы
            cur.execute("CALL dm.fill_f101_round_f(%s::DATE)", (date_str,))
            conn.commit()
            print(f"[{datetime.now()}] Форма 101 успешно рассчитана")

        except Exception as e:
            conn.rollback()
            print(f"[{datetime.now()}] Ошибка при расчете формы 101: {e}")
            raise


# Главная функция: расчет формы 101 (оборотно-сальдовая ведомость)
def main():
    conn = get_db_connection()

    # Для расчета за январь 2018 года передаем 1 февраля 2018
    report_date = datetime(2018, 2, 1).date()

    try:
        print(f"[{datetime.now()}] Начало расчета формы 101...")
        print(f"[{datetime.now()}] Отчетный период: январь 2018 года (дата окончания: {report_date})")

        calculate_f101(conn, report_date)

        print(f"[{datetime.now()}] Расчет формы 101 завершен!")

        # Проверяем результаты
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) as cnt, 
                       MIN(from_date) as min_date, 
                       MAX(to_date) as max_date
                FROM dm.dm_f101_round_f
            """)
            result = cur.fetchone()
            print(f"[{datetime.now()}] Всего строк в таблице F101: {result[0]}")
            print(f"[{datetime.now()}] Период: с {result[1]} по {result[2]}")

            # Выводим первые 10 строк для проверки
            cur.execute("""
                SELECT * FROM dm.dm_f101_round_f
                ORDER BY ledger_account, characteristic
                LIMIT 10
            """)
            rows = cur.fetchall()
            print(f"[{datetime.now()}] Пример данных (первые 10 строк):")
            for row in rows:
                print(f"  {row}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()