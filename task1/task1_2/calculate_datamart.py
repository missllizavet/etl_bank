import psycopg2
from datetime import datetime, timedelta
from task1.configurations import DB_CONFIG


def get_db_connection():
#Устанавливает соединение с базой данных
    return psycopg2.connect(**DB_CONFIG)


def calculate_turnovers(conn, start_date, end_date):
    #Рассчитывает витрину оборотов за период
    cur_date = start_date
    while cur_date <= end_date:
        date_str = cur_date.strftime('%Y-%m-%d')
        print(f"[{datetime.now()}] Расчет оборотов за {date_str}...")

        with conn.cursor() as cur:
            try:
                cur.execute("CALL ds.fill_account_turnover_f(%s)", (date_str,))
                conn.commit()
                print(f"[{datetime.now()}] Обороты за {date_str} успешно рассчитаны")
            except Exception as e:
                conn.rollback()
                print(f"[{datetime.now()}] Ошибка при расчете оборотов за {date_str}: {e}")
                raise

        cur_date += timedelta(days=1)


def calculate_balances(conn, start_date, end_date):
    #Рассчитывает витрину остатков за период
    # Сначала заполняем начальные остатки за 31.12.2017
    print(f"[{datetime.now()}] Заполнение начальных остатков за 2017-12-31...")
    with conn.cursor() as cur:
        try:
            cur.execute("CALL ds.fill_account_balance_f_initial()")
            conn.commit()
            print(f"[{datetime.now()}] Начальные остатки успешно заполнены")
        except Exception as e:
            conn.rollback()
            print(f"[{datetime.now()}] Ошибка при заполнении начальных остатков: {e}")
            raise

    # Затем рассчитываем остатки за каждый день
    cur_date = start_date
    while cur_date <= end_date:
        date_str = cur_date.strftime('%Y-%m-%d')
        print(f"[{datetime.now()}] Расчет остатков за {date_str}...")

        with conn.cursor() as cur:
            try:
                cur.execute("CALL ds.fill_account_balance_f(%s)", (date_str,))
                conn.commit()
                print(f"[{datetime.now()}] Остатки за {date_str} успешно рассчитаны")
            except Exception as e:
                conn.rollback()
                print(f"[{datetime.now()}] Ошибка при расчете остатков за {date_str}: {e}")
                raise

        cur_date += timedelta(days=1)


def main():
    #Расчет витрин оборотов и остатков
    conn = get_db_connection()

    start_date = datetime(2018, 1, 1).date()
    end_date = datetime(2018, 1, 31).date()

    try:
        # Рассчитываем обороты за январь 2018
        print(f"[{datetime.now()}] Начало расчета оборотов за январь 2018 года...")
        calculate_turnovers(conn, start_date, end_date)
        print(f"[{datetime.now()}] Расчет оборотов завершен!")

        # Рассчитываем остатки за январь 2018
        print(f"[{datetime.now()}] Начало расчета остатков за январь 2018 года...")
        calculate_balances(conn, start_date, end_date)
        print(f"[{datetime.now()}] Расчет остатков завершен!")

        print(f"[{datetime.now()}] Все расчеты успешно завершены!")

    finally:
        conn.close()


if __name__ == "__main__":
    main()