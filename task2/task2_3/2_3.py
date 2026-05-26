import psycopg2
from configurations import DB_CONFIG


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


def analyze_inconsistencies(conn):
    print("\n" + "=" * 60)
    print("АНАЛИЗ НЕСООТВЕТСТВИЙ В БАЛАНСАХ СЧЕТОВ")
    print("=" * 60)

    cursor = conn.cursor()

    try:
        cursor.execute("""
            WITH account_balance_with_prev AS (
                SELECT 
                    ab.account_rk,
                    ab.effective_date,
                    ab.account_in_sum,
                    ab.account_out_sum,
                    LAG(ab.account_out_sum) OVER (
                        PARTITION BY ab.account_rk 
                        ORDER BY ab.effective_date
                    ) AS prev_day_account_out_sum
                FROM rd.account_balance ab
            )
            SELECT 
                COUNT(*) as total_inconsistencies,
                COUNT(DISTINCT account_rk) as affected_accounts
            FROM account_balance_with_prev
            WHERE prev_day_account_out_sum IS NOT NULL 
              AND account_in_sum != prev_day_account_out_sum
        """)

        result = cursor.fetchone()
        print(f"\nНайдено несоответствий: {result[0]}")
        print(f"Затронуто счетов: {result[1]}")

        if result[0] > 0:
            cursor.execute("""
                WITH account_balance_with_prev AS (
                    SELECT 
                        ab.account_rk,
                        ab.effective_date,
                        ab.account_in_sum,
                        ab.account_out_sum,
                        LAG(ab.account_out_sum) OVER (
                            PARTITION BY ab.account_rk 
                            ORDER BY ab.effective_date
                        ) AS prev_day_account_out_sum
                    FROM rd.account_balance ab
                )
                SELECT 
                    account_rk,
                    effective_date,
                    account_in_sum,
                    prev_day_account_out_sum,
                    (account_in_sum - prev_day_account_out_sum) as difference
                FROM account_balance_with_prev
                WHERE prev_day_account_out_sum IS NOT NULL 
                  AND account_in_sum != prev_day_account_out_sum
                LIMIT 10
            """)

            print("\nПримеры несоответствий (первые 10):")
            print(f"{'Счет':<10} {'Дата':<12} {'account_in_sum':<15} {'prev_out_sum':<15} {'Разница':<12}")
            print("-" * 64)
            for row in cursor.fetchall():
                print(f"{row[0]:<10} {row[1]} {row[2]:<15} {row[3]:<15} {row[4]:<12}")

    finally:
        cursor.close()


def check_correct_account_in_sum(conn):
    print("\n" + "=" * 60)
    print("ЗАПРОС 1: Корректное значение account_in_sum")
    print("=" * 60)

    cursor = conn.cursor()

    try:
        cursor.execute("""
            WITH account_balance_with_prev AS (
                SELECT 
                    ab.account_rk,
                    ab.effective_date,
                    ab.account_in_sum,
                    ab.account_out_sum,
                    LAG(ab.account_out_sum) OVER (
                        PARTITION BY ab.account_rk 
                        ORDER BY ab.effective_date
                    ) AS prev_day_account_out_sum
                FROM rd.account_balance ab
            ),
            corrected_balance AS (
                SELECT 
                    account_rk,
                    effective_date,
                    account_in_sum AS original_account_in_sum,
                    account_out_sum,
                    prev_day_account_out_sum,
                    CASE 
                        WHEN prev_day_account_out_sum IS NOT NULL 
                             AND account_in_sum != prev_day_account_out_sum 
                        THEN prev_day_account_out_sum
                        ELSE account_in_sum
                    END AS corrected_account_in_sum,
                    CASE 
                        WHEN prev_day_account_out_sum IS NOT NULL 
                             AND account_in_sum != prev_day_account_out_sum 
                        THEN TRUE
                        ELSE FALSE
                    END AS was_corrected
                FROM account_balance_with_prev
            )
            SELECT 
                cb.account_rk,
                cb.effective_date,
                cb.original_account_in_sum,
                cb.corrected_account_in_sum,
                cb.account_out_sum,
                cb.prev_day_account_out_sum,
                cb.was_corrected
            FROM corrected_balance cb
            WHERE cb.was_corrected = TRUE
            ORDER BY cb.account_rk, cb.effective_date
            LIMIT 20
        """)

        results = cursor.fetchall()

        if results:
            print(f"\nНайдено записей требующих коррекции account_in_sum: {len(results)} (показаны первые 20)")
            print(
                f"{'Счет':<10} {'Дата':<12} {'Исходный_in':<15} {'Скорректированный_in':<20} {'out_sum':<15} {'prev_out':<15}")
            print("-" * 87)
            for row in results:
                print(f"{row[0]:<10} {row[1]} {row[2]:<15} {row[3]:<20} {row[4]:<15} {row[5]:<15}")
        else:
            print("\nНесоответствий для account_in_sum не найдено")

    finally:
        cursor.close()


def check_correct_account_out_sum(conn):
    print("\n" + "=" * 60)
    print("ЗАПРОС 2: Корректное значение account_out_sum")
    print("=" * 60)

    cursor = conn.cursor()

    try:
        cursor.execute("""
            WITH account_balance_with_next AS (
                SELECT 
                    ab.account_rk,
                    ab.effective_date,
                    ab.account_in_sum,
                    ab.account_out_sum,
                    LEAD(ab.account_in_sum) OVER (
                        PARTITION BY ab.account_rk 
                        ORDER BY ab.effective_date
                    ) AS next_day_account_in_sum
                FROM rd.account_balance ab
            ),
            corrected_balance AS (
                SELECT 
                    account_rk,
                    effective_date,
                    account_in_sum,
                    account_out_sum AS original_account_out_sum,
                    next_day_account_in_sum,
                    CASE 
                        WHEN next_day_account_in_sum IS NOT NULL 
                             AND account_out_sum != next_day_account_in_sum 
                        THEN next_day_account_in_sum
                        ELSE account_out_sum
                    END AS corrected_account_out_sum,
                    CASE 
                        WHEN next_day_account_in_sum IS NOT NULL 
                             AND account_out_sum != next_day_account_in_sum 
                        THEN TRUE
                        ELSE FALSE
                    END AS was_corrected
                FROM account_balance_with_next
            )
            SELECT 
                cb.account_rk,
                cb.effective_date,
                cb.account_in_sum,
                cb.original_account_out_sum,
                cb.corrected_account_out_sum,
                cb.next_day_account_in_sum,
                cb.was_corrected
            FROM corrected_balance cb
            WHERE cb.was_corrected = TRUE
            ORDER BY cb.account_rk, cb.effective_date
            LIMIT 20
        """)

        results = cursor.fetchall()

        if results:
            print(f"\nНайдено записей требующих коррекции account_out_sum: {len(results)} (показаны первые 20)")
            print(
                f"{'Счет':<10} {'Дата':<12} {'in_sum':<15} {'Исходный_out':<15} {'Скорректированный_out':<20} {'next_in':<15}")
            print("-" * 87)
            for row in results:
                print(f"{row[0]:<10} {row[1]} {row[2]:<15} {row[3]:<15} {row[4]:<20} {row[5]:<15}")
        else:
            print("\nНесоответствий для account_out_sum не найдено")

    finally:
        cursor.close()


def update_account_balance(conn):
    print("\n" + "=" * 60)
    print("ЗАПРОС 3: Исправление данных в rd.account_balance")
    print("=" * 60)

    cursor = conn.cursor()

    try:
        cursor.execute("""
            WITH account_balance_with_prev AS (
                SELECT 
                    ab.account_rk,
                    ab.effective_date,
                    ab.account_in_sum,
                    ab.account_out_sum,
                    LAG(ab.account_out_sum) OVER (
                        PARTITION BY ab.account_rk 
                        ORDER BY ab.effective_date
                    ) AS prev_day_account_out_sum
                FROM rd.account_balance ab
            ),
            corrections_needed AS (
                SELECT 
                    account_rk,
                    effective_date,
                    account_in_sum AS original_account_in_sum,
                    prev_day_account_out_sum AS corrected_account_in_sum
                FROM account_balance_with_prev
                WHERE prev_day_account_out_sum IS NOT NULL 
                  AND account_in_sum != prev_day_account_out_sum
            )
            UPDATE rd.account_balance 
            SET account_in_sum = cn.corrected_account_in_sum
            FROM corrections_needed cn
            WHERE rd.account_balance.account_rk = cn.account_rk
              AND rd.account_balance.effective_date = cn.effective_date
        """)

        conn.commit()
        updated_count = cursor.rowcount
        print(f"\nОбновлено записей: {updated_count}")

        return updated_count

    except Exception as e:
        conn.rollback()
        print(f"Ошибка при обновлении: {e}")
        raise
    finally:
        cursor.close()


def create_refresh_procedure(conn):
    print("\n" + "=" * 60)
    print("СОЗДАНИЕ ПРОЦЕДУРЫ ДЛЯ ПЕРЕЗАГРУЗКИ ВИТРИНЫ")
    print("=" * 60)

    procedure_sql = """
    CREATE OR REPLACE PROCEDURE refresh_account_balance_turnover()
    LANGUAGE plpgsql
    AS $$
    DECLARE
        v_start_time TIMESTAMP;
        v_end_time TIMESTAMP;
        v_rows_inserted INTEGER;
        v_error_message TEXT;
        v_log_id INTEGER;
    BEGIN
        v_start_time := NOW();

        INSERT INTO logs.etl_log (table_name, start_time, status)
        VALUES ('dm.account_balance_turnover', v_start_time, 'IN PROGRESS')
        RETURNING log_id INTO v_log_id;

        DELETE FROM dm.account_balance_turnover;

        INSERT INTO dm.account_balance_turnover (
            account_rk,
            currency_name,
            department_rk,
            effective_date,
            account_in_sum,
            account_out_sum
        )
        WITH corrected_account_balance AS (
            SELECT 
                ab.account_rk,
                ab.effective_date,
                CASE 
                    WHEN LAG(ab.account_out_sum) OVER (
                        PARTITION BY ab.account_rk 
                        ORDER BY ab.effective_date
                    ) IS NOT NULL 
                    AND ab.account_in_sum != LAG(ab.account_out_sum) OVER (
                        PARTITION BY ab.account_rk 
                        ORDER BY ab.effective_date
                    )
                    THEN LAG(ab.account_out_sum) OVER (
                        PARTITION BY ab.account_rk 
                        ORDER BY ab.effective_date
                    )
                    ELSE ab.account_in_sum
                END AS corrected_account_in_sum,
                ab.account_out_sum
            FROM rd.account_balance ab
        )
        SELECT 
            a.account_rk,
            COALESCE(dc.currency_name, '-1') AS currency_name,
            a.department_rk,
            cab.effective_date,
            cab.corrected_account_in_sum AS account_in_sum,
            cab.account_out_sum
        FROM rd.account a
        LEFT JOIN corrected_account_balance cab ON a.account_rk = cab.account_rk
        LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd
        WHERE cab.effective_date IS NOT NULL
        ORDER BY a.account_rk, cab.effective_date;

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
        v_end_time := NOW();

        UPDATE logs.etl_log 
        SET end_time = v_end_time, 
            status = 'COMPLETED', 
            rows_processed = v_rows_inserted
        WHERE log_id = v_log_id;

        RAISE NOTICE 'Витрина обновлена. Вставлено строк: %', v_rows_inserted;

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
        v_end_time := NOW();

        UPDATE logs.etl_log 
        SET end_time = v_end_time, 
            status = 'FAILED', 
            error_message = v_error_message
        WHERE log_id = v_log_id;

        RAISE NOTICE 'Ошибка: %', v_error_message;
    END;
    $$;
    """

    with conn.cursor() as cur:
        cur.execute(procedure_sql)
        conn.commit()
    print("Процедура refresh_account_balance_turnover создана")


def refresh_mart(conn):
    print("\n" + "=" * 60)
    print("ПЕРЕЗАГРУЗКА ВИТРИНЫ")
    print("=" * 60)

    with conn.cursor() as cur:
        cur.execute("CALL refresh_account_balance_turnover()")
        conn.commit()
        print("Витрина dm.account_balance_turnover успешно перезагружена")

        cur.execute("SELECT COUNT(*) FROM dm.account_balance_turnover")
        count = cur.fetchone()[0]
        print(f"Количество записей в витрине: {count}")


def verify_fix(conn):
    print("\n" + "=" * 60)
    print("ПРОВЕРКА РЕЗУЛЬТАТА")
    print("=" * 60)

    cursor = conn.cursor()

    try:
        cursor.execute("""
            WITH balance_check AS (
                SELECT 
                    account_rk,
                    effective_date,
                    account_in_sum,
                    account_out_sum,
                    LAG(account_out_sum) OVER (
                        PARTITION BY account_rk 
                        ORDER BY effective_date
                    ) AS prev_account_out_sum
                FROM dm.account_balance_turnover
            )
            SELECT 
                COUNT(*) AS remaining_inconsistencies
            FROM balance_check
            WHERE prev_account_out_sum IS NOT NULL 
              AND account_in_sum != prev_account_out_sum
        """)

        remaining = cursor.fetchone()[0]

        if remaining == 0:
            print("\nПроверка пройдена! Несоответствий не обнаружено.")
        else:
            print(f"\nВНИМАНИЕ! Осталось несоответствий: {remaining}")

        cursor.execute("""
            SELECT 
                COUNT(*) AS total_records,
                COUNT(DISTINCT account_rk) AS unique_accounts,
                MIN(effective_date) AS min_date,
                MAX(effective_date) AS max_date
            FROM dm.account_balance_turnover
        """)

        result = cursor.fetchone()
        print(f"\nСтатистика витрины:")
        print(f"  Всего записей: {result[0]}")
        print(f"  Уникальных счетов: {result[1]}")
        print(f"  Диапазон дат: {result[2]} - {result[3]}")

    finally:
        cursor.close()


def main():
    print("\n" + "=" * 60)
    print("ЗАДАНИЕ 2.3: Исправление балансов счетов")
    print("=" * 60)

    conn = get_db_connection()

    try:
        print("\n[1/6] Анализ несоответствий в данных...")
        analyze_inconsistencies(conn)

        print("\n[2/6] Проверка корректных значений account_in_sum...")
        check_correct_account_in_sum(conn)

        print("\n[3/6] Проверка корректных значений account_out_sum...")
        check_correct_account_out_sum(conn)

        print("\n[4/6] Исправление данных в rd.account_balance...")
        updated_count = update_account_balance(conn)

        if updated_count > 0:
            print(f"\n[5/6] Создание процедуры перезагрузки витрины...")
            create_refresh_procedure(conn)

            print("\n[6/6] Перезагрузка витрины...")
            refresh_mart(conn)

            print("\n" + "=" * 60)
            print("ПРОВЕРКА РЕЗУЛЬТАТА")
            print("=" * 60)
            verify_fix(conn)
        else:
            print("\nНесоответствий не найдено, обновление не требуется.")

        print("\n" + "=" * 60)
        print("ЗАДАНИЕ 2.3 ВЫПОЛНЕНО УСПЕШНО")
        print("=" * 60)

    except Exception as e:
        print(f"\nОШИБКА: {str(e)}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    from datetime import datetime

    main()