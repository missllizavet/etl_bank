from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'bank_etl',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def create_f101_table():
    hook = PostgresHook(postgres_conn_id='bank_postgres')
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS dm.dm_f101_round_f;
            CREATE TABLE dm.dm_f101_round_f (
                from_date DATE,
                to_date DATE,
                chapter CHAR(1),
                ledger_account CHAR(5),
                characteristic CHAR(1),
                balance_in_rub NUMERIC(23,8),
                r_balance_in_rub NUMERIC(23,8),
                balance_in_val NUMERIC(23,8),
                r_balance_in_val NUMERIC(23,8),
                balance_in_total NUMERIC(23,8),
                r_balance_in_total NUMERIC(23,8),
                turn_deb_rub NUMERIC(23,8),
                r_turn_deb_rub NUMERIC(23,8),
                turn_deb_val NUMERIC(23,8),
                r_turn_deb_val NUMERIC(23,8),
                turn_deb_total NUMERIC(23,8),
                r_turn_deb_total NUMERIC(23,8),
                turn_cre_rub NUMERIC(23,8),
                r_turn_cre_rub NUMERIC(23,8),
                turn_cre_val NUMERIC(23,8),
                r_turn_cre_val NUMERIC(23,8),
                turn_cre_total NUMERIC(23,8),
                r_turn_cre_total NUMERIC(23,8),
                balance_out_rub NUMERIC(23,8),
                r_balance_out_rub NUMERIC(23,8),
                balance_out_val NUMERIC(23,8),
                r_balance_out_val NUMERIC(23,8),
                balance_out_total NUMERIC(23,8),
                r_balance_out_total NUMERIC(23,8),
                PRIMARY KEY (from_date, to_date, ledger_account, characteristic)
            );
        """)
        conn.commit()
    conn.close()
    print("Table dm.dm_f101_round_f created")

def create_f101_procedure():
    hook = PostgresHook(postgres_conn_id='bank_postgres')
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
            LANGUAGE plpgsql AS $$
            DECLARE
                v_start_time TIMESTAMP;
                v_end_time TIMESTAMP;
                v_rows_affected INTEGER;
                v_from_date DATE;
                v_to_date DATE;
                v_prev_date DATE;
                v_log_id INTEGER;
            BEGIN
                v_from_date := DATE_TRUNC('month', i_OnDate) - INTERVAL '1 month';
                v_to_date := i_OnDate - INTERVAL '1 day';
                v_prev_date := v_from_date - INTERVAL '1 day';

                v_start_time := NOW();
                INSERT INTO logs.etl_log (table_name, start_time, status)
                VALUES ('dm.dm_f101_round_f', v_start_time, 'IN PROGRESS')
                RETURNING log_id INTO v_log_id;

                RAISE NOTICE 'Расчет за период с % по %', v_from_date, v_to_date;

                DELETE FROM dm.dm_f101_round_f
                WHERE from_date = v_from_date AND to_date = v_to_date;

                WITH accounts AS (
                    SELECT DISTINCT
                        a.account_rk,
                        LEFT(a.account_number, 5) AS ledger_account,
                        a.char_type AS characteristic,
                        CASE WHEN a.currency_code IN ('810', '643') THEN 1 ELSE 0 END AS is_rub,
                        las.chapter
                    FROM ds.md_account_d a
                    LEFT JOIN ds.md_ledger_account_s las
                        ON LEFT(a.account_number, 5)::INTEGER = las.ledger_account
                    WHERE a.data_actual_date <= v_to_date
                        AND a.data_actual_end_date >= v_from_date
                ),
                balance_start AS (
                    SELECT account_rk, balance_out_rub
                    FROM dm.dm_account_balance_f
                    WHERE on_date = v_prev_date
                ),
                balance_end AS (
                    SELECT account_rk, balance_out_rub
                    FROM dm.dm_account_balance_f
                    WHERE on_date = v_to_date
                ),
                turnovers AS (
                    SELECT account_rk,
                        SUM(debet_amount_rub) AS total_deb_rub,
                        SUM(credit_amount_rub) AS total_cre_rub
                    FROM dm.dm_account_turnover_f
                    WHERE on_date BETWEEN v_from_date AND v_to_date
                    GROUP BY account_rk
                ),
                combined AS (
                    SELECT
                        a.ledger_account,
                        a.characteristic,
                        a.chapter,
                        a.is_rub,
                        COALESCE(bs.balance_out_rub, 0) AS balance_start_rub,
                        COALESCE(be.balance_out_rub, 0) AS balance_end_rub,
                        COALESCE(t.total_deb_rub, 0) AS deb_turn_rub,
                        COALESCE(t.total_cre_rub, 0) AS cre_turn_rub
                    FROM accounts a
                    LEFT JOIN balance_start bs ON a.account_rk = bs.account_rk
                    LEFT JOIN balance_end be ON a.account_rk = be.account_rk
                    LEFT JOIN turnovers t ON a.account_rk = t.account_rk
                )
                INSERT INTO dm.dm_f101_round_f (
                    from_date, to_date, chapter, ledger_account, characteristic,
                    balance_in_rub, balance_in_val, balance_in_total,
                    turn_deb_rub, turn_deb_val, turn_deb_total,
                    turn_cre_rub, turn_cre_val, turn_cre_total,
                    balance_out_rub, balance_out_val, balance_out_total
                )
                SELECT
                    v_from_date, v_to_date, chapter, ledger_account, characteristic,
                    SUM(CASE WHEN is_rub = 1 THEN balance_start_rub ELSE 0 END) AS balance_in_rub,
                    SUM(CASE WHEN is_rub = 0 THEN balance_start_rub ELSE 0 END) AS balance_in_val,
                    SUM(balance_start_rub) AS balance_in_total,
                    SUM(CASE WHEN is_rub = 1 THEN deb_turn_rub ELSE 0 END) AS turn_deb_rub,
                    SUM(CASE WHEN is_rub = 0 THEN deb_turn_rub ELSE 0 END) AS turn_deb_val,
                    SUM(deb_turn_rub) AS turn_deb_total,
                    SUM(CASE WHEN is_rub = 1 THEN cre_turn_rub ELSE 0 END) AS turn_cre_rub,
                    SUM(CASE WHEN is_rub = 0 THEN cre_turn_rub ELSE 0 END) AS turn_cre_val,
                    SUM(cre_turn_rub) AS turn_cre_total,
                    SUM(CASE WHEN is_rub = 1 THEN balance_end_rub ELSE 0 END) AS balance_out_rub,
                    SUM(CASE WHEN is_rub = 0 THEN balance_end_rub ELSE 0 END) AS balance_out_val,
                    SUM(balance_end_rub) AS balance_out_total
                FROM combined
                GROUP BY chapter, ledger_account, characteristic;

                GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

                UPDATE dm.dm_f101_round_f
                SET
                    r_balance_in_rub = balance_in_rub,
                    r_balance_in_val = balance_in_val,
                    r_balance_in_total = balance_in_total,
                    r_turn_deb_rub = turn_deb_rub,
                    r_turn_deb_val = turn_deb_val,
                    r_turn_deb_total = turn_deb_total,
                    r_turn_cre_rub = turn_cre_rub,
                    r_turn_cre_val = turn_cre_val,
                    r_turn_cre_total = turn_cre_total,
                    r_balance_out_rub = balance_out_rub,
                    r_balance_out_val = balance_out_val,
                    r_balance_out_total = balance_out_total
                WHERE from_date = v_from_date AND to_date = v_to_date;

                v_end_time := NOW();
                UPDATE logs.etl_log
                SET end_time = v_end_time, status = 'COMPLETED', rows_processed = v_rows_affected
                WHERE log_id = v_log_id;

                RAISE NOTICE 'F101 за период с % по % заполнена. Строк: %', v_from_date, v_to_date, v_rows_affected;

            EXCEPTION WHEN OTHERS THEN
                UPDATE logs.etl_log
                SET end_time = NOW(), status = 'FAILED', error_message = SQLERRM
                WHERE log_id = v_log_id;
                RAISE;
            END;
            $$;
        """)
        conn.commit()
    conn.close()
    print("Procedure dm.fill_f101_round_f created")

def calculate_f101():
    hook = PostgresHook(postgres_conn_id='bank_postgres')
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("CALL dm.fill_f101_round_f('2018-02-01'::DATE);")
        conn.commit()
    conn.close()
    print("F101 form calculated for January 2018")

def check_f101_result():
    hook = PostgresHook(postgres_conn_id='bank_postgres')
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) as total_rows,
                   MIN(from_date) as period_start,
                   MAX(to_date) as period_end
            FROM dm.dm_f101_round_f
        """)
        result = cur.fetchone()
        print(f"F101 table rows: {result[0]}")
        print(f"Period: {result[1]} - {result[2]}")
    conn.close()

dag = DAG(
    'f101_calculation',
    default_args=default_args,
    description='Расчет формы 101',
    schedule=None,
    catchup=False,
    tags=['f101', 'report'],
)

start = EmptyOperator(task_id='start', dag=dag)

create_table = PythonOperator(
    task_id='create_f101_table',
    python_callable=create_f101_table,
    dag=dag,
)

create_proc = PythonOperator(
    task_id='create_f101_procedure',
    python_callable=create_f101_procedure,
    dag=dag,
)

calculate = PythonOperator(
    task_id='calculate_f101',
    python_callable=calculate_f101,
    dag=dag,
)

check = PythonOperator(
    task_id='check_result',
    python_callable=check_f101_result,
    dag=dag,
)

end = EmptyOperator(task_id='end', dag=dag)

start >> create_table >> create_proc >> calculate >> check >> end
