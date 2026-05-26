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

def create_dm_schema():
    hook = PostgresHook(postgres_conn_id='bank_postgres')
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dm;")
        conn.commit()
    conn.close()
    print("Schema dm created")

def create_dm_tables():
    hook = PostgresHook(postgres_conn_id='bank_postgres')
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS dm.dm_account_turnover_f;
            CREATE TABLE dm.dm_account_turnover_f (
                on_date DATE,
                account_rk BIGINT,
                credit_amount NUMERIC(23,8),
                credit_amount_rub NUMERIC(23,8),
                debet_amount NUMERIC(23,8),
                debet_amount_rub NUMERIC(23,8),
                PRIMARY KEY (on_date, account_rk)
            );
            DROP TABLE IF EXISTS dm.dm_account_balance_f;
            CREATE TABLE dm.dm_account_balance_f (
                on_date DATE,
                account_rk BIGINT,
                balance_out NUMERIC(23,8),
                balance_out_rub NUMERIC(23,8),
                PRIMARY KEY (on_date, account_rk)
            );
        """)
        conn.commit()
    conn.close()
    print("DM tables created")

def create_procedures():
    hook = PostgresHook(postgres_conn_id='bank_postgres')
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
            LANGUAGE plpgsql AS $$
            DECLARE
                v_start_time TIMESTAMP;
                v_end_time TIMESTAMP;
                v_rows_affected INTEGER;
                v_log_id INTEGER;
            BEGIN
                v_start_time := NOW();
                INSERT INTO logs.etl_log (table_name, start_time, status)
                VALUES ('dm.dm_account_turnover_f', v_start_time, 'IN PROGRESS')
                RETURNING log_id INTO v_log_id;
                DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;
                WITH credit_turnovers AS (
                    SELECT p.oper_date, p.credit_account_rk AS account_rk,
                        SUM(p.credit_amount) AS credit_amount,
                        SUM(p.credit_amount * COALESCE(er.reduced_cource, 1)) AS credit_amount_rub
                    FROM ds.ft_posting_f p
                    LEFT JOIN ds.md_exchange_rate_d er ON p.oper_date = er.data_actual_date
                        AND er.currency_rk = (SELECT a.currency_rk FROM ds.md_account_d a
                            WHERE a.account_rk = p.credit_account_rk
                            AND a.data_actual_date <= p.oper_date
                            AND a.data_actual_end_date >= p.oper_date LIMIT 1)
                    WHERE p.oper_date = i_OnDate
                    GROUP BY p.oper_date, p.credit_account_rk
                ), debet_turnovers AS (
                    SELECT p.oper_date, p.debet_account_rk AS account_rk,
                        SUM(p.debet_amount) AS debet_amount,
                        SUM(p.debet_amount * COALESCE(er.reduced_cource, 1)) AS debet_amount_rub
                    FROM ds.ft_posting_f p
                    LEFT JOIN ds.md_exchange_rate_d er ON p.oper_date = er.data_actual_date
                        AND er.currency_rk = (SELECT a.currency_rk FROM ds.md_account_d a
                            WHERE a.account_rk = p.debet_account_rk
                            AND a.data_actual_date <= p.oper_date
                            AND a.data_actual_end_date >= p.oper_date LIMIT 1)
                    WHERE p.oper_date = i_OnDate
                    GROUP BY p.oper_date, p.debet_account_rk
                )
                INSERT INTO dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
                SELECT i_OnDate, COALESCE(c.account_rk, d.account_rk),
                    COALESCE(c.credit_amount, 0), COALESCE(c.credit_amount_rub, 0),
                    COALESCE(d.debet_amount, 0), COALESCE(d.debet_amount_rub, 0)
                FROM credit_turnovers c FULL OUTER JOIN debet_turnovers d ON c.account_rk = d.account_rk;
                GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
                v_end_time := NOW();
                UPDATE logs.etl_log SET end_time = v_end_time, status = 'COMPLETED', rows_processed = v_rows_affected
                WHERE log_id = v_log_id;
                RAISE NOTICE 'Turnover for % done', i_OnDate;
            EXCEPTION WHEN OTHERS THEN
                UPDATE logs.etl_log SET end_time = NOW(), status = 'FAILED', error_message = SQLERRM
                WHERE log_id = v_log_id;
                RAISE;
            END;
            $$;
        """)
        cur.execute("""
            CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f_initial()
            LANGUAGE plpgsql AS $$
            DECLARE
                v_start_time TIMESTAMP;
                v_end_time TIMESTAMP;
                v_rows_affected INTEGER;
                v_log_id INTEGER;
            BEGIN
                v_start_time := NOW();
                INSERT INTO logs.etl_log (table_name, start_time, status)
                VALUES ('dm.dm_account_balance_f', v_start_time, 'IN PROGRESS')
                RETURNING log_id INTO v_log_id;
                DELETE FROM dm.dm_account_balance_f WHERE on_date = '2017-12-31';
                INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
                SELECT b.on_date, b.account_rk, b.balance_out,
                    b.balance_out * COALESCE(er.reduced_cource, 1)
                FROM ds.ft_balance_f b
                LEFT JOIN ds.md_exchange_rate_d er ON b.on_date = er.data_actual_date AND er.currency_rk = b.currency_rk
                WHERE b.on_date = '2017-12-31';
                GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
                v_end_time := NOW();
                UPDATE logs.etl_log SET end_time = v_end_time, status = 'COMPLETED', rows_processed = v_rows_affected
                WHERE log_id = v_log_id;
            EXCEPTION WHEN OTHERS THEN
                UPDATE logs.etl_log SET end_time = NOW(), status = 'FAILED', error_message = SQLERRM
                WHERE log_id = v_log_id;
                RAISE;
            END;
            $$;
        """)
        cur.execute("""
            CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
            LANGUAGE plpgsql AS $$
            DECLARE
                v_start_time TIMESTAMP;
                v_end_time TIMESTAMP;
                v_rows_affected INTEGER;
                v_log_id INTEGER;
                v_prev_date DATE := i_OnDate - 1;
            BEGIN
                v_start_time := NOW();
                INSERT INTO logs.etl_log (table_name, start_time, status)
                VALUES ('dm.dm_account_balance_f', v_start_time, 'IN PROGRESS')
                RETURNING log_id INTO v_log_id;
                DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;
                INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
                SELECT i_OnDate, a.account_rk,
                    CASE WHEN a.char_type = 'А' THEN
                        COALESCE(prev.balance_out, 0) + COALESCE(t.debet_amount, 0) - COALESCE(t.credit_amount, 0)
                    ELSE
                        COALESCE(prev.balance_out, 0) - COALESCE(t.debet_amount, 0) + COALESCE(t.credit_amount, 0)
                    END,
                    CASE WHEN a.char_type = 'А' THEN
                        COALESCE(prev.balance_out_rub, 0) + COALESCE(t.debet_amount_rub, 0) - COALESCE(t.credit_amount_rub, 0)
                    ELSE
                        COALESCE(prev.balance_out_rub, 0) - COALESCE(t.debet_amount_rub, 0) + COALESCE(t.credit_amount_rub, 0)
                    END
                FROM ds.md_account_d a
                LEFT JOIN dm.dm_account_balance_f prev ON a.account_rk = prev.account_rk AND prev.on_date = v_prev_date
                LEFT JOIN dm.dm_account_turnover_f t ON a.account_rk = t.account_rk AND t.on_date = i_OnDate
                WHERE a.data_actual_date <= i_OnDate AND a.data_actual_end_date >= i_OnDate;
                GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
                v_end_time := NOW();
                UPDATE logs.etl_log SET end_time = v_end_time, status = 'COMPLETED', rows_processed = v_rows_affected
                WHERE log_id = v_log_id;
            EXCEPTION WHEN OTHERS THEN
                UPDATE logs.etl_log SET end_time = NOW(), status = 'FAILED', error_message = SQLERRM
                WHERE log_id = v_log_id;
                RAISE;
            END;
            $$;
        """)
        conn.commit()
    conn.close()
    print("Procedures created")

def init_balance():
    hook = PostgresHook(postgres_conn_id='bank_postgres')
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("CALL ds.fill_account_balance_f_initial();")
        conn.commit()
    conn.close()
    print("Initial balance done")

def run_turnover(date_str):
    hook = PostgresHook(postgres_conn_id='bank_postgres')
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute(f"CALL ds.fill_account_turnover_f('{date_str}');")
        conn.commit()
    conn.close()
    print(f"Turnover for {date_str} done")

def run_balance(date_str):
    hook = PostgresHook(postgres_conn_id='bank_postgres')
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute(f"CALL ds.fill_account_balance_f('{date_str}');")
        conn.commit()
    conn.close()
    print(f"Balance for {date_str} done")

dag = DAG(
    'datamart_calculation',
    default_args=default_args,
    description='DM calculation',
    schedule=None,
    catchup=False,
    tags=['dm'],
)

start = EmptyOperator(task_id='start', dag=dag)
create_schema = PythonOperator(task_id='create_dm_schema', python_callable=create_dm_schema, dag=dag)
create_tables = PythonOperator(task_id='create_dm_tables', python_callable=create_dm_tables, dag=dag)
create_proc = PythonOperator(task_id='create_procedures', python_callable=create_procedures, dag=dag)
init = PythonOperator(task_id='init_balance', python_callable=init_balance, dag=dag)

dates = ['2018-01-01', '2018-01-02', '2018-01-03', '2018-01-04', '2018-01-05',
         '2018-01-06', '2018-01-07', '2018-01-08', '2018-01-09', '2018-01-10',
         '2018-01-11', '2018-01-12', '2018-01-13', '2018-01-14', '2018-01-15',
         '2018-01-16', '2018-01-17', '2018-01-18', '2018-01-19', '2018-01-20',
         '2018-01-21', '2018-01-22', '2018-01-23', '2018-01-24', '2018-01-25',
         '2018-01-26', '2018-01-27', '2018-01-28', '2018-01-29', '2018-01-30',
         '2018-01-31']

prev_task = init
for date in dates:
    t = PythonOperator(task_id=f'turnover_{date}', python_callable=run_turnover, op_args=[date], dag=dag)
    b = PythonOperator(task_id=f'balance_{date}', python_callable=run_balance, op_args=[date], dag=dag)
    prev_task >> t >> b
    prev_task = b

end = EmptyOperator(task_id='end', dag=dag)
prev_task >> end

start >> create_schema >> create_tables >> create_proc >> init