-- Создание схемы DM
CREATE SCHEMA IF NOT EXISTS dm;

-- Создание витрины оборотов
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

-- Создание витрины остатков
DROP TABLE IF EXISTS dm.dm_account_balance_f;
CREATE TABLE dm.dm_account_balance_f (
    on_date DATE,
    account_rk BIGINT,
    balance_out NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8),
    PRIMARY KEY (on_date, account_rk)
);


-- Процедура заполнения витрины оборотов
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(
    i_OnDate DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_rows_affected INTEGER;
BEGIN
    -- Логируем старт
    v_start_time := NOW();
    INSERT INTO logs.etl_log (table_name, start_time, status)
    VALUES ('dm.dm_account_turnover_f', v_start_time, 'IN PROGRESS')
    RETURNING log_id INTO v_rows_affected;  -- Временно используем для log_id

    -- Удаляем записи за дату расчета (для возможности перезапуска)
    DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;

    -- Вставляем обороты по кредиту и дебету
    WITH credit_turnovers AS (
        SELECT
            p.oper_date,
            p.credit_account_rk AS account_rk,
            SUM(p.credit_amount) AS credit_amount,
            SUM(p.credit_amount * COALESCE(er.reduced_cource, 1)) AS credit_amount_rub
        FROM ds.ft_posting_f p
        LEFT JOIN ds.md_exchange_rate_d er
            ON p.oper_date = er.data_actual_date
            AND er.currency_rk = (
                SELECT a.currency_rk
                FROM ds.md_account_d a
                WHERE a.account_rk = p.credit_account_rk
                AND a.data_actual_date <= p.oper_date
                AND a.data_actual_end_date >= p.oper_date
                LIMIT 1
            )
        WHERE p.oper_date = i_OnDate
        GROUP BY p.oper_date, p.credit_account_rk
    ),
    debet_turnovers AS (
        SELECT
            p.oper_date,
            p.debet_account_rk AS account_rk,
            SUM(p.debet_amount) AS debet_amount,
            SUM(p.debet_amount * COALESCE(er.reduced_cource, 1)) AS debet_amount_rub
        FROM ds.ft_posting_f p
        LEFT JOIN ds.md_exchange_rate_d er
            ON p.oper_date = er.data_actual_date
            AND er.currency_rk = (
                SELECT a.currency_rk
                FROM ds.md_account_d a
                WHERE a.account_rk = p.debet_account_rk
                AND a.data_actual_date <= p.oper_date
                AND a.data_actual_end_date >= p.oper_date
                LIMIT 1
            )
        WHERE p.oper_date = i_OnDate
        GROUP BY p.oper_date, p.debet_account_rk
    )
    INSERT INTO dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    SELECT
        i_OnDate AS on_date,
        COALESCE(c.account_rk, d.account_rk) AS account_rk,
        COALESCE(c.credit_amount, 0) AS credit_amount,
        COALESCE(c.credit_amount_rub, 0) AS credit_amount_rub,
        COALESCE(d.debet_amount, 0) AS debet_amount,
        COALESCE(d.debet_amount_rub, 0) AS debet_amount_rub
    FROM credit_turnovers c
    FULL OUTER JOIN debet_turnovers d
        ON c.account_rk = d.account_rk
        AND c.oper_date = d.oper_date;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    -- Получаем ID последней записи лога для обновления
    -- (используем временную таблицу для передачи log_id)

    -- Логируем завершение
    v_end_time := NOW();
    UPDATE logs.etl_log
    SET end_time = v_end_time,
        status = 'COMPLETED',
        rows_processed = v_rows_affected
    WHERE table_name = 'dm.dm_account_turnover_f'
        AND start_time = v_start_time;

    -- Выводим информацию
    RAISE NOTICE 'Витрина dm_account_turnover_f за % заполнена. Обработано строк: %', i_OnDate, v_rows_affected;

EXCEPTION WHEN OTHERS THEN
    -- Логируем ошибку
    UPDATE logs.etl_log
    SET end_time = NOW(),
        status = 'FAILED',
        error_message = SQLERRM
    WHERE table_name = 'dm.dm_account_turnover_f'
        AND start_time = v_start_time;
    RAISE;
END;
$$;

-- Заполнение витрины остатков за 31.12.2017 из FT_BALANCE_F
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f_initial()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_rows_affected INTEGER;
    v_balance_date DATE := '2017-12-31';
BEGIN
    -- Логируем старт
    v_start_time := NOW();
    INSERT INTO logs.etl_log (table_name, start_time, status)
    VALUES ('dm.dm_account_balance_f', v_start_time, 'IN PROGRESS');

    -- Удаляем записи за 31.12.2017
    DELETE FROM dm.dm_account_balance_f WHERE on_date = v_balance_date;

    -- Заполняем начальные остатки
    INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        b.on_date,
        b.account_rk,
        b.balance_out,
        b.balance_out * COALESCE(er.reduced_cource, 1) AS balance_out_rub
    FROM ds.ft_balance_f b
    LEFT JOIN ds.md_exchange_rate_d er
        ON b.on_date = er.data_actual_date
        AND er.currency_rk = b.currency_rk
    WHERE b.on_date = v_balance_date;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    -- Логируем завершение
    v_end_time := NOW();
    UPDATE logs.etl_log
    SET end_time = v_end_time,
        status = 'COMPLETED',
        rows_processed = v_rows_affected
    WHERE table_name = 'dm.dm_account_balance_f'
        AND start_time = v_start_time;

    RAISE NOTICE 'Начальные остатки за % заполнены. Обработано строк: %', v_balance_date, v_rows_affected;

EXCEPTION WHEN OTHERS THEN
    UPDATE logs.etl_log
    SET end_time = NOW(),
        status = 'FAILED',
        error_message = SQLERRM
    WHERE table_name = 'dm.dm_account_balance_f'
        AND start_time = v_start_time;
    RAISE;
END;
$$;

-- Процедура заполнения витрины остатков
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(
    i_OnDate DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_rows_affected INTEGER;
    v_prev_date DATE := i_OnDate - INTERVAL '1 day';
BEGIN
    -- Логируем старт
    v_start_time := NOW();
    INSERT INTO logs.etl_log (table_name, start_time, status)
    VALUES ('dm.dm_account_balance_f', v_start_time, 'IN PROGRESS');

    -- Удаляем записи за дату расчета (для возможности перезапуска)
    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;

    -- Рассчитываем остатки
    INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        i_OnDate AS on_date,
        a.account_rk,
        CASE
            WHEN a.char_type = 'А' THEN
                COALESCE(prev.balance_out, 0)
                + COALESCE(t.debet_amount, 0)
                - COALESCE(t.credit_amount, 0)
            WHEN a.char_type = 'П' THEN
                COALESCE(prev.balance_out, 0)
                - COALESCE(t.debet_amount, 0)
                + COALESCE(t.credit_amount, 0)
            ELSE 0
        END AS balance_out,
        CASE
            WHEN a.char_type = 'А' THEN
                COALESCE(prev.balance_out_rub, 0)
                + COALESCE(t.debet_amount_rub, 0)
                - COALESCE(t.credit_amount_rub, 0)
            WHEN a.char_type = 'П' THEN
                COALESCE(prev.balance_out_rub, 0)
                - COALESCE(t.debet_amount_rub, 0)
                + COALESCE(t.credit_amount_rub, 0)
            ELSE 0
        END AS balance_out_rub
    FROM ds.md_account_d a
    LEFT JOIN dm.dm_account_balance_f prev
        ON a.account_rk = prev.account_rk
        AND prev.on_date = v_prev_date
    LEFT JOIN dm.dm_account_turnover_f t
        ON a.account_rk = t.account_rk
        AND t.on_date = i_OnDate
    WHERE a.data_actual_date <= i_OnDate
        AND a.data_actual_end_date >= i_OnDate;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    -- Логируем завершение
    v_end_time := NOW();
    UPDATE logs.etl_log
    SET end_time = v_end_time,
        status = 'COMPLETED',
        rows_processed = v_rows_affected
    WHERE table_name = 'dm.dm_account_balance_f'
        AND start_time = v_start_time;

    RAISE NOTICE 'Витрина dm_account_balance_f за % заполнена. Обработано строк: %', i_OnDate, v_rows_affected;

EXCEPTION WHEN OTHERS THEN
    UPDATE logs.etl_log
    SET end_time = NOW(),
        status = 'FAILED',
        error_message = SQLERRM
    WHERE table_name = 'dm.dm_account_balance_f'
        AND start_time = v_start_time;
    RAISE;
END;
$$;