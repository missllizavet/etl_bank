-- Создание витрины 101 формы
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

-- =====================================================
-- Процедура заполнения 101 формы
-- =====================================================
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(
    i_OnDate DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_rows_affected INTEGER;
    v_from_date DATE;
    v_to_date DATE;
    v_prev_date DATE;
    v_log_id INTEGER;
BEGIN
    -- Определяем отчетный период
    -- i_OnDate - первый день следующего месяца
    -- Например, для января 2018: i_OnDate = '2018-02-01'
    v_from_date := DATE_TRUNC('month', i_OnDate) - INTERVAL '1 month';
    v_to_date := i_OnDate - INTERVAL '1 day';
    v_prev_date := v_from_date - INTERVAL '1 day';

    -- Логируем старт
    v_start_time := NOW();
    INSERT INTO logs.etl_log (table_name, start_time, status)
    VALUES ('dm.dm_f101_round_f', v_start_time, 'IN PROGRESS')
    RETURNING log_id INTO v_log_id;

    RAISE NOTICE 'Расчет 101 формы за период с % по % (предыдущий день: %)',
        v_from_date, v_to_date, v_prev_date;

    -- Удаляем записи за этот отчетный период (для возможности перезапуска)
    DELETE FROM dm.dm_f101_round_f
    WHERE from_date = v_from_date
        AND to_date = v_to_date;

    -- Вставляем расчетные данные
    INSERT INTO dm.dm_f101_round_f (
        from_date,
        to_date,
        chapter,
        ledger_account,
        characteristic,
        balance_in_rub,
        balance_in_val,
        balance_in_total,
        turn_deb_rub,
        turn_deb_val,
        turn_deb_total,
        turn_cre_rub,
        turn_cre_val,
        turn_cre_total,
        balance_out_rub,
        balance_out_val,
        balance_out_total
    )
    WITH
    -- Подготавливаем данные по счетам с их характеристиками
    accounts AS (
        SELECT DISTINCT
            a.account_rk,
            LEFT(a.account_number, 5) AS ledger_account,
            a.char_type AS characteristic,
            a.currency_code,
            -- Определяем, является ли счет рублевым
            CASE
                WHEN a.currency_code IN ('810', '643') THEN 1
                ELSE 0
            END AS is_rub,
            -- Получаем главу из справочника
            las.chapter
        FROM ds.md_account_d a
        LEFT JOIN ds.md_ledger_account_s las
            ON LEFT(a.account_number, 5)::INTEGER = las.ledger_account
        WHERE a.data_actual_date <= v_to_date
            AND a.data_actual_end_date >= v_from_date
    ),
    -- Остатки на начало периода (предыдущий день)
    balance_start AS (
        SELECT
            b.account_rk,
            b.balance_out_rub
        FROM dm.dm_account_balance_f b
        WHERE b.on_date = v_prev_date
    ),
    -- Остатки на конец периода
    balance_end AS (
        SELECT
            b.account_rk,
            b.balance_out_rub
        FROM dm.dm_account_balance_f b
        WHERE b.on_date = v_to_date
    ),
    -- Обороты за период
    turnovers AS (
        SELECT
            t.account_rk,
            SUM(t.debet_amount_rub) AS total_deb_rub,
            SUM(t.credit_amount_rub) AS total_cre_rub
        FROM dm.dm_account_turnover_f t
        WHERE t.on_date BETWEEN v_from_date AND v_to_date
        GROUP BY t.account_rk
    ),
    -- Объединяем все данные
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
    SELECT
        v_from_date AS from_date,
        v_to_date AS to_date,
        chapter,
        ledger_account,
        characteristic,
        -- Входящие остатки в рублях (только рублевые счета)
        SUM(CASE WHEN is_rub = 1 THEN balance_start_rub ELSE 0 END) AS balance_in_rub,
        -- Входящие остатки в валюте (не рублевые счета)
        SUM(CASE WHEN is_rub = 0 THEN balance_start_rub ELSE 0 END) AS balance_in_val,
        -- Входящие остатки всего
        SUM(balance_start_rub) AS balance_in_total,
        -- Дебетовые обороты в рублях (рублевые счета)
        SUM(CASE WHEN is_rub = 1 THEN deb_turn_rub ELSE 0 END) AS turn_deb_rub,
        -- Дебетовые обороты в валюте (не рублевые)
        SUM(CASE WHEN is_rub = 0 THEN deb_turn_rub ELSE 0 END) AS turn_deb_val,
        -- Дебетовые обороты всего
        SUM(deb_turn_rub) AS turn_deb_total,
        -- Кредитовые обороты в рублях (рублевые счета)
        SUM(CASE WHEN is_rub = 1 THEN cre_turn_rub ELSE 0 END) AS turn_cre_rub,
        -- Кредитовые обороты в валюте (не рублевые)
        SUM(CASE WHEN is_rub = 0 THEN cre_turn_rub ELSE 0 END) AS turn_cre_val,
        -- Кредитовые обороты всего
        SUM(cre_turn_rub) AS turn_cre_total,
        -- Исходящие остатки в рублях (рублевые счета)
        SUM(CASE WHEN is_rub = 1 THEN balance_end_rub ELSE 0 END) AS balance_out_rub,
        -- Исходящие остатки в валюте (не рублевые)
        SUM(CASE WHEN is_rub = 0 THEN balance_end_rub ELSE 0 END) AS balance_out_val,
        -- Исходящие остатки всего
        SUM(balance_end_rub) AS balance_out_total
    FROM combined
    GROUP BY chapter, ledger_account, characteristic;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    -- Обновляем R-поля (пока что дублируем основные значения)
    -- В реальной системе здесь могут быть другие расчеты
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
    WHERE from_date = v_from_date
        AND to_date = v_to_date;

    -- Логируем завершение
    v_end_time := NOW();
    UPDATE logs.etl_log
    SET end_time = v_end_time,
        status = 'COMPLETED',
        rows_processed = v_rows_affected
    WHERE log_id = v_log_id;

    RAISE NOTICE 'Витрина dm_f101_round_f за период с % по % заполнена. Обработано строк: %',
        v_from_date, v_to_date, v_rows_affected;

EXCEPTION WHEN OTHERS THEN
    -- Логируем ошибку
    UPDATE logs.etl_log
    SET end_time = NOW(), 
        status = 'FAILED',
        error_message = SQLERRM
    WHERE log_id = v_log_id;
    RAISE;
END;
$$;