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
ORDER BY cb.account_rk, cb.effective_date;