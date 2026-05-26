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
ORDER BY cb.account_rk, cb.effective_date;