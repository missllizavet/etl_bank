-- поиск дубликатов: группируем по ключу и считаем повторы
SELECT
    client_rk,
    effective_from_date,
    COUNT(*) AS количество_дублей
FROM dm.client
GROUP BY client_rk, effective_from_date
HAVING COUNT(*) > 1
ORDER BY client_rk, effective_from_date;

-- просмотр всех строк-дубликатов целиком
SELECT *
FROM dm.client
WHERE (client_rk, effective_from_date) IN (
    SELECT client_rk, effective_from_date
    FROM dm.client
    GROUP BY client_rk, effective_from_date
    HAVING COUNT(*) > 1
)
ORDER BY client_rk, effective_from_date;

-- удаление дубликатов: оставляем запись с самой поздней effective_to_date
DELETE FROM dm.client
WHERE ctid NOT IN (
    SELECT DISTINCT ON (client_rk, effective_from_date) ctid
    FROM dm.client
    ORDER BY client_rk, effective_from_date, effective_to_date DESC
);
-- проверка: после удаления запрос должен вернуть 0 строк
SELECT client_rk, effective_from_date, COUNT(*)
FROM dm.client
GROUP BY client_rk, effective_from_date
HAVING COUNT(*) > 1;

-- добавление первичного ключа для защиты от дублей в будущем
ALTER TABLE dm.client
ADD PRIMARY KEY (client_rk, effective_from_date);

