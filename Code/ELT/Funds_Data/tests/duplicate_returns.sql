
SELECT returns_id || date AS id, COUNT(returns_id || date) AS count
FROM {{ ref('raw_returns') }}
GROUP BY returns_id || date
HAVING COUNT(returns_id || date) > 1

