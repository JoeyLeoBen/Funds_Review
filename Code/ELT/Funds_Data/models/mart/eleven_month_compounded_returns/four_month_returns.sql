-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}


WITH x_year_returns AS (
    SELECT 
        fund_category, 
        fund_identifier, 
        returns, 
        fees, 
        net_returns, 
        fund_company_id, 
        date, 
        CAST(date_full AS VARCHAR) AS date_full_text
    FROM {{ ref('returns_with_fees_categories') }}
    WHERE 
        fees IS NOT NULL 
        AND date_full >= (
            SELECT CAST((DATE(MAX(date_full)) - interval '3 months')::DATE AS TEXT)
            FROM {{ ref('returns_with_fees_categories') }}
            WHERE fees IS NOT NULL
        )
    ORDER BY date_full ASC
),

funds_to_exclude AS (
    SELECT DISTINCT fund_identifier
    FROM x_year_returns
    WHERE net_returns > 101
)

SELECT 
    fund_category, 
    fund_identifier, 
    fund_company_id, 
    '4M' AS return_type, 
    (mul(net_returns) - 1) AS net_compounded_returns, 
    (AVG(net_returns - 1)) AS net_returns_avg, 
    (STDDEV(net_returns - 1)) AS net_returns_stddev
FROM x_year_returns
WHERE 
    fees IS NOT NULL 
    AND fund_identifier NOT IN (SELECT fund_identifier FROM funds_to_exclude)
GROUP BY 
    fund_category, 
    fund_identifier, 
    fund_company_id
HAVING 
    (mul(net_returns) - 1) IS NOT NULL 
    AND COUNT(*) >= 4
