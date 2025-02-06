-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}

-- Gets all rolling returns for all funds
WITH net_returns_one AS (
    SELECT 
        fc.fund_company, 
        f.fund_type, 
        CASE WHEN aa.invested IS NULL THEN 'No' WHEN aa.invested = 'Yes' THEN 'Yes' END AS invested, 
        rfc.fund_category, 
        rfc.fund_identifier, 
        f.funds, 
        aa.market_value, 
        aa.member_count, 
        rfc.returns, 
        rfc.fees, 
        rfc.net_returns, 
        rfc.date_full AS date
    FROM {{ ref('returns_with_fees_categories') }} AS rfc LEFT JOIN {{ ref('raw_fund_companies') }} AS fc
    ON rfc.fund_company_id = fc.fund_company_id
    LEFT JOIN {{ ref('raw_funds') }} AS f
    ON rfc.fund_identifier = f.fund_identifier
    LEFT JOIN {{ ref('raw_assets') }} AS aa
    ON rfc.fund_identifier = aa.fund_identifier
    WHERE fees IS NOT NULL AND net_returns < 101
)

SELECT 
    fund_company, fund_type, invested, fund_category, fund_identifier, funds, market_value, member_count, returns, fees, net_returns, date,
    (mul(net_returns) OVER(ORDER BY fund_company, fund_type, fund_category, fund_identifier, funds, date ASC ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) - 1) AS net_compounded_returns_12_rolling,
    (POWER(mul(net_returns) OVER(ORDER BY fund_company, fund_type, fund_category, fund_identifier, funds, date ASC ROWS BETWEEN 35 PRECEDING AND CURRENT ROW),0.3333333333333333) - 1) AS net_compounded_returns_36_rolling,
    (POWER(mul(net_returns) OVER(ORDER BY fund_company, fund_type, fund_category, fund_identifier, funds, date ASC ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),0.2) - 1) AS net_compounded_returns_60_rolling
FROM net_returns_one
ORDER BY funds ASC, date ASC