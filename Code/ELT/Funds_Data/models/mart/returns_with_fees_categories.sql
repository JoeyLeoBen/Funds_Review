-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    fc.fund_category, 
    r.fund_identifier, 
    ((r.returns/100) + 1) AS returns, 
    f.fees, (((r.returns/100) + 1) - (0.0833333333333333*f.fees)) AS net_returns, 
    r.fund_company_id, date, date || '-01' AS date_full
FROM {{ ref('raw_returns') }} AS r LEFT JOIN {{ ref('raw_fees') }} AS f
ON f.fund_identifier = r.fund_identifier
LEFT JOIN {{ ref('all_fund_categories') }} AS fc
ON r.fund_identifier = fc.fund_identifier