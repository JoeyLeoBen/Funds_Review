-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}

-- Creates a full table with sub peer group avg rolling returns to union 
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
    rfc.date_full AS date, 
    TRIM(regexp_replace(CONCAT(fb.peer_group,' ',fb.sub_peer_group,' ',rfc.date_full), '\s+', ' ', 'g')) AS peer_group_identifier, 
    fb.sub_peer_group, 
    nrt.peer_12_rolling_net_compounded_returns,
    nrt.peer_36_rolling_net_compounded_returns,
    nrt.peer_60_rolling_net_compounded_returns
FROM {{ ref('returns_with_fees_categories') }} AS rfc LEFT JOIN {{ ref('raw_fund_companies') }} AS fc
ON rfc.fund_company_id = fc.fund_company_id
LEFT JOIN {{ ref('raw_funds') }} AS f
ON rfc.fund_identifier = f.fund_identifier
LEFT JOIN {{ ref('raw_assets') }} AS aa
ON rfc.fund_identifier = aa.fund_identifier
LEFT JOIN {{ ref('all_fund_benchmarks') }} AS fb
ON rfc.fund_identifier = fb.fund_identifier
LEFT JOIN {{ ref('rolling_returns_two') }} AS nrt
ON TRIM(regexp_replace(CONCAT(fb.peer_group,' ',fb.sub_peer_group,' ',rfc.date_full), '\s+', ' ', 'g')) = nrt.peer_group_identifier
WHERE 
    fees IS NOT NULL AND net_returns < 101 AND sub_peer_group IN (
    'Advanced (80% Equity & 20% Fixed Income)','Aggressive (100% Equity)',
    'Balanced (60% Equity & 40% Fixed Income)','Conservative (25% Equity & 75% Fixed Income)',
    'Moderate (40% Equity & 65% Fixed Income)','Retirement Funds','2025 Funds', '2030 Funds', 
    '2035 Funds', '2040 Funds', '2045 Funds', '2050 Funds', '2055 Funds', '2060 Funds'
    )





