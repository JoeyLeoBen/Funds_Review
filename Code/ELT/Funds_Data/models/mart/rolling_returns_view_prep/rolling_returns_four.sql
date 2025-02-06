-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}

-- Cleans up full table with sub peer group avg rolling returns (rolling_returns_three) to union
SELECT DISTINCT
    fund_company, 
    fund_type, 
    invested, 
    fund_category,
    NULL AS fund_identifier,
    sub_peer_group AS funds,
    NULL AS market_value, 
    NULL AS member_count, 
    NULL AS returns, 
    NULL AS fees, 
    date,
    peer_12_rolling_net_compounded_returns,
    peer_36_rolling_net_compounded_returns,
    peer_60_rolling_net_compounded_returns
FROM {{ ref('rolling_returns_three') }}