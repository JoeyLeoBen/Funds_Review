-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}

-- Gets avg rolling returns for peer abd sub peer groups
WITH avg_compounded_peer_group AS (
	SELECT 
		TRIM(regexp_replace(CONCAT(fb.peer_group,' ',fb.sub_peer_group,' ',nro.date), '\s+', ' ', 'g')) AS peer_group_identifier, 
		AVG(nro.net_compounded_returns_12_rolling) AS peer_12_rolling_net_compounded_returns, 
		AVG(nro.net_compounded_returns_36_rolling) AS peer_36_rolling_net_compounded_returns, 
		AVG(nro.net_compounded_returns_60_rolling) AS peer_60_rolling_net_compounded_returns
	FROM {{ ref('rolling_returns_one') }} AS nro LEFT JOIN {{ ref('all_fund_benchmarks') }} AS fb
	ON nro.fund_identifier = fb.fund_identifier
	GROUP BY peer_group_identifier
	ORDER BY peer_group_identifier ASC
)

SELECT *
FROM avg_Compounded_peer_group