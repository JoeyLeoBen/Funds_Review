
-- Complete view with fund rooling net compounded returns minus benchmarks
WITH rolling_view AS (
	SELECT 
		rro.fund_company, 
		rro.fund_type, 
		CASE WHEN rro.invested IS NULL THEN 'No' WHEN rro.invested = 'Yes' THEN 'Yes' END AS invested, 
		rro.fund_category, 
		fb.sub_peer_group,
		rro.fund_identifier, 
		rro.funds, 
		rro.market_value, 
		rro.member_count, 
		rro.fees, 
		rro.date,
		net_compounded_returns_12_rolling,
		net_compounded_returns_36_rolling,
		net_compounded_returns_60_rolling
	FROM {{ ref('rolling_returns_one') }} AS rro LEFT JOIN {{ ref('all_fund_benchmarks') }} AS fb
	ON rro.fund_identifier = fb.fund_identifier
	UNION
	SELECT
		fund_company, 
		fund_type, 
		invested, 
		fund_category, 
		funds AS sub_peer_group,
		fund_identifier,
		funds, 
		CAST(market_value AS DOUBLE PRECISION) AS market_value, 
		CAST(member_count AS DOUBLE PRECISION) AS member_count, 
		CAST(fees AS DOUBLE PRECISION) AS fees,
		date,
		peer_12_rolling_net_compounded_returns AS net_compounded_returns_12_rolling,
		peer_36_rolling_net_compounded_returns AS net_compounded_returns_36_rolling,
		peer_60_rolling_net_compounded_returns AS net_compounded_returns_60_rolling
	FROM {{ ref('rolling_returns_four') }}
)

SELECT rv.fund_company,
	rv.fund_type,
	rv.invested,
	rv.fund_category,
	rv.sub_peer_group,
	rv.fund_identifier,
	rv.funds,
	rv.market_value,
	rv.member_count,
	rv.fees,
	rv.date,
	-- rrt.peer_12_rolling_net_compounded_returns,
	-- rrt.peer_36_rolling_net_compounded_returns,
	-- rrt.peer_60_rolling_net_compounded_returns,
	rv.net_compounded_returns_12_rolling - rrt.peer_12_rolling_net_compounded_returns AS net_net_compounded_returns_12_rolling,
	rv.net_compounded_returns_36_rolling - rrt.peer_36_rolling_net_compounded_returns AS net_net_compounded_returns_36_rolling,
	rv.net_compounded_returns_60_rolling - rrt.peer_60_rolling_net_compounded_returns AS net_net_compounded_returns_60_rolling
FROM rolling_view AS rv LEFT JOIN {{ ref('rolling_returns_two') }} AS rrt
ON TRIM(regexp_replace(CONCAT(rv.fund_category,' ',rv.sub_peer_group,' ',rv.date), '\s+', ' ', 'g')) = rrt.peer_group_identifier




