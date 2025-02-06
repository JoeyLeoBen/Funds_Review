-- Create multiplication aggregate function if not already exists
-- Uncomment and execute the following lines if the 'mul' aggregate does not exist in your database.
-- CREATE AGGREGATE mul(double precision) (
--     SFUNC = float8mul,
--     STYPE = double precision
-- );
-- DROP AGGREGATE mul(double precision);

-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}


WITH combined_returns AS (
    SELECT * FROM {{ ref('one_month_returns') }}
    UNION
    SELECT * FROM {{ ref('two_month_returns') }}
    UNION
    SELECT * FROM {{ ref('three_month_returns') }}
    UNION
    SELECT * FROM {{ ref('four_month_returns') }}
    UNION
    SELECT * FROM {{ ref('five_month_returns') }}
    UNION
    SELECT * FROM {{ ref('six_month_returns') }}
    UNION
    SELECT * FROM {{ ref('seven_month_returns') }}
    UNION
    SELECT * FROM {{ ref('eight_month_returns') }}
    UNION
    SELECT * FROM {{ ref('nine_month_returns') }}
    UNION
    SELECT * FROM {{ ref('ten_month_returns') }}
    UNION
    SELECT * FROM {{ ref('eleven_month_returns') }}
    UNION
    SELECT * FROM {{ ref('one_year_returns') }}
    UNION
    SELECT * FROM {{ ref('two_year_returns') }}
    UNION
    SELECT * FROM {{ ref('three_year_returns') }}
    UNION
    SELECT * FROM {{ ref('four_year_returns') }}
    UNION
    SELECT * FROM {{ ref('five_year_returns') }}
    UNION
    SELECT * FROM {{ ref('six_year_returns') }}
    UNION
    SELECT * FROM {{ ref('seven_year_returns') }}
    UNION
    SELECT * FROM {{ ref('eight_year_returns') }}
    UNION
    SELECT * FROM {{ ref('nine_year_returns') }}
    UNION
    SELECT * FROM {{ ref('ten_year_returns') }}
    UNION
    SELECT * FROM {{ ref('eleven_year_returns') }}
    UNION
    SELECT * FROM {{ ref('twelve_year_returns') }}
    UNION
    SELECT * FROM {{ ref('thirteen_year_returns') }}
    UNION
    SELECT * FROM {{ ref('fourteen_year_returns') }}
    UNION
    SELECT * FROM {{ ref('fifteen_year_returns') }}
    UNION
    SELECT * FROM {{ ref('sixteen_year_returns') }}
    UNION
    SELECT * FROM {{ ref('seventeen_year_returns') }}
    UNION
    SELECT * FROM {{ ref('eighteen_year_returns') }}
    UNION
    SELECT * FROM {{ ref('nineteen_year_returns') }}
    UNION
    SELECT * FROM {{ ref('twenty_year_returns') }}
    UNION
    SELECT * FROM {{ ref('hybrid_returns') }}
),

joined_returns AS (
    SELECT 
        fc.fund_company, 
        r.fund_type, 
        CASE 
            WHEN aa.invested IS NULL THEN 'No' 
            WHEN aa.invested = 'Yes' THEN 'Yes' 
        END AS invested, 
        v.fund_category, 
        v.fund_identifier, 
        r.funds, 
        aa.market_value, 
        aa.member_count, 
        v.return_type, 
        v.net_compounded_returns, 
        v.net_returns_avg, 
        v.net_returns_stddev
    FROM combined_returns AS v
    LEFT JOIN {{ ref('raw_fund_companies') }} AS fc
        ON v.fund_company_id = fc.fund_company_id
    LEFT JOIN {{ ref('raw_funds') }} AS r
        ON v.fund_identifier = r.fund_identifier
    LEFT JOIN {{ ref('raw_assets') }} AS aa
        ON v.fund_identifier = aa.fund_identifier
)

-- Grouped for peers
SELECT 
    TRIM(CONCAT(xr.return_type, ' ', pg.peer_group, ' ', pg.sub_peer_group)) AS peer_group_identifier, 
    AVG(xr.net_compounded_returns) AS peer_net_compounded_returns
FROM joined_returns AS xr
LEFT JOIN {{ ref('all_fund_benchmarks') }} AS pg
    ON xr.fund_identifier = pg.fund_identifier
GROUP BY peer_group_identifier
