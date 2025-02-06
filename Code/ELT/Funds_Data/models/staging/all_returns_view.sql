-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}


-- 1. Combine returns data from multiple timeframes
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

-- 2. Join returns data to additional tables (fund companies, funds, assets, fund benchmarks)
joined_returns AS (
    SELECT
        fc.fund_company,
        r.fund_type,
        CASE 
            WHEN aa.invested IS NULL THEN 'No' 
            WHEN aa.invested = 'Yes' THEN 'Yes' 
        END AS invested,
        cr.fund_category,
        cr.fund_identifier,
        r.funds,
        aa.market_value,
        aa.member_count,
        cr.return_type,
        cr.net_compounded_returns,
        cr.net_returns_avg,
        cr.net_returns_stddev,
        TRIM(regexp_replace(CONCAT(cr.fund_category, ' ', ab.sub_peer_group), '\s+', ' ', 'g')) AS peer_groups,
        TRIM(regexp_replace(CONCAT(cr.return_type, ' ', cr.fund_category, ' ', ab.sub_peer_group), '\s+', ' ', 'g')) AS peer_group_identifier
    FROM combined_returns AS cr
    LEFT JOIN {{ ref('raw_fund_companies') }} AS fc
        ON cr.fund_company_id = fc.fund_company_id
    LEFT JOIN {{ ref('raw_funds') }} AS r
        ON cr.fund_identifier = r.fund_identifier
    LEFT JOIN {{ ref('raw_assets') }} AS aa
        ON cr.fund_identifier = aa.fund_identifier
    LEFT JOIN {{ ref('all_fund_benchmarks') }} AS ab
        ON cr.fund_identifier = ab.fund_identifier
)

-- 3. Final output: Join with peer group average returns
SELECT
    jr.*,
    pg.peer_net_compounded_returns
FROM joined_returns AS jr
LEFT JOIN {{ ref('all_returns_peer_group') }} AS pg
    ON jr.peer_group_identifier = pg.peer_group_identifier
