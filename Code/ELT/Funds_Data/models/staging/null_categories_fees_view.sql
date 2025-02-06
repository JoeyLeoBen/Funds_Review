
-- 1. Gather data from returns_with_fees_categories, raw_fund_companies, raw_funds, raw_assets
WITH with_null_net_returns_view AS (
    SELECT 
        fc.fund_company, 
        f.fund_type, 
        aa.invested, 
        rfc.fund_category, 
        rfc.fund_identifier, 
        f.funds, 
        aa.market_value, 
        aa.member_count, 
        rfc.returns, 
        rfc.fees, 
        (rfc.net_returns - 1) AS net_returns, 
        rfc.date_full AS date
    FROM {{ ref('returns_with_fees_categories') }} AS rfc
    LEFT JOIN {{ ref('raw_fund_companies') }} AS fc
        ON rfc.fund_company_id = fc.fund_company_id
    LEFT JOIN {{ ref('raw_funds') }} AS f
        ON rfc.fund_identifier = f.fund_identifier
    LEFT JOIN {{ ref('raw_assets') }} AS aa
        ON rfc.fund_identifier = aa.fund_identifier
),

-- 2. Identify rows with null categories OR null fees, and aggregate fees
null_cat_view AS (
    SELECT 
        fund_company,
        fund_type,
        invested,
        fund_category,
        fund_identifier,
        funds,
        SUM(fees) AS x
    FROM with_null_net_returns_view
    WHERE fund_category IS NULL 
       OR fees IS NULL
    GROUP BY 
        fund_company,
        fund_type,
        invested,
        fund_category,
        fund_identifier,
        funds
)

-- 3. Final SELECT: Join the aggregated rows with raw_fees
SELECT 
    nc.fund_company,
    nc.fund_type,
    nc.invested,
    nc.fund_category AS "Fund Category",
    nc.fund_identifier,
    nc.funds,
    f.fees AS "Fees"
FROM null_cat_view AS nc
LEFT JOIN {{ ref('raw_fees') }} AS f
    ON nc.fund_identifier = f.fund_identifier
