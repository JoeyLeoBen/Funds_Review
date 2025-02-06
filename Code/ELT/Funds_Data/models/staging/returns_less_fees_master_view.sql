
-- Returns less fees
SELECT rf.fund_company, 
    r.fund_identifier, 
    CONCAT(r.fund_identifier,' - ',rfunds.funds) AS funds, 
    r.date, r.returns, 
    rf.fees, 
    (((r.returns / 100) + 1) - (0.0833333333333333 * rf.fees)) AS returns_less_fees, 
    CONCAT(rfunds.funds, ' - ', rf.fund_company) AS master_funds
FROM {{ ref('raw_returns') }} AS r LEFT JOIN {{ ref('raw_fees') }} AS rf
ON r.fund_identifier = rf.fund_identifier
LEFT JOIN analytics.raw_funds AS rfunds
ON r.fund_identifier = rfunds.fund_identifier
WHERE rf.fees IS NOT NULL AND fund_company IN (
 	SELECT DISTINCT(fund_company)
	FROM {{ ref('raw_assets') }}
) AND rfunds.funds IS NOT NULL