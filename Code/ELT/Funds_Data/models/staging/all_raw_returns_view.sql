
-- Raw returns
SELECT 
	fc.fund_company, 
	r.fund_identifier, 
	f.funds, r.date, r.returns  
FROM {{ ref('raw_returns') }} AS r LEFT JOIN {{ ref('raw_fund_companies') }} AS fc
ON r.fund_company_id = fc.fund_company_id
LEFT JOIN {{ ref('raw_funds') }} AS f
ON r.fund_identifier = f.fund_identifier
WHERE fund_company IN (
 	SELECT DISTINCT(fund_company)
	FROM {{ ref('raw_assets') }}
)