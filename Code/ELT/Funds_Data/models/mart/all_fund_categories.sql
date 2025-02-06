-- Config to set as table and not view
{{
    config(
        materialized = 'table'
    )
}}

SELECT * 
FROM {{ ref('raw_canada_life_fund_categories') }}
UNION
SELECT * 
FROM {{ ref('raw_manulife_fund_categories') }}
UNION
SELECT * 
FROM {{ ref('raw_sun_life_fund_categories') }} 
UNION
SELECT * 
FROM {{ ref('raw_industrial_alliance_fund_categories') }}