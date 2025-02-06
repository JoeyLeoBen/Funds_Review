import glob

import pandas as pd

###########################################################################################################################################
# DROP TABLES
###########################################################################################################################################

fund_companies_table_drop = "DROP TABLE IF EXISTS raw.fund_companies;"
funds_table_drop = "DROP TABLE IF EXISTS raw.funds;"
returns_table_drop = "DROP TABLE IF EXISTS raw.returns;"


###########################################################################################################################################
# DROP RPOWS
###########################################################################################################################################

returns_table_drop_rows = "DELETE FROM raw.returns;"


###########################################################################################################################################
# CREATE TABLES
###########################################################################################################################################

# DIMENSION TABLES
################################################################
fund_companies_table_create = """

    CREATE TABLE IF NOT EXISTS raw.fund_companies(
        fund_company_id int NOT NULL PRIMARY KEY,
        fund_company varchar NOT NULL
    );

"""

funds_table_create = """

    CREATE TABLE IF NOT EXISTS raw.funds(
        fund_identifier varchar NOT NULL PRIMARY KEY,
        funds varchar NOT NULL,
        fund_type varchar NOT NULL
    );

"""

# FACTS TABLE
################################################################
returns_table_create = """

    CREATE TABLE IF NOT EXISTS raw.returns(
        returns_id SERIAL NOT NULL PRIMARY KEY,
        fund_identifier varchar NOT NULL,
        returns float NOT NULL,
        fund_company_id int NOT NULL,
        date varchar NOT NULL
        
    );

"""


###########################################################################################################################################
# INSERT RECORDS
###########################################################################################################################################

fund_companies_col_num = 2
fund_companies_table_variables = "%s" + (",%s" * (fund_companies_col_num - 1))
fund_companies_table_insert = (
    """

    INSERT INTO raw.fund_companies(
         fund_company_id,
         fund_company
    )

    VALUES ("""
    + fund_companies_table_variables
    + """)
    ON CONFLICT (fund_company_id)
        DO UPDATE
            SET
                fund_company = EXCLUDED.fund_company;

"""
)

funds_col_num = 3
funds_table_variables = "%s" + (",%s" * (funds_col_num - 1))
funds_table_insert = (
    """

    INSERT INTO raw.funds(
         fund_identifier,
         funds,
         fund_type
    )

    VALUES ("""
    + funds_table_variables
    + """)
    ON CONFLICT (fund_identifier)
        DO UPDATE
            SET
                funds = EXCLUDED.funds,
                fund_type = EXCLUDED.fund_type;

"""
)

# Delete return records before replacing example to avoid duplicate records because of bulk insertion
# DELETE FROM raw.returns
# WHERE fund_company_id = 0 and date = '2023-08'


###########################################################################################################################################
# QUERY LISTS
###########################################################################################################################################

create_table_queries = [
    fund_companies_table_create,
    funds_table_create,
    returns_table_create,
]

drop_table_queries = [fund_companies_table_drop, funds_table_drop, returns_table_drop]
