import os

import psycopg2 as ps
from dotenv import load_dotenv
from ETL_Queries import *

load_dotenv()
DB_PASSWORD = os.getenv("DB_PASSWORD")

###########################################################################################################################################
# NEW CODE BLOCK - Create funds_review_db
###########################################################################################################################################


def create_database():
    """
    - Creates and connects to the funds_review_db
    - Returns the connection and cursor to funds_review_db
    """

    # connect to default database port: 5432
    conn = ps.connect(
        f"""
    
        host=localhost
        dbname=postgres
        user=postgres
        password={DB_PASSWORD}
           
    """
    )

    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create funds_review_db database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS funds_review_db;")
    cur.execute(
        "CREATE DATABASE funds_review_db WITH ENCODING 'utf8' TEMPLATE template0;"
    )

    # close connection to default database
    conn.close()

    # connect to funds_review_db database
    conn = ps.connect(
        f"""
    
        host=localhost
        dbname=funds_review_db
        user=postgres
        password={DB_PASSWORD}
        
    """
    )

    cur = conn.cursor()

    # Create schema raw
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    # Create schema analytics
    cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
    conn.commit()

    return cur, conn


###########################################################################################################################################
# NEW CODE BLOCK - Create tables in funds_review_db
###########################################################################################################################################


# Drop tables
################################################################
def drop_tables(cur, conn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list

    Args:
        conn (ps.extensions.connection): The database connection.
        cur (ps.extensions.cursor): The database cursor.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    return None


# Create tables
################################################################
def create_tables(cur, conn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list

    Args:
        conn (ps.extensions.connection): The database connection.
        cur (ps.extensions.cursor): The database cursor.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

    return None


###########################################################################################################################################
# NEW CODE BLOCK - Runs etl pipeline
###########################################################################################################################################


def main() -> None:
    """
    - Drops (if exists) and creates the funds_review_db database
    - Establishes connection with the funds_review_db database and gets cursor to it
    - Drops all the tables
    - Closes the connection
    """

    try:
        cur, conn = create_database()

        # Drop tables
        drop_tables(cur=cur, conn=conn)

        # Create tables
        create_tables(cur=cur, conn=conn)

        print("database funds_review_db has been created")
        print("schemas raw and analytics have been created")
        print("tables fund_companies, funds, and returns have been created")

        cur.close()
        conn.close()

    except ps.Error as e:
        print("\n error:")
        print(e)

    return None


if __name__ == "__main__":
    main()
