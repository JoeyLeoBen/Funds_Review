import pandas as pd
import psycopg2 as ps

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
import warnings

import numpy as np

warnings.filterwarnings("ignore")

import os

# File management
import sys

# For bulk data import
from io import StringIO

# Get the current working directory (the directory of the running script)
current_dir = os.getcwd()
# Add the target directory to the system path
sys.path.append(os.path.abspath(os.path.join(current_dir, "Load/SQL_Queries")))
from typing import Any, Tuple

from ETL_Extract_Load_Base import ExtractLoad
from SQL_Queries import *


############################################################################################################################
# NEW BLOCK - Load Data
############################################################################################################################
# Class for handling data loading into the database
class LoadData(ExtractLoad):
    # Load fund company data
    ################################################################
    def load_fund_company_data(
        self,
        fund_company_df: pd.DataFrame,
        export_path: str,
        conn: ps.extensions.connection,
        cur: ps.extensions.cursor,
    ) -> None:
        """
        Loads fund company data into the database.

        Steps:
        - Backs up existing data from `raw.fund_companies` table.
        - Saves the backup as a CSV file.
        - Inserts new fund company data into the `fund_companies` table.

        Args:
            fund_company_df (pd.DataFrame): DataFrame containing fund company data to be inserted.
            export_path (str): Path where the backup CSV will be saved.
            conn (ps.extensions.connection): Database connection object.
            cur (ps.extensions.cursor): Database cursor object.
        """
        # Backup existing fund company data
        query = """SELECT *
                 FROM raw.fund_companies;"""
        cur.execute(query)
        old_fund_company_df = cur.fetchall()

        old_fund_company_df_columns = ["fund_company_id", "fund_company"]
        old_fund_company_df = pd.DataFrame(
            old_fund_company_df, columns=old_fund_company_df_columns
        )

        # Save backup as CSV
        self._save_csv(
            df=old_fund_company_df,
            label="Old DB Fund Company Data",
            status="Saved",
            export_path=export_path,
        )

        # Insert new fund company data line by line
        try:
            count = 0
            for index, row in fund_company_df.iterrows():
                cur.execute(fund_companies_table_insert, list(row))
                conn.commit()

                count += 1
                print(
                    f"Fund company data inserted line-by-line into funds_review_db successfully {count}"
                )

        except ps.Error as e:
            print("Insert fund company data error:")
            print(e)

        print(f"Columns inserted: {fund_company_df.shape[1]}")

        return None

    # Load fund data
    ################################################################
    def load_fund_data(
        self,
        fund_df: pd.DataFrame,
        export_path: str,
        conn: ps.extensions.connection,
        cur: ps.extensions.cursor,
    ) -> None:
        """
        Loads fund data into the database.

        Steps:
        - Backs up existing data from `raw.funds` table.
        - Saves the backup as a CSV file.
        - Inserts new fund data into the `funds` table.

        Args:
            fund_df (pd.DataFrame): DataFrame containing fund data to be inserted.
            export_path (str): Path where the backup CSV will be saved.
            conn (ps.extensions.connection): Database connection object.
            cur (ps.extensions.cursor): Database cursor object.
        """
        # Backup existing fund data
        query = """SELECT *
                 FROM raw.funds;"""
        cur.execute(query)
        old_fund_df = cur.fetchall()

        old_fund_df_columns = ["fund_identifier", "funds", "fund_type"]
        old_fund_df = pd.DataFrame(old_fund_df, columns=old_fund_df_columns)

        # Save backup as CSV
        self._save_csv(
            df=old_fund_df,
            label="Old DB Funds Data",
            status="Saved",
            export_path=export_path,
        )

        # Insert new fund data line by line
        try:
            count = 0
            for index, row in fund_df.iterrows():
                cur.execute(funds_table_insert, list(row))
                conn.commit()

                count += 1
                print(
                    f"Fund data inserted line-by-line into funds_review_db successfully {count}"
                )

        except ps.Error as e:
            print("Insert fund data error:")
            print(e)

        print(f"Columns inserted: {fund_df.shape[1]}")

        return None

    # Load returns data
    ################################################################
    def load_returns_data(
        self,
        returns_df: pd.DataFrame,
        export_path: str,
        conn: ps.extensions.connection,
        cur: ps.extensions.cursor,
    ) -> None:
        """
        Loads returns data into the database using bulk import.

        Steps:
        - Backs up existing data from `raw.returns` table.
        - Saves the backup as a CSV file.
        - Concatenates old and new returns data, removes duplicates.
        - Drops existing data from the `returns` table.
        - Inserts concatenated data into the `returns` table using bulk import.

        Args:
            returns_df (pd.DataFrame): DataFrame containing returns data to be inserted.
            export_path (str): Path where the backup CSV will be saved.
            conn (ps.extensions.connection): Database connection object.
            cur (ps.extensions.cursor): Database cursor object.
        """
        # Stream cleaned data in bulk to database
        try:
            # Backup existing returns data
            query = """SELECT *
                     FROM raw.returns;"""
            cur.execute(query)
            old_returns_df = cur.fetchall()

            old_returns_df_columns = [
                "returns_id",
                "fund_identifier",
                "returns",
                "fund_company_id",
                "date",
            ]
            old_returns_df = pd.DataFrame(
                old_returns_df, columns=old_returns_df_columns
            )

            # Save backup as CSV
            self._save_csv(
                df=old_returns_df,
                label="Old DB Returns Data",
                status="Saved",
                export_path=export_path,
            )

            # Concatenate old and new returns data
            old_returns_df = old_returns_df.iloc[:, 1:]
            returns_df = pd.concat([returns_df, old_returns_df], axis=0)

            # Remove duplicates based on unique combination
            returns_df["duplicate"] = (
                returns_df["fund_identifier"].astype(str)
                + returns_df["fund_company_id"].astype(str)
                + returns_df["date"].astype(str)
            )
            returns_df = returns_df.drop_duplicates(
                subset=["duplicate"], keep="first"
            ).drop("duplicate", axis=1, errors="ignore")

            # Delete old data from the database
            cur.execute(returns_table_drop_rows)
            conn.commit()

            # Set schema
            cur.execute("SET search_path TO raw")

            # Prepare data for bulk insert
            sio = StringIO()
            sio.write(returns_df.to_csv(index=None, header=None))
            sio.seek(0)

            # Bulk insert using copy_from
            cur.copy_from(
                file=sio, table="returns", columns=returns_df.columns, sep=","
            )
            conn.commit()

            print("Returns data inserted in bulk to funds_review_db successfully")
            print(f"Rows inserted: {returns_df.shape[0]}")

        except ps.Error as e:
            print("Insert returns data error:")
            print(e)

        return None
