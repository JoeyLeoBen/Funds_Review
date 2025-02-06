import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
import warnings

import numpy as np

warnings.filterwarnings("ignore")

from typing import List, Optional

from ETL_Extract_Load_Base import ExtractLoad


############################################################################################################################
# Preprocess data
############################################################################################################################
# Class for processing and preparing data for loading into the database
class ProcessLoadData(ExtractLoad):
    # Process full data
    ################################################################
    def process_data(self, date: str) -> pd.DataFrame:
        """
        Retrieves the cleaned returns data and further manipulates it to be stored in the funds_review_db.

        Args:
            date (str): The date associated with the returns data.

        Returns:
            pd.DataFrame: Processed returns data ready for loading.
        """
        # Fund returns data file paths
        files = self._get_files(suffix="_Returns")

        # Concatenate all returns data into a single DataFrame
        df = pd.concat(map(pd.read_csv, files), ignore_index=True, axis=0)

        # Remove extra commas, breaks, and white space to allow for CSV delimiter for contact data
        df["funds"] = df["funds"].replace(",", "", regex=True)
        df["date"] = date

        print(f"To store fund returns data to funds_review_db: {files}")

        return df

    # Process fund company data
    ################################################################
    def process_fund_company_data(self, full_df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates the fund company table.

        Args:
            full_df (pd.DataFrame): The full dataset containing fund information.

        Returns:
            pd.DataFrame: DataFrame containing unique fund companies.
        """
        id = 0
        fund_company_dict_list = []
        fund_company_list = [
            "Canada Life",
            "Manulife",
            "Sun Life",
            "Industrial Alliance",
        ]

        # Create a list of dictionaries for each fund company with unique IDs
        for company in fund_company_list:
            company_dict = {"fund_company_id": id, "fund_company": company}

            fund_company_dict_list.append(company_dict)
            id += 1

        fund_company_df = pd.DataFrame(fund_company_dict_list)

        return fund_company_df

    # Process fund data
    ################################################################
    def process_fund_data(self, full_df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates the fund table.

        Args:
            full_df (pd.DataFrame): The full dataset containing fund information.

        Returns:
            pd.DataFrame: DataFrame containing unique funds.
        """
        fund_df = full_df[["fund_identifier", "funds", "fund_type"]]
        fund_df = fund_df.drop_duplicates(
            subset=["fund_identifier", "funds", "fund_type"]
        )

        return fund_df

    # Process fund returns data
    ################################################################
    def process_returns_data(self, full_df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates the returns table.

        Args:
            full_df (pd.DataFrame): The full dataset containing returns information.

        Returns:
            pd.DataFrame: DataFrame containing processed returns data with fund company IDs.
        """
        returns_df = full_df[["fund_identifier", "returns", "fund_company", "date"]]
        fund_company_list = [
            "Canada Life",
            "Manulife",
            "Sun Life",
            "Industrial Alliance",
        ]

        id = 0
        # Replace fund company names with corresponding IDs
        for company in fund_company_list:
            returns_df["fund_company"] = returns_df["fund_company"].replace(company, id)
            id += 1

        returns_df = returns_df.rename(columns={"fund_company": "fund_company_id"})

        return returns_df
