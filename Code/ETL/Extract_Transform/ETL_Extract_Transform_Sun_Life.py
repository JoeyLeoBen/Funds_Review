import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
import warnings

import numpy as np
from ETL_Extract_Transform_Base import ExtractTransform

warnings.filterwarnings("ignore")

from typing import List


############################################################################################################################
# NEW BLOCK - Sun Life Data
############################################################################################################################
# Class for handling Sun Life data extraction and transformation
class SunLife(ExtractTransform):
    # Returns Data
    ################################################################
    def extract_transform_returns_data(self, export_path: str) -> None:
        """
        Extracts, transforms, and saves Sun Life Returns data.

        Args:
            export_path (str): Path where the transformed CSV will be saved.
        """
        try:
            # Retrieve Returns Core files
            files = self._get_files(prefix="SL_", suffix="_Returns_Core")

            # Process Returns - Core Main
            ################################################################
            df_core = pd.read_excel(files[0])

            # Drop writing at the bottom of table
            df_core.drop(df_core.tail(3).index, inplace=True)

            df_core = df_core.iloc[:, :4].dropna()
            new_header = df_core.iloc[0]
            df_core = df_core[1:]
            df_core.columns = new_header
            df_core = df_core.reset_index(drop=True)
            df_core.columns = ["fund_identifier", "funds", "returns"]
            df_core["fund_type"] = "Available"
            df_core = df_core[["fund_identifier", "funds", "fund_type", "returns"]]

            # Retrieve Returns - Other files
            ################################################################
            files = self._get_files(prefix="SL_", suffix="_Returns_Others")

            df_other = pd.read_excel(files[0])

            # Drop writing at the bottom of table
            df_other.drop(df_other.tail(3).index, inplace=True)

            df_other = df_other.iloc[:, :4].dropna()
            new_header = df_other.iloc[0]
            df_other = df_other[1:]
            df_other.columns = new_header
            df_other = df_other.reset_index(drop=True)
            df_other.columns = ["fund_identifier", "funds", "returns"]
            df_other["fund_type"] = "Not Available"
            df_other = df_other[["fund_identifier", "funds", "fund_type", "returns"]]

            # Combine Core and Other Returns
            ################################################################
            df = pd.concat([df_core, df_other], axis=0).reset_index(drop=True)
            df = df[["fund_identifier", "funds", "returns", "fund_type"]]
            df = df.drop_duplicates(
                subset=["fund_identifier", "funds", "returns", "fund_type"]
            )
            df["fund_company"] = "Sun Life"

            # Sort and remove duplicates
            df = df.sort_values(by=["fund_identifier", "fund_type"], ascending=True)
            df = df.drop_duplicates(subset=["fund_identifier"], keep="first")

            # ***************************************************************#

            # Returns - Core Benchmark
            ################################################################
            files = self._get_files(prefix="SL_", suffix="_Returns_Core")

            df_bm_core = pd.read_excel(files[0])

            df_bm_core = df_bm_core.iloc[1:, :4]
            df_bm_core = df_bm_core.iloc[::3, :].dropna()

            df_bm_core = df_bm_core.reset_index(drop=True)
            df_bm_core.columns = ["fund_identifier", "funds", "returns"]
            df_bm_core["fund_type_bm"] = "Benchmark"
            df_bm_core = df_bm_core[["fund_identifier", "fund_type_bm"]]

            # Returns - Other Benchmark
            ################################################################
            files = self._get_files(prefix="SL_", suffix="_Returns_Others")

            df_bm_other = pd.read_excel(files[0])

            df_bm_other = df_bm_other.iloc[1:, :4]
            df_bm_other = df_bm_other.iloc[::3, :].dropna()

            df_bm_other = df_bm_other.reset_index(drop=True)
            df_bm_other.columns = ["fund_identifier", "funds", "returns"]
            df_bm_other["fund_type_bm"] = "Benchmark"
            df_bm_other = df_bm_other[["fund_identifier", "fund_type_bm"]]

            # Combine Core and Other Benchmarks
            ################################################################
            df_bm = pd.concat([df_bm_core, df_bm_other], axis=0).reset_index(drop=True)
            df_bm = df_bm.drop_duplicates()

            # ***************************************************************#

            # Merge Returns with Benchmarks
            ################################################################
            df = df.merge(df_bm, on="fund_identifier", how="left")

            df["fund_type_bm"].fillna(df["fund_type"], inplace=True)
            df = df.drop("fund_type", axis=1)
            df.columns = [
                "fund_identifier",
                "funds",
                "returns",
                "fund_company",
                "fund_type",
            ]
            df = df[
                ["fund_identifier", "funds", "returns", "fund_type", "fund_company"]
            ]
            df = df.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)

            # Log status and save CSV
            self._log_status("Sun Life Returns", "cleaned", df.isna().sum().sum())
            self._save_csv(df, "Sun Life Returns", "Saved", export_path)

        except Exception as e:
            self._log_status("Sun Life Returns", "Failed", str(e))

        return None

    # Assets Data
    ################################################################
    def extract_transform_assets_data(self, export_path: str) -> None:
        """
        Extracts, transforms, and saves Sun Life Assets data.

        Args:
            export_path (str): Path where the transformed CSV will be saved.
        """
        try:
            # Retrieve Assets files
            files = self._get_files(prefix="SL_", suffix="_Assets")

            df = pd.read_excel(files[0])
            df = df.iloc[:, [4, 5]].dropna()
            new_header = df.iloc[0]
            df = df[1:]
            df.columns = new_header
            df = df.reset_index(drop=True)
            df.columns = ["funds", "market_value"]

            # Extract Fund Identifier
            fund_identifier = df.copy()
            fund_identifier["fund_identifier"] = df["funds"].str[-4:]
            fund_identifier = fund_identifier.iloc[:, -1:]
            fund_identifier["fund_identifier"] = fund_identifier[
                "fund_identifier"
            ].str.replace(")", "")

            # Combine Fund Identifier with Funds and Market Value
            df = pd.concat([df, fund_identifier], axis=1)
            df = df[["fund_identifier", "funds", "market_value"]]
            df = df.drop_duplicates(subset=["fund_identifier", "funds", "market_value"])
            df["member_count"] = ""
            df["fund_company"] = "Sun Life"
            df = df.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)

            # Add Invested Marker
            df["invested"] = np.where(df["market_value"] > 0, "Yes", "No")

            # Log status and save CSV
            self._log_status("Sun Life Assets", "cleaned", df.isna().sum().sum())
            self._save_csv(df, "Sun Life Assets", "Saved", export_path)

        except Exception as e:
            self._log_status("Sun Life Assets", "Failed", str(e))

        return None

    # Fees Data
    ################################################################
    def extract_transform_fees_data(self, export_path: str) -> None:
        """
        Extracts, transforms, and saves Sun Life Fees data.

        Args:
            export_path (str): Path where the transformed CSV will be saved.
        """
        try:
            # Retrieve Fees files
            files = self._get_files(prefix="SL_", suffix="_Fees")

            # Process Fees Data
            ################################################################
            df = pd.read_excel(files[0])
            df = df.iloc[:, [1, 5]].dropna()
            new_header = df.iloc[0]
            df = df[1:]
            df.columns = new_header
            df.columns = ["funds", "fees"]
            df = df.reset_index(drop=True)

            # Extract Fund Identifier from Funds Column
            fund_identifier = df.copy()
            fund_identifier["fund_identifier"] = df["funds"].str[-4:]
            fund_identifier = fund_identifier.iloc[:, -1:]
            fund_identifier["fund_identifier"] = fund_identifier[
                "fund_identifier"
            ].str.replace(")", "")

            # Combine Fund Identifier with Funds and Fees
            df = pd.concat([df, fund_identifier], axis=1)
            df = df[["fund_identifier", "funds", "fees"]]
            df = df.drop_duplicates(subset=["fund_identifier", "funds", "fees"])
            df["fund_company"] = "Sun Life"
            df = df.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)

            # Log status and save CSV
            self._log_status("Sun Life Fees", "cleaned", df.isna().sum().sum())
            self._save_csv(df, "Sun Life Fees", "Saved", export_path)

        except Exception as e:
            self._log_status("Sun Life Fees", "Failed", str(e))

        return None
