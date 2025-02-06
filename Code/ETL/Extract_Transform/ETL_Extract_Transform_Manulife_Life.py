import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
import warnings

import numpy as np

warnings.filterwarnings("ignore")

from typing import List, Optional

from ETL_Extract_Transform_Base import ExtractTransform


############################################################################################################################
# NEW BLOCK - Manulife Life Data
############################################################################################################################
# Class for handling Manulife data extraction and transformation
class Manulife(ExtractTransform):
    # Returns Data
    ################################################################
    def extract_transform_returns_data(self, export_path: str) -> None:
        """
        Extracts, transforms, and saves Manulife Returns data.

        Args:
            export_path (str): Path where the transformed CSV will be saved.
        """
        try:
            # Retrieve Returns Core files
            files = self._get_files(prefix="ML_", suffix="_Returns_Core")

            # Process Returns - Core Main
            ################################################################
            df_core = pd.read_excel(files[0])
            df_core = df_core.iloc[:, [0, 1, 4]].reset_index(drop=True)
            df_core = df_core.dropna()
            new_header = df_core.iloc[0]
            df_core = df_core[1:]
            df_core.columns = new_header
            df_core = df_core.reset_index(drop=True)
            df_core.columns = ["fund_identifier", "funds", "returns"]
            df_core = df_core.drop_duplicates(
                subset=["fund_identifier", "funds", "returns"]
            )
            df_core["fund_company"] = "Manulife"

            # Retrieve Returns - Other files
            ################################################################
            files = self._get_files(prefix="ML_", suffix="_Returns_Others")

            df_other = pd.read_excel(files[0])
            df_other = df_other.iloc[:, [0, 1, 4]].reset_index(drop=True)
            df_other = df_other.dropna()
            new_header = df_other.iloc[0]
            df_other = df_other[1:]
            df_other.columns = new_header
            df_other = df_other.reset_index(drop=True)
            df_other.columns = ["fund_identifier", "funds", "returns"]
            df_other = df_other.drop_duplicates(
                subset=["fund_identifier", "funds", "returns"]
            )
            df_other["fund_company"] = "Manulife"

            # Combine Core and Other Returns
            ################################################################
            # Determine available funds vs not available
            df_core1 = (
                pd.concat([df_core, df_other], axis=0)
                .drop_duplicates()
                .reset_index(drop=True)
            )
            df_core1["fund_type"] = "Available"

            df_other1 = (
                pd.concat([df_core, df_other], axis=0)
                .reset_index(drop=True)
                .drop_duplicates(keep=False)
            )
            df_other1["fund_type"] = "Not Available"

            # Concatenate dataframes
            df = pd.concat([df_core1, df_other1], axis=0).reset_index(drop=True)
            df = df.drop_duplicates(
                subset=["fund_identifier", "funds", "returns", "fund_type"]
            )
            df["fund_company"] = "Manulife"
            # Sort and remove duplicates
            df = df.sort_values(by=["fund_identifier", "fund_type"], ascending=True)
            df = df.drop_duplicates(
                subset=["fund_identifier"], keep="first"
            ).reset_index(drop=True)
            df = df.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)

            # Log status and save CSV
            self._log_status("Manulife Returns", "cleaned", df.isna().sum().sum())
            self._save_csv(df, "Manulife Returns", "Saved", export_path)

        except Exception as e:
            self._log_status("Manulife Returns", "Failed", str(e))

        return None

    # Assets Data
    ################################################################
    def extract_transform_assets_data(self, export_path: str) -> None:
        """
        Extracts, transforms, and saves Manulife Assets data.

        Args:
            export_path (str): Path where the transformed CSV will be saved.
        """
        try:
            # Retrieve Assets files
            files = self._get_files(prefix="ML_", suffix="_Assets")

            # Process Assets - Core Main
            ################################################################
            df = pd.read_excel(files[0])
            df = df.iloc[:, [1, 4]].dropna()
            new_header = df.iloc[0]
            df = df[1:]
            df.columns = new_header
            df = df.reset_index(drop=True)
            df.columns = ["funds", "market_value"]

            # Extract Fund Identifier
            fund_identifier = df.copy()
            fund_identifier["fund_identifier"] = df["funds"].str[:4]
            fund_identifier = fund_identifier.iloc[:, -1:]
            fund_identifier = fund_identifier[
                fund_identifier["fund_identifier"].apply(lambda x: x.isnumeric())
            ]

            # Combine Fund Identifier with Funds and Market Value
            df = pd.concat([df, fund_identifier], axis=1)
            df = df.dropna()
            df = df[["fund_identifier", "funds", "market_value"]]
            df = df.drop_duplicates(subset=["fund_identifier", "funds", "market_value"])
            df["member_count"] = ""
            df["fund_company"] = "Manulife"
            df = df.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)

            # Add Invested Marker
            df["invested"] = np.where(df["market_value"] > 0, "Yes", "No")

            # Log status and save CSV
            self._log_status("Manulife Assets", "cleaned", df.isna().sum().sum())
            self._save_csv(df, "Manulife Assets", "Saved", export_path)

        except Exception as e:
            self._log_status("Manulife Assets", "Failed", str(e))

        return None

    # Fees Data
    ################################################################
    def extract_transform_fees_data(self, export_path: str) -> None:
        """
        Extracts, transforms, and saves Manulife Fees data.

        Args:
            export_path (str): Path where the transformed CSV will be saved.
        """
        try:
            # Retrieve Fees files
            files = self._get_files(prefix="ML_", suffix="_Fees")

            # Process Fees Data
            ################################################################
            df = pd.read_excel(files[0])
            df = df.iloc[:, [0, 6]].dropna()
            new_header = df.iloc[0]
            df = df[1:]
            df.columns = new_header
            df.columns = ["funds", "fees"]
            df = df.reset_index(drop=True)

            # Extract Fund Identifier from Funds Column
            fund_identifier = df["funds"].str.split(" - ", expand=True).iloc[:, 0:1]
            df = pd.concat([fund_identifier, df], axis=1)
            df.columns = ["fund_identifier", "funds", "fees"]
            df = df.drop_duplicates(subset=["fund_identifier", "funds", "fees"])
            df["fund_company"] = "Manulife"
            df = df.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)

            # Log status and save CSV
            self._log_status("Manulife Fees", "cleaned", df.isna().sum().sum())
            self._save_csv(df, "Manulife Fees", "Saved", export_path)

        except Exception as e:
            self._log_status("Manulife Fees", "Failed", str(e))

        return None
