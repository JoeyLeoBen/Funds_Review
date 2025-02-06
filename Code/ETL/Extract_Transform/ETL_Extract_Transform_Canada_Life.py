import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
import warnings

import numpy as np
from ETL_Extract_Transform_Base import ExtractTransform

warnings.filterwarnings("ignore")

from typing import List


############################################################################################################################
# NEW BLOCK - Canada Life Data
############################################################################################################################
# Class for handling Canada Life data extraction and transformation
class CanadaLife(ExtractTransform):
    # Returns Data
    ################################################################
    def extract_transform_returns_data(self, export_path: str) -> None:
        """
        Extracts, transforms, and saves Canada Life Returns data.

        Args:
            export_path (str): Path where the transformed CSV will be saved.
        """
        try:
            # Retrieve Returns Core files
            files = self._get_files(prefix="CL_", suffix="_Returns_Core")

            # Process Returns - Core Main
            ################################################################
            df_core = pd.read_excel(files[0])
            df_core = df_core.iloc[:, :3].dropna()
            new_header = df_core.iloc[0]
            df_core = df_core[1:]
            df_core.columns = new_header
            df_core = df_core.reset_index(drop=True)
            df_core.columns = ["funds", "fund_identifier", "returns"]
            df_core = df_core[["fund_identifier", "funds", "returns"]]
            df_core = df_core.loc[df_core["returns"] != "1 MO"]
            df_core["fund_type"] = "Available"

            # Retrieve Returns - Other files
            ################################################################
            cl_files = self._get_files(prefix="CL_", suffix="_Returns_Others")

            df_other = pd.read_excel(cl_files[0])
            df_other = df_other.iloc[:, :3].dropna()
            new_header = df_other.iloc[0]
            df_other = df_other[1:]
            df_other.columns = new_header
            df_other = df_other.reset_index(drop=True)
            df_other.columns = ["funds", "fund_identifier", "returns"]
            df_other = df_other[["fund_identifier", "funds", "returns"]]
            df_other = df_other.loc[df_other["returns"] != "1 MO"]
            df_other["fund_type"] = "Not Available"

            # Combine Core and Other Returns
            ################################################################
            df = pd.concat([df_core, df_other], axis=0).reset_index(drop=True)
            df = df.drop_duplicates(
                subset=["fund_identifier", "funds", "returns", "fund_type"]
            )
            df["fund_company"] = "Canada Life"
            df = df.sort_values(by=["fund_identifier", "fund_type"], ascending=True)
            df = df.drop_duplicates(
                subset=["fund_identifier"], keep="first"
            ).reset_index(drop=True)
            df = df.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)

            # Log status and save CSV
            self._log_status("Canada Life Returns", "cleaned", df.isna().sum().sum())
            self._save_csv(df, "Canada Life Returns", "Saved", export_path)

        except Exception as e:
            self._log_status("Canada Life Returns", "Failed", str(e))

        return None

    # Assets Data
    ################################################################
    def extract_transform_assets_data(self, export_path: str) -> None:
        """
        Extracts, transforms, and saves Canada Life Assets data.

        Args:
            export_path (str): Path where the transformed CSV will be saved.
        """
        try:
            # Retrieve Assets files
            files = self._get_files(prefix="CL_", suffix="_Assets")

            # Read and concatenate multiple Assets Excel files
            ################################################################
            dfs = [
                pd.read_excel(file, header=None, names=["A", "B", "C"])
                for file in files
            ]
            df = pd.concat(dfs, axis=0, ignore_index=False).reset_index()
            df = df.dropna().reset_index(drop=True)

            new_header = df.iloc[0]
            df = df[1:]
            df.columns = new_header
            df = df.reset_index(drop=True)
            df = df.replace(df.columns[0], np.NaN)
            df = df.replace(df.columns[1], np.NaN)
            df = df.replace(df.columns[2], np.NaN)
            df = df.dropna()
            df = df.groupby(["Fund identifier", "Fund"]).sum().reset_index()
            df.columns = ["fund_identifier", "funds", "market_value", "member_count"]
            df = df[["fund_identifier", "funds", "market_value", "member_count"]]
            df = df.drop_duplicates(
                subset=["fund_identifier", "funds", "market_value", "member_count"]
            )
            df["fund_company"] = "Canada Life"
            df = df.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)

            # Add Invested Marker
            df["invested"] = np.where(df["market_value"] > 0, "Yes", "No")

            # Log status and save CSV
            self._log_status("Canada Life Assets", "cleaned", df.isna().sum().sum())
            self._save_csv(df, "Canada Life Assets", "Saved", export_path)

        except Exception as e:
            self._log_status("Canada Life Assets", "Failed", str(e))

        return None

    # Fees Data
    ################################################################
    def extract_transform_fees_data(self, export_path: str) -> None:
        """
        Extracts, transforms, and saves Canada Life Fees data.

        Args:
            export_path (str): Path where the transformed CSV will be saved.
        """
        try:
            # Retrieve Fees files
            files = self._get_files(prefix="CL_", suffix="_Fees")

            df = pd.read_excel(files[0])
            df = pd.concat([df.iloc[:, 0:1], df.iloc[:, -1:]], axis=1)
            df = df.dropna().reset_index(drop=True)
            df.columns = ["funds", "fees"]

            # Extract Fund Identifier
            fund_identifier = df["funds"].str.split("(", expand=True).iloc[:, -1:]
            fund_identifier.columns = ["fund_identifier"]
            fund_identifier = pd.DataFrame(
                fund_identifier["fund_identifier"].str.replace(")", "")
            )

            df = pd.concat([df, fund_identifier], axis=1)
            df = df[["fund_identifier", "funds", "fees"]]
            df["fund_identifier"] = df["fund_identifier"].str.split().str[0]
            df = df.copy()
            df = df.drop_duplicates(subset=["fund_identifier", "funds", "fees"])
            df["fund_company"] = "Canada Life"
            df = df.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)

            # Log status and save CSV
            self._log_status("Canada Life Fees", "cleaned", df.isna().sum().sum())
            self._save_csv(df, "Canada Life Fees", "Saved", export_path)

        except Exception as e:
            self._log_status("Canada Life Fees", "Failed", str(e))

        return None
