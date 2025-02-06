import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
import warnings

import numpy as np
from ETL_Extract_Transform_Base import ExtractTransform

warnings.filterwarnings("ignore")

from typing import List


############################################################################################################################
# NEW BLOCK - Industrial Alliance Life Data
############################################################################################################################
# Class for handling Industrial Alliance data extraction and transformation
class IndustrialAlliance(ExtractTransform):
    # Returns Data
    ################################################################
    def extract_transform_returns_data(self, export_path: str) -> None:
        """
        Extracts, transforms, and saves Industrial Alliance Returns data.

        Args:
            export_path (str): Path where the transformed CSV will be saved.
        """
        try:
            # Retrieve Returns Core files
            files = self._get_files(prefix="IA_", suffix="Fund_Returns_Core")

            # Process Returns - Core Main
            ################################################################
            df_core = pd.read_excel(files[0], sheet_name="Funds")
            df_core = df_core.iloc[:, :9].dropna()
            new_header = df_core.iloc[0]
            df_core = df_core[1:]
            df_core.columns = new_header
            df_core = df_core.reset_index(drop=True)

            # Save to CSV to handle duplicate columns and locked spreadsheet
            df_core.to_csv(
                self.main_file_search_path + "\\" + "IA_Returns_Core.csv", index=False
            )
            df_core = pd.read_csv(
                self.main_file_search_path + "\\" + "IA_Returns_Core.csv"
            )

            df_core = df_core[["Investment Funds", "Fund Code", "1 month"]]
            df_core["Fund Code"] = df_core["Fund Code"].astype(str) + "BRUT"
            df_core.columns = ["funds", "fund_identifier", "returns"]
            df_core = df_core[["fund_identifier", "funds", "returns"]]
            df_core = df_core.loc[df_core["returns"] != "1 MO"]
            df_core["fund_type"] = "Available"
            df_core["fund_company"] = "Industrial Alliance"
            df_core["returns"] = df_core["returns"].str.replace(
                r"-(?![0-9])", "99999999", regex=True
            )
            df_core["returns"] = df_core["returns"].str.strip()
            df_core["returns"] = df_core["returns"].astype(float)
            df_core = df_core.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)
            # Adjust returns due to percentage formatting
            df_core["returns"] = df_core["returns"] * 100

            # Retrieve Returns - Other files
            ################################################################
            files = self._get_files(prefix="IA_", suffix="Fund_Returns_Core")

            df_other = pd.read_excel(files[0], sheet_name="Portfolios")
            df_other = df_other.iloc[:, :5].dropna()
            new_header = df_other.iloc[0]
            df_other = df_other[1:]
            df_other.columns = new_header
            df_other = df_other.reset_index(drop=True)

            # Save to CSV to handle duplicate columns and locked spreadsheet
            df_other.to_csv(
                self.main_file_search_path + "\\" + "IA_Returns_Others.csv", index=False
            )
            df_other = pd.read_csv(
                self.main_file_search_path + "\\" + "IA_Returns_Others.csv"
            )

            # Remove redundant lettering from fund code
            df_other["Portfolio Code.1"] = df_other["Portfolio Code.1"].str.replace(
                r"P", ""
            )

            df_other = df_other[["Portfolio", "Portfolio Code.1", "1 month"]]
            df_other["Portfolio Code.1"] = (
                "FU" + df_other["Portfolio Code.1"].astype(str) + "BRUT"
            )
            df_other.columns = ["funds", "fund_identifier", "returns"]
            df_other = df_other[["fund_identifier", "funds", "returns"]]
            df_other["fund_type"] = "Available"
            df_other["fund_company"] = "Industrial Alliance"
            df_other["returns"] = (
                df_other["returns"]
                .astype(str)
                .str.replace(r"-(?![0-9])", "99999999", regex=True)
            )
            df_other["returns"] = df_other["returns"].str.strip()
            df_other["returns"] = df_other["returns"].astype(float)
            df_other = df_other.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)
            # Adjust returns due to percentage formatting
            df_other["returns"] = df_other["returns"] * 100

            # Combine Core and Other Returns
            ################################################################
            df = pd.concat([df_core, df_other], axis=0)

            # Log status and save CSV
            self._log_status(
                "Industrial Alliance Returns", "cleaned", df.isna().sum().sum()
            )
            self._save_csv(df, "Industrial Alliance Returns", "Saved", export_path)

        except Exception as e:
            self._log_status("Industrial Alliance Returns", "Failed", str(e))

        return None

    # Assets Data
    ################################################################
    def extract_transform_assets_data(self, export_path: str) -> None:
        """
        Extracts, transforms, and saves Industrial Alliance Assets data.

        Args:
            export_path (str): Path where the transformed CSV will be saved.
        """
        try:
            # Retrieve Assets files
            files = self._get_files(prefix="IA_", suffix="_Assets")

            # DPSP Assets Processing
            # ************************************************************************************************************************#
            assets_dpsp_df = pd.read_excel(files[0])
            assets_dpsp_df["fund_identifier"] = [
                str(x)[:5] if isinstance(x, str) else ""
                for x in assets_dpsp_df["GLOB Investment"]
            ]
            assets_dpsp_df["fund_identifier"] = assets_dpsp_df[
                "fund_identifier"
            ].str.strip()
            assets_dpsp_df["fund_identifier"] = assets_dpsp_df[
                "fund_identifier"
            ].str.replace(r"P", "FU")
            assets_dpsp_df["fund_identifier"] = (
                assets_dpsp_df["fund_identifier"].astype(str) + "BRUT"
            )
            # Get fund name
            assets_dpsp_df["funds"] = [
                str(x)[7:] if isinstance(x, str) else ""
                for x in assets_dpsp_df["GLOB Investment"]
            ]
            assets_dpsp_df["funds"] = assets_dpsp_df["funds"].str.strip()
            assets_dpsp_df["member_count"] = ""

            # Attitude Funds (not including individual funds)
            ################################################################
            assets_dpsp_att_df = assets_dpsp_df.rename(
                columns={"Total Member Asset": "market_value"}
            )
            assets_dpsp_att_df = assets_dpsp_att_df[
                ["fund_identifier", "funds", "market_value", "member_count"]
            ]
            assets_dpsp_att_df = assets_dpsp_att_df.loc[
                assets_dpsp_att_df["fund_identifier"] != "BRUT"
            ].reset_index(drop=True)

            # Individual Funds (not including Attitude funds)
            ################################################################
            assets_dpsp_no_att_df = assets_dpsp_df.loc[
                assets_dpsp_df["fund_identifier"] == "BRUT"
            ].T.reset_index()
            assets_dpsp_no_att_df = assets_dpsp_no_att_df[24:-3].dropna(how="all")
            assets_dpsp_no_att_df["market_value"] = assets_dpsp_no_att_df.iloc[
                :, 1:
            ].sum(axis=1)
            # Create data to concat with attitude assets
            assets_dpsp_no_att_df["fund_identifier"] = [
                str(x)[:5] if isinstance(x, str) else ""
                for x in assets_dpsp_no_att_df["index"]
            ]
            assets_dpsp_no_att_df["fund_identifier"] = assets_dpsp_no_att_df[
                "fund_identifier"
            ].str.strip()
            assets_dpsp_no_att_df["fund_identifier"] = assets_dpsp_no_att_df[
                "fund_identifier"
            ].str.replace(r"P", "FU")
            assets_dpsp_no_att_df["fund_identifier"] = (
                assets_dpsp_no_att_df["fund_identifier"].astype(str) + "BRUT"
            )
            # Get fund name
            assets_dpsp_no_att_df["funds"] = [
                str(x)[7:] if isinstance(x, str) else ""
                for x in assets_dpsp_no_att_df["index"]
            ]
            assets_dpsp_no_att_df["funds"] = assets_dpsp_no_att_df["funds"].str.strip()
            assets_dpsp_no_att_df["member_count"] = ""
            assets_dpsp_no_att_df = assets_dpsp_no_att_df.rename(
                columns={"Total Member Asset": "market_value"}
            )
            assets_dpsp_no_att_df = assets_dpsp_no_att_df[
                ["fund_identifier", "funds", "market_value", "member_count"]
            ].reset_index(drop=True)
            assets_dpsp_no_att_df = assets_dpsp_no_att_df.loc[
                assets_dpsp_no_att_df["market_value"] != 0
            ].reset_index(drop=True)

            # Total DPSP Assets
            ################################################################
            dpsp_df = pd.concat(
                [assets_dpsp_att_df, assets_dpsp_no_att_df], axis=0
            ).reset_index(drop=True)
            dpsp_df = dpsp_df.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)

            # RRSP Assets Processing
            # ************************************************************************************************************************#
            assets_rrsp_df = pd.read_excel(self.main_file_search_path + str(files[1]))
            assets_rrsp_df["fund_identifier"] = [
                str(x)[:5] if isinstance(x, str) else ""
                for x in assets_rrsp_df["GLOB Investment"]
            ]
            assets_rrsp_df["fund_identifier"] = assets_rrsp_df[
                "fund_identifier"
            ].str.strip()
            assets_rrsp_df["fund_identifier"] = assets_rrsp_df[
                "fund_identifier"
            ].str.replace(r"P", "FU")
            assets_rrsp_df["fund_identifier"] = (
                assets_rrsp_df["fund_identifier"].astype(str) + "BRUT"
            )
            # Get fund name
            assets_rrsp_df["funds"] = [
                str(x)[7:] if isinstance(x, str) else ""
                for x in assets_rrsp_df["GLOB Investment"]
            ]
            assets_rrsp_df["funds"] = assets_rrsp_df["funds"].str.strip()
            assets_rrsp_df["member_count"] = ""

            # Attitude Funds (not including individual funds)
            ################################################################
            assets_rrsp_att_df = assets_rrsp_df.rename(
                columns={"Total Member Asset": "market_value"}
            )
            assets_rrsp_att_df = assets_rrsp_att_df[
                ["fund_identifier", "funds", "market_value", "member_count"]
            ]
            assets_rrsp_att_df = assets_rrsp_att_df.loc[
                assets_rrsp_att_df["fund_identifier"] != "BRUT"
            ].reset_index(drop=True)

            # Individual Funds (not including Attitude funds)
            ################################################################
            assets_rrsp_no_att_df = assets_rrsp_df.loc[
                assets_rrsp_df["fund_identifier"] == "BRUT"
            ].T.reset_index()
            # Drop rows with NaN values in all columns
            assets_rrsp_no_att_df = assets_rrsp_no_att_df[24:-3].dropna(how="all")
            assets_rrsp_no_att_df["market_value"] = assets_rrsp_no_att_df.iloc[
                :, 1:
            ].sum(axis=1)
            # Create data to concat with attitude assets
            assets_rrsp_no_att_df["fund_identifier"] = [
                str(x)[:5] if isinstance(x, str) else ""
                for x in assets_rrsp_no_att_df["index"]
            ]
            assets_rrsp_no_att_df["fund_identifier"] = assets_rrsp_no_att_df[
                "fund_identifier"
            ].str.strip()
            assets_rrsp_no_att_df["fund_identifier"] = assets_rrsp_no_att_df[
                "fund_identifier"
            ].str.replace(r"P", "FU")
            assets_rrsp_no_att_df["fund_identifier"] = (
                assets_rrsp_no_att_df["fund_identifier"].astype(str) + "BRUT"
            )
            # Get fund name
            assets_rrsp_no_att_df["funds"] = [
                str(x)[7:] if isinstance(x, str) else ""
                for x in assets_rrsp_no_att_df["index"]
            ]
            assets_rrsp_no_att_df["funds"] = assets_rrsp_no_att_df["funds"].str.strip()
            assets_rrsp_no_att_df["member_count"] = ""
            assets_rrsp_no_att_df = assets_rrsp_no_att_df.rename(
                columns={"Total Member Asset": "market_value"}
            )
            assets_rrsp_no_att_df = assets_rrsp_no_att_df[
                ["fund_identifier", "funds", "market_value", "member_count"]
            ].reset_index(drop=True)
            assets_rrsp_no_att_df = assets_rrsp_no_att_df.loc[
                assets_rrsp_no_att_df["market_value"] != 0
            ].reset_index(drop=True)

            # Total RRSP Assets
            ################################################################
            rrsp_df = pd.concat(
                [assets_rrsp_att_df, assets_rrsp_no_att_df], axis=0
            ).reset_index(drop=True)
            rrsp_df = rrsp_df.replace(r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True)

            # Total Assets
            # ************************************************************************************************************************#
            ia_assets_df = pd.concat([dpsp_df, rrsp_df], axis=0).reset_index(drop=True)
            ia_assets_df = (
                ia_assets_df.groupby(by=["fund_identifier", "funds", "member_count"])
                .sum()
                .reset_index()
            )

            ia_assets_df = ia_assets_df[
                ["fund_identifier", "funds", "market_value", "member_count"]
            ]
            ia_assets_df["market_value"] = (
                ia_assets_df["market_value"]
                .astype(str)
                .str.replace(r"-(?![0-9])", "99999999")
            )
            ia_assets_df["market_value"] = ia_assets_df["market_value"].str.strip()
            ia_assets_df["market_value"] = ia_assets_df["market_value"].astype(float)
            ia_assets_df = ia_assets_df.replace(
                r"[^0-9a-zA-Z-()%$*+& ]", "", regex=True
            )
            ia_assets_df["fund_company"] = "Industrial Alliance"

            # Add Invested Marker
            ia_assets_df["invested"] = np.where(
                ia_assets_df["market_value"] > 0, "Yes", "No"
            )

            # Log status and save CSV
            self._log_status(
                "Industrial Alliance Assets", "cleaned", ia_assets_df.isna().sum().sum()
            )
            self._save_csv(
                ia_assets_df, "Industrial Alliance Assets", "Saved", export_path
            )

        except Exception as e:
            self._log_status("Industrial Alliance Assets", "Failed", str(e))

        return None
