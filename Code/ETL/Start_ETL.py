import os
import sys
from datetime import datetime
from typing import Tuple

import psycopg2 as ps
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Load Modules
################################################################
# Get the current working directory (the directory of the running script)
current_dir = os.getcwd()
# Add the target directory to the system path
sys.path.append(os.path.abspath(os.path.join(current_dir, "Load")))
from ETL_Extract_Load_Base import ExtractLoad
from ETL_Load_Data import *
from ETL_Process_Data import *

# Extract & Transform Modules
################################################################
# Get the current working directory (the directory of the running script)
current_dir = os.getcwd()
# Add the target directory to the system path
sys.path.append(os.path.abspath(os.path.join(current_dir, "Extract_Transform")))
from ETL_Extract_Transform_Base import ExtractTransform
from ETL_Extract_Transform_Canada_Life import *
from ETL_Extract_Transform_Industrial_Alliance import *
from ETL_Extract_Transform_Manulife_Life import *
from ETL_Extract_Transform_Sun_Life import *


def main() -> None:
    """
    Runs all ETL-based tasks prompted by the user.
    """
    ###########################################################################################################################################
    # NEW BLOCK - Module 1 - DATA CLEANING
    ###########################################################################################################################################

    # Validation
    yesChoice = ["yes"]
    noChoice = ["no"]

    date_validation = list(range(0, 13))  # [0,1,2,...,12]

    # Input validation for DATA CLEANING
    while True:
        try:
            input_1 = str(
                input("Would you like to run the DATA CLEANING task? 'yes', or 'no' ")
            )
            input_1 = input_1.lower()
        except ValueError:
            print("Please select a valid response as described")
            continue

        if input_1 in yesChoice or input_1 in noChoice:
            break
        else:
            print("Please select a valid response as described")

    if input_1 in yesChoice:
        try:
            # Canada Life
            ################################################################
            cl_transform = CanadaLife(
                main_file_search_path=os.path.abspath(
                    os.path.join(current_dir, "..", "..", "..", "Funds_Review")
                ),
                store_export_path=os.path.abspath(
                    os.path.join(current_dir, "..", "..", "Canada_Life_Fund_Data_Raw")
                ),
            )

            cl_transform.extract_transform_returns_data(
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Canada_Life_Returns.csv",
                    )
                )
            )
            cl_transform.extract_transform_assets_data(
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir, "..", r"ELT\Funds_Data\seeds\raw_assets.csv"
                    )
                )
            )
            cl_transform.extract_transform_fees_data(
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir, "..", r"ELT\Funds_Data\seeds\raw_fees.csv"
                    )
                )
            )

            cl_transform.store_data(
                label="Canada Life Returns", status="Moved", prefix="CL_Fund_Returns"
            )

            cl_transform.store_data(
                label="Canada Life Assets", status="Moved", prefix="CL_Assets"
            )

            cl_transform.store_data(
                label="Canada Life Fees", status="Moved", prefix="CL_Fees"
            )

            # Manulife
            ################################################################
            ml_transform = Manulife(
                main_file_search_path=os.path.abspath(
                    os.path.join(current_dir, "..", "..", "..", "Funds_Review")
                ),
                store_export_path=os.path.abspath(
                    os.path.join(current_dir, "..", "..", "Manulife_Fund_Data_Raw")
                ),
            )

            ml_transform.extract_transform_returns_data(
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Manulife_Returns.csv",
                    )
                )
            )
            ml_transform.extract_transform_assets_data(
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir, "..", r"ELT\Funds_Data\seeds\raw_assets.csv"
                    )
                )
            )
            ml_transform.extract_transform_fees_data(
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir, "..", r"ELT\Funds_Data\seeds\raw_fees.csv"
                    )
                )
            )

            ml_transform.store_data(
                label="Manulife Returns", status="Moved", prefix="ML_Fund_Returns"
            )

            ml_transform.store_data(
                label="Manulife Assets", status="Moved", prefix="ML_Assets"
            )

            ml_transform.store_data(
                label="Manulife Fees", status="Moved", prefix="ML_Fees"
            )

            # Sun Life
            ################################################################
            sl_transform = SunLife(
                main_file_search_path=os.path.abspath(
                    os.path.join(current_dir, "..", "..", "..", "Funds_Review")
                ),
                store_export_path=os.path.abspath(
                    os.path.join(current_dir, "..", "..", "Sun_Life_Fund_Data_Raw")
                ),
            )

            sl_transform.extract_transform_returns_data(
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Sun_Life_Returns.csv",
                    )
                )
            )
            sl_transform.extract_transform_assets_data(
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir, "..", r"ELT\Funds_Data\seeds\raw_assets.csv"
                    )
                )
            )
            sl_transform.extract_transform_fees_data(
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir, "..", r"ELT\Funds_Data\seeds\raw_fees.csv"
                    )
                )
            )

            sl_transform.store_data(
                label="Sun Life Returns", status="Moved", prefix="SL_Fund_Returns"
            )

            sl_transform.store_data(
                label="Sun Life Assets", status="Moved", prefix="SL_Assets"
            )

            sl_transform.store_data(
                label="Sun Life Fees", status="Moved", prefix="SL_Fees"
            )

            # Industrial Alliance
            ################################################################
            ia_transform = IndustrialAlliance(
                main_file_search_path=os.path.abspath(
                    os.path.join(current_dir, "..", "..", "..", "Funds_Review")
                ),
                store_export_path=os.path.abspath(
                    os.path.join(
                        current_dir, "..", "..", "Industrial_Alliance_Fund_Data_Raw"
                    )
                ),
            )

            ia_transform.extract_transform_returns_data(
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Industrial_Alliance_Returns.csv",
                    )
                )
            )
            ia_transform.extract_transform_assets_data(
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir, "..", r"ELT\Funds_Data\seeds\raw_assets.csv"
                    )
                )
            )

            ia_transform.store_data(
                label="Industrial Alliance Returns",
                status="Moved",
                prefix="IA_",
            )

            ia_transform.store_data(
                label="Industrial Alliance Assets", status="Moved", prefix="IA_Assets"
            )

            """
            - Cleans all funds data
            """
            input(
                "DATA CLEANING complete, please press enter to continue to ETL task or Ctrl C to end the program"
            )

        except Exception as e:
            print(e)
            input(
                "Please check the noted error above and decide to press enter and continue to ETL or Ctrl C to end the program"
            )

    else:
        print("You have skipped DATA CLEANING")

    ###########################################################################################################################################
    # NEW BLOCK - Module 2 - ETL
    ###########################################################################################################################################

    # Input validation for ETL
    while True:
        try:
            input_2 = str(input("Would you like to run the ETL task? 'yes', or 'no' "))
            input_2 = input_2.lower()
        except ValueError:
            print("Please select a valid response as described")
            continue

        if input_2 in yesChoice or input_2 in noChoice:
            break
        else:
            print("Please select a valid response as described")

    if input_2 in yesChoice:
        try:
            while True:
                try:
                    input_date = str(
                        input(
                            "What date is this data for? 'ex: enter 0 for the current month and >= 1 for every extra month previous "
                        )
                    )
                    input_date = int(input_date)
                except ValueError:
                    print("Please select a valid response as described")
                    continue

                if input_date in date_validation:
                    break
                else:
                    print("Please select a valid response as described")

            # Calculate the target date
            ################################################################
            target_date = datetime.now() - relativedelta(months=input_date)
            target_date = target_date.strftime("%Y-%m")

            # Process data
            ################################################################
            process_load_data = ProcessLoadData(
                main_file_search_path=os.path.abspath(
                    os.path.join(current_dir, "..", "..", "..", "Funds_Review")
                )
            )

            full_df = process_load_data.process_data(date=target_date)
            fund_company_df = process_load_data.process_fund_company_data(
                full_df=full_df
            )
            fund_df = process_load_data.process_fund_data(full_df=full_df)
            returns_df = process_load_data.process_returns_data(full_df=full_df)

            # Connect to database
            ################################################################
            try:
                conn = ps.connect(
                    f"""
                    host=localhost
                    dbname=funds_review_db
                    user=postgres
                    password={DB_PASSWORD}
                """
                )

                cur = conn.cursor()

                print("Successfully connected to funds_review_db")

            except ps.Error as e:
                print("Database connection error:")
                print(e)
                sys.exit(1)  # Exit the program if connection fails

            # Load processed data
            ################################################################
            load_data = LoadData(
                main_file_search_path=os.path.abspath(
                    os.path.join(current_dir, "..", "..", "..", "Funds_Review")
                )
            )

            load_data.load_fund_company_data(
                fund_company_df=fund_company_df,
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Table_Backup\Fund_Company_Backup.csv",
                    )
                ),
                conn=conn,
                cur=cur,
            )

            load_data.load_fund_data(
                fund_df=fund_df,
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Table_Backup\Funds_Backup.csv",
                    )
                ),
                conn=conn,
                cur=cur,
            )

            load_data.load_returns_data(
                returns_df=returns_df,
                export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Table_Backup\Returns_Backup.csv",
                    )
                ),
                conn=conn,
                cur=cur,
            )

            # Store processed data
            ################################################################
            store_processed_data = ExtractLoad(
                main_file_search_path=os.path.abspath(
                    os.path.join(current_dir, "..", "..", "..", "Funds_Review")
                )
            )

            store_processed_data.store_data(
                label="Canada Life Cleaned Returns",
                status="Moved",
                prefix="Canada_Life_Returns",
                store_export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Canada_Life_Fund_Data_Cleaned",
                    )
                ),
            )

            store_processed_data.store_data(
                label="Manulife Cleaned Returns",
                status="Moved",
                prefix="Manulife_Returns",
                store_export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Manulife_Fund_Data_Cleaned",
                    )
                ),
            )

            store_processed_data.store_data(
                label="Sun Life Cleaned Returns",
                status="Moved",
                prefix="Sun_Life_Returns",
                store_export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Sun_Life_Fund_Data_Cleaned",
                    )
                ),
            )

            store_processed_data.store_data(
                label="Industrial Alliance Cleaned Returns",
                status="Moved",
                prefix="Industrial_Alliance_Returns",  # Corrected typo from 'Aliiance' to 'Alliance'
                store_export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Industrial_Alliance_Fund_Data_Cleaned",
                    )
                ),
            )

            # Store backup data
            ################################################################
            store_backup_data = ExtractLoad(
                main_file_search_path=os.path.abspath(
                    os.path.join(
                        current_dir, "..", "..", "..", r"Funds_Review\Table_Backup"
                    )
                )
            )

            store_backup_data.store_data(
                label="Fund Company Backup Data",
                status="Moved",
                prefix="Fund_Company_Backup",
                store_export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Table_Backup\Fund_Company",
                    )
                ),
            )

            store_backup_data.store_data(
                label="Funds Backup Data",
                status="Moved",
                prefix="Funds_Backup",
                store_export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Table_Backup\Funds",
                    )
                ),
            )

            store_backup_data.store_data(
                label="Returns Backup Data",
                status="Moved",
                prefix="Returns_Backup",
                store_export_path=os.path.abspath(
                    os.path.join(
                        current_dir,
                        "..",
                        "..",
                        "..",
                        r"Funds_Review\Table_Backup\Returns",
                    )
                ),
            )
            """
            - Runs the ETL task for funds_review_db
            """
            input("ETL complete, please press enter or Ctrl C to end the program")

        except Exception as e:
            print(e)
            input(
                "Please press enter and check the noted error above or Ctrl C to end the program"
            )

    else:
        print("You have skipped the ETL task")
        print("Program complete")

    return None


if __name__ == "__main__":
    main()
