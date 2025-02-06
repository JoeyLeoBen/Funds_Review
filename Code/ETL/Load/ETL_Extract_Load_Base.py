import numpy as np
import pandas as pd
import psycopg2 as ps

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

import warnings

warnings.filterwarnings("ignore")

import datetime
import glob
import os
import shutil

# File management
import sys
from typing import List, Optional


############################################################################################################################
# NEW BLOCK - Base Class
############################################################################################################################
# Base class for Extract and Load operations
class ExtractLoad:
    def __init__(self, main_file_search_path: str) -> None:
        """
        Initializes the ExtractLoad class with the main file search path.

        Args:
            main_file_search_path (str): Path to search for input files.
        """
        self.main_file_search_path = main_file_search_path

    # Extract data
    ################################################################
    def _get_files(self, suffix: str) -> List[str]:
        """
        Retrieve files with the specified suffix from the search path.

        Args:
            suffix (str): Suffix that the target files should end with.

        Returns:
            List[str]: List of full paths to the matching files.

        Raises:
            FileNotFoundError: If no files are found with the specified suffix.
            NotADirectoryError: If the found path is not a valid directory.
        """
        paths = glob.glob(
            self.main_file_search_path, recursive=True
        )  # Returns a list of paths

        if not paths:
            raise FileNotFoundError(f"No files found in {self.main_file_search_path}")

        path = paths[0]  # Take the first matching path
        if not os.path.isdir(path):
            raise NotADirectoryError(f"The path '{path}' is not a valid directory.")

        files = []
        for i in os.listdir(path):
            if os.path.isfile(os.path.join(path, i)) and suffix in i:
                files.append(os.path.join(path, i))  # Append full paths to the files

        files_2 = []
        for file in files:
            file = file
            files_2.append(file)

        if not files:
            raise FileNotFoundError(
                f"No files with suffix '{suffix}' found in '{path}'"
            )

        return files_2

    # Save newly transformed data
    ################################################################
    def _save_csv(
        self, df: pd.DataFrame, label: str, status: str, export_path: str
    ) -> None:
        """
        Save a DataFrame to a CSV file.

        Args:
            df (pd.DataFrame): DataFrame to be saved.
            label (str): Label describing the operation.
            status (str): Status message to be printed.
            export_path (str): Path where the CSV will be saved.
        """
        df.to_csv(export_path, index=False, encoding="utf-8-sig")
        print(f"{label:{35}} {status:.>{20}}: {export_path}")

        return None

    # Log the status of process
    ################################################################
    def _log_status(self, label: str, status: str, nulls: Optional[int] = None) -> None:
        """
        Log the processing status with optional nulls information.

        Args:
            label (str): Label describing the operation.
            status (str): Status message to be printed.
            nulls (Optional[int], optional): Number of nulls encountered. Defaults to None.
        """
        if nulls is not None:
            print(f"{label:{35}} {status:.>{20}} & nulls: {nulls}")
        else:
            print(f"{label:{35}} {status:.>{20}}")

        return None

    # Store raw data
    ################################################################
    def store_data(
        self, label: str, status: str, prefix: str, store_export_path: str
    ) -> None:
        """
        Move files with a specified prefix to the export directory organized by date.

        Args:
            label (str): Label describing the operation.
            status (str): Status message to be printed.
            prefix (str): Prefix that the target files should start with.
            store_export_path (str): Path to store the exported files.
        """
        files = [
            f for f in os.listdir(self.main_file_search_path) if f.startswith(prefix)
        ]

        if len(files) > 0:
            try:
                date_dir = os.path.join(
                    store_export_path, datetime.datetime.now().strftime("%Y-%m-%d")
                )
                os.makedirs(date_dir, exist_ok=True)
                for file in files:
                    shutil.move(
                        os.path.join(self.main_file_search_path, file),
                        os.path.join(date_dir, file),
                    )
                    print(f"{label:{35}} {status:.>{20}}: {date_dir}")
            except Exception as e:
                print(f"Error moving files: {e}")
        else:
            print(f"{label:{35}} No files found with prefix '{prefix}' to store.")

        return None
