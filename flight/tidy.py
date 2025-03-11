from typing import Dict, List, Any
from loguru import logger
from hashlib import md5
import pandas as pd
import json
import glob
import re
import os


class TidyHistorical:
    """
    A class for processing and cleaning historical flight data from CSV files.

    This class handles various data transformation tasks including:
    - Loading multiple CSV files from a directory structure
    - Extracting nested JSON data into separate columns
    - Standardizing column names and data types
    - Creating unique identifiers for flight records
    - Applying a consistent schema to the resulting DataFrame
    """

    raw_columns: List[str] = [
        "movement",
        "number",
        "status",
        "codeshareStatus",
        "isCargo",
        "aircraft",
        "airline",
        "callSign",
        "code",
        "flight_type",
        "date",
    ]
    # Columns containing JSON data that need to be unpacked
    json_columns: List[str] = [
        "airline",
        "aircraft",
        "movement",
        "movement_airport",
        "movement_scheduledTime",
        "movement_revisedTime",
        "movement_runwayTime",
    ]

    # Columns containing list data that need to be joined into strings
    list_columns: List[str] = ["movement_quality"]

    # Columns used to create unique identifiers for each flight record
    id_columns: List[str] = [
        "number",
        "flight_type",
        "movement_scheduledTime_utc",
    ]

    # Target schema for the final DataFrame, including data types
    schema: Dict[str, Any] = {
        "id": str,
        "number": str,
        "code": str,
        "date": "datetime64[ns]",
        "flight_type": str,
        "status": str,
        "codeshare_status": str,
        "is_cargo": bool,
        "call_sign": str,
        "airline_name": str,
        "airline_iata": str,
        "airline_icao": str,
        "aircraft_model": str,
        "aircraft_reg": str,
        "aircraft_mode_s": str,
        "terminal": float,
        "baggage_belt": str,
        "quality": str,
        "gate": str,
        "airport_icao": str,
        "airport_iata": str,
        "airport_name": str,
        "airport_time_zone": str,
        "scheduled_time_utc": "datetime64[ns]",
        "scheduled_time_local": "datetime64[ns]",
        "revised_time_utc": "datetime64[ns]",
        "revised_time_local": "datetime64[ns]",
        "runway_time_utc": "datetime64[ns]",
        "runway_time_local": "datetime64[ns]",
    }

    @staticmethod
    def clean_json(value: Any) -> Dict[str, Any]:
        """
        Convert string JSON or dictionary to a properly formatted dictionary.

        Args:
            value: Input value that could be a JSON string or dictionary

        Returns:
            Dictionary representation of the input value, or empty dict if invalid
        """
        if isinstance(value, str):
            try:
                return json.loads(value.replace("'", '"'))
            except json.JSONDecodeError as e:
                logger.debug(f"Failed to parse JSON: {value}. Error: {e}")
        elif isinstance(value, dict):
            return value
        elif pd.isna(value):
            return {}
        return {}

    @staticmethod
    def camel_to_snake(name: str) -> str:
        """
        Convert camelCase or PascalCase string to snake_case.

        Args:
            name: String in camelCase or PascalCase format

        Returns:
            String converted to snake_case format
        """
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        name = re.sub("__([A-Z])", r"_\1", name)
        name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
        return name.lower()

    @staticmethod
    def read_file(path: str):
        date = os.path.basename(path).replace(".csv", "")
        df = pd.read_csv(path)
        df["date"] = date
        return df

    @classmethod
    def read_data(cls, path: str) -> pd.DataFrame:
        """
        Read and combine all CSV files from the specified path and its subdirectories.

        Args:
            path: Directory path containing CSV files

        Returns:
            Combined DataFrame of all CSV files, or empty DataFrame if none found
        """
        all_files = glob.glob(os.path.join(path, "**/*.csv"), recursive=True)
        if not all_files:
            logger.warning(f"No CSV files found in the specified path: {path}")
            return pd.DataFrame()

        return pd.concat(map(cls.read_file, all_files), ignore_index=True)

    @classmethod
    def unroll_column(
        cls,
        df: pd.DataFrame,
        colname: str,
    ) -> pd.DataFrame:
        """
        Extract nested JSON data from a column into separate columns.

        Args:
            df: Input DataFrame
            colname: Name of column containing JSON data

        Returns:
            DataFrame with JSON data expanded into multiple columns
        """
        if colname not in df.columns:
            logger.warning(f"Column {colname} not found in DataFrame")
            return pd.DataFrame()

        # Convert JSON strings to dictionaries
        values = df[colname].map(cls.clean_json)

        # Convert dictionaries to a DataFrame
        if values.empty or all(v == {} for v in values):
            return pd.DataFrame()

        aux = pd.DataFrame.from_records(values)

        # Prefix the original column name to all new column names
        aux.columns = [f"{colname}_{col}" for col in aux.columns]

        return aux

    @classmethod
    def clean_data(
        cls,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Apply all data cleaning operations to the DataFrame.

        Args:
            df: Input DataFrame with raw flight data

        Returns:
            Cleaned DataFrame with standardized structure
        """
        if df.empty:
            logger.warning("Empty DataFrame provided to clean_data")
            return df

        # Create a copy to avoid modifying the original DataFrame
        df = df.copy()

        # Process JSON columns
        for colname in cls.json_columns:
            if colname in df.columns:
                unroll_df = cls.unroll_column(df=df, colname=colname)
                if not unroll_df.empty:
                    df = pd.concat((df, unroll_df), axis=1)
                df.drop(colname, axis=1, inplace=True)
            else:
                logger.warning(f"JSON column {colname} not found in DataFrame")

        # Process list columns
        for colname in cls.list_columns:
            if colname in df.columns:
                df[colname] = df[colname].apply(
                    lambda x: (
                        "-".join(sorted(x))
                        if isinstance(x, list)
                        else (x if pd.notna(x) else "")
                    )
                )

        # Remove duplicates
        df = df.drop_duplicates().reset_index(drop=True)

        df["id"] = df.astype(str).apply("-".join, axis=1)
        df["id"] = df["id"].apply(lambda x: md5(x.encode("utf-8")).hexdigest())

        # Convert column names to snake_case
        df.columns = [cls.camel_to_snake(col) for col in df.columns]
        df.columns = [
            col.replace("movement_", "") if col.startswith("movement_") else col
            for col in df.columns
        ]

        # Apply schema to standardize data types
        schema_cols = [col for col in cls.schema.keys() if col in df.columns]
        for col in schema_cols:
            dtype = cls.schema[col]
            try:
                if "datetime" in str(dtype):
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                else:
                    df[col] = df[col].astype(dtype)
            except (ValueError, TypeError) as e:
                logger.warning(f"Error converting {col} to {dtype}: {e}")

        # Return only columns specified in the schema that exist in the DataFrame
        available_cols = [col for col in cls.schema.keys() if col in df.columns]
        missing_cols = set(cls.schema.keys()) - set(available_cols)
        if missing_cols:
            logger.warning(f"Missing columns in final DataFrame: {missing_cols}")

        return df[available_cols]

    @classmethod
    def tidy(
        cls,
        path: str,
    ) -> pd.DataFrame:
        """
        Main method to load, process, and clean flight data from CSV files.

        Args:
            path: Directory path containing CSV files

        Returns:
            Cleaned and standardized DataFrame of flight data
        """
        logger.info(f"Loading flight data from {path}")

        # Read data from CSV files
        df = cls.read_data(path=path)
        missing = set(cls.raw_columns) - set(df.columns)
        if missing:
            logger.error(f"Missing columns when reading data {','.join(missing)}")

        if df.empty:
            logger.warning("No data found or could be read")
            return df

        logger.info(f"Raw data loaded: {len(df)} rows, {len(df.columns)} columns")

        # Clean and process the data
        df = cls.clean_data(df=df)

        logger.info(
            f"Data processing complete: {len(df)} rows, {len(df.columns)} columns"
        )
        return df
