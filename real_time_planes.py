"""
Real-time Flight Data Processing Pipeline

This module defines a Dagster pipeline that fetches real-time flight data
from Bogota airport (BOG) and stores it in a DuckDB database.

Components:
-----------
- FlightDataRealTime: External API client class that retrieves flight data
- DuckDBPandasIOManager: Dagster I/O manager for storing pandas DataFrames in DuckDB
- real_time_planes: Asset that fetches and returns flight data as a DataFrame

Configuration:
-------------
DB_PATH: Path to the DuckDB database file used for storage ('/app/data/db/prod.duckdb')

Usage:
------
Run the pipeline with:
    $ dagster dev -f <filename>.py
    $ dagit  -f <filename>.py

Or deploy as part of a larger Dagster instance.

Notes:
------
- The asset is defined with key_prefix="main_silver", indicating this is a silver-layer
  asset in the medallion architecture
- Real-time flight data is fetched specifically for Bogota airport (BOG)
- Data is stored automatically in the DuckDB database through the I/O manager
"""

from dagster_duckdb_pandas import DuckDBPandasIOManager
from flight.api import FlightDataRealTime
from dagster import asset, Definitions
from pandas import DataFrame

DB_PATH = "/app/data/db/prod.duckdb"


@asset(key_prefix=["main_silver"])
def real_time_planes() -> DataFrame:
    return FlightDataRealTime.get_data("BOG")


defs = Definitions(
    assets=[real_time_planes],
    resources={"io_manager": DuckDBPandasIOManager(database=DB_PATH)},
)
