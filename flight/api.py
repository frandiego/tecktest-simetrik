from datetime import date, datetime, timezone
from loguru import logger
from typing import Union
import pandas as pd
import requests
import time
import os


class FlightDataHistorical:
    api_key: str = os.environ["API_KEY_FLIGHTS"]
    url: str = "https://app.goflightlabs.com/historical"

    @classmethod
    def api_get_historical(
        cls,
        date: Union[date, str],
        flight_type: str,
        airport_code: str,
    ) -> dict:
        """
        Fetches historical flight data from the API.

        Args:
            date (Union[date, str]): The date for which to fetch data.
            flight_type (str): The type of flight data ('arrival' or 'departure').
            airport_code (str): The airport code.

        Returns:
            dict: The API response as a dictionary.
        """
        try:
            response = requests.get(
                cls.url,
                params={
                    "access_key": cls.api_key,
                    "code": airport_code,
                    "date": str(date),
                    "type": str(flight_type),
                },
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    @classmethod
    def api_get_historical_data(
        cls,
        date: Union[date, str],
        airport_code: str,
        flight_type: str,
    ) -> pd.DataFrame:
        """
        Fetches historical flight data and converts it to a DataFrame.

        Args:
            date (Union[date, str]): The date for which to fetch data.
            airport_code (str): The airport code.
            flight_type (str): The type of flight data ('arrival' or 'departure').

        Returns:
            pd.DataFrame: A DataFrame containing the flight data.
        """
        response = cls.api_get_historical(date, flight_type, airport_code)
        if response.get("data"):
            try:
                df = pd.DataFrame.from_records(response["data"])
                df["flight_type"] = flight_type
                df["code"] = airport_code
                return df
            except Exception as e:
                logger.warning(f"Error parsing data: {e}")
        return pd.DataFrame()

    @classmethod
    def get_data_date(
        cls,
        date: Union[date, str],
        airport_code: str,
    ) -> pd.DataFrame:
        """
        Fetches both arrival and departure data for a given date and airport.

        Args:
            date (Union[date, str]): The date for which to fetch data.
            airport_code (str): The airport code.

        Returns:
            pd.DataFrame: A DataFrame containing both arrival and departure data.
        """
        df_arrival = cls.api_get_historical_data(date, airport_code, "arrival")
        df_departure = cls.api_get_historical_data(date, airport_code, "departure")
        return pd.concat((df_arrival, df_departure))

    @classmethod
    def make_filename(
        cls,
        path: str,
        airport_code: str,
        date: Union[date, str],
    ) -> str:
        """
        Generates a filename for saving the flight data.

        Args:
            date (Union[date, str]): The date for which the data is fetched.
            airport_code (str): The airport code.

        Returns:
            str: The generated filename.
        """
        airport_path = os.path.join(path, airport_code)
        os.makedirs(airport_path, mode=0o755, exist_ok=True)
        return os.path.join(airport_path, str(date).replace("-", "_") + ".csv")

    @classmethod
    def save_data_date(
        cls,
        path: str,
        airport_code: str,
        date: Union[date, str],
        overwrite: bool = False,
    ) -> None:
        """
        Saves flight data for a given date and airport to a CSV file.

        Args:
            date (Union[date, str]): The date for which to save data.
            airport_code (str): The airport code.
            overwrite (bool): Whether to overwrite the file if it already exists.
        """
        filename = cls.make_filename(path=path, date=date, airport_code=airport_code)
        if overwrite or not os.path.exists(filename):
            df = cls.get_data_date(date, airport_code)
            if df.empty:
                logger.error(f"No data for date {date}")
            logger.info(f"Saving data for date {date}")
            df.to_csv(filename, index=False)

    @classmethod
    def save_data_range(
        cls,
        path: str,
        date_start: Union[date, str],
        date_end: Union[date, str],
        airport_code: str,
        overwrite: bool = False,
    ) -> None:
        """
        Saves flight data for a range of dates and a given airport to CSV files.

        Args:
            date_start (Union[date, str]): The start date of the range.
            date_end (Union[date, str]): The end date of the range.
            airport_code (str): The airport code.
            overwrite (bool): Whether to overwrite the files if they already exist.
        """
        for single_date in pd.date_range(date_start, date_end, freq="D"):
            cls.save_data_date(
                path=path,
                date=single_date.date(),
                airport_code=airport_code,
                overwrite=overwrite,
            )
            time.sleep(1)

    @classmethod
    def update(
        cls,
        path: str,
        airport_code: str,
    ):

        last_date = max(os.listdir(os.path.join(path, airport_code))).replace(
            ".csv", ""
        )
        last_date = datetime.strptime(last_date, "%Y_%m_%d")
        to_date = str(datetime.now(timezone.utc).date())

        cls.save_data_range(
            path=path,
            date_start=last_date,
            date_end=to_date,
            overwrite=True,
            airport_code=airport_code,
        )


class FlightDataRealTime:
    api_key: str = os.environ["API_KEY_FLIGHTS"]
    url: str = "https://app.goflightlabs.com/flights"

    @classmethod
    def api_get_realtime(
        cls,
        flight_type: str,
        airport_code: str,
    ) -> dict:
        try:
            params = {"access_key": cls.api_key}
            if flight_type == "arrival":
                params.update(arrIata=airport_code)
            elif flight_type == "departure":
                params.update(depIata=airport_code)

            response = requests.get(
                cls.url,
                params=params,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return {}

    @classmethod
    def api_get_realtime_data(
        cls,
        airport_code: str,
        flight_type: str,
    ) -> pd.DataFrame:
        response = cls.api_get_realtime(flight_type, airport_code)
        if response.get("data"):
            try:
                df = pd.DataFrame.from_records(response["data"])
                df["flight_type"] = flight_type
                df["code"] = airport_code
                df["updated_timestamp"] = df["updated"].map(datetime.fromtimestamp)
                return df
            except Exception as e:
                logger.warning(f"Error parsing data: {e}")
        return pd.DataFrame()

    @classmethod
    def get_data(
        cls,
        airport_code: str,
    ) -> pd.DataFrame:
        df_arrival = cls.api_get_realtime_data(airport_code, "arrival")
        df_departure = cls.api_get_realtime_data(airport_code, "departure")
        return pd.concat((df_arrival, df_departure))
