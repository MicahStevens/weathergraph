import requests
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import time


class OpenMeteoClient:
    """Client for Open-Meteo Historical Weather API."""

    BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

    def __init__(self, max_requests_per_second: float = 2.0):
        """Initialize the client with rate limiting.

        Args:
            max_requests_per_second: Maximum requests per second to avoid hitting rate limits
        """
        self.max_requests_per_second = max_requests_per_second
        self.last_request_time = 0.0

    def _rate_limit(self):
        """Simple rate limiting to avoid overwhelming the API."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        min_interval = 1.0 / self.max_requests_per_second

        if time_since_last_request < min_interval:
            sleep_time = min_interval - time_since_last_request
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def get_historical_weather(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        variables: Optional[List[str]] = None,
    ) -> Optional[Dict]:
        """Fetch historical weather data from Open-Meteo API.

        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            variables: List of weather variables to retrieve

        Returns:
            Dictionary containing weather data or None if request fails
        """
        if variables is None:
            variables = [
                "temperature_2m_max",
                "temperature_2m_min",
                "temperature_2m_mean",
                "precipitation_sum",
                "cloud_cover_mean",
                "wind_speed_10m_max",
                "wind_direction_10m_dominant",
                "relative_humidity_2m_mean",
            ]

        self._rate_limit()

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": ",".join(variables),
            "timezone": "America/New_York",
            "temperature_unit": "fahrenheit",
            "windspeed_unit": "mph",
            "precipitation_unit": "inch",
        }

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")
            return None

    def get_weather_for_date_across_years(
        self,
        latitude: float,
        longitude: float,
        month: int,
        day: int,
        start_year: int,
        end_year: int,
    ) -> Optional[pd.DataFrame]:
        """Get weather data for a specific date (month/day) across multiple years.

        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            month: Month (1-12)
            day: Day (1-31)
            start_year: Starting year
            end_year: Ending year

        Returns:
            DataFrame with weather data for the specified date across years
        """
        all_data = []

        for year in range(start_year, end_year + 1):
            try:
                target_date = date(year, month, day)

                data = self.get_historical_weather(
                    latitude, longitude, target_date, target_date
                )

                if data and "daily" in data:
                    daily_data = data["daily"]

                    row_data = {
                        "year": year,
                        "date": target_date,
                        "latitude": latitude,
                        "longitude": longitude,
                    }

                    for var_name, values in daily_data.items():
                        if var_name != "time" and values:
                            row_data[var_name] = values[0] if len(values) > 0 else None

                    all_data.append(row_data)

            except ValueError:
                continue
            except Exception as e:
                print(f"Error fetching data for {year}-{month:02d}-{day:02d}: {e}")
                continue

        if all_data:
            df = pd.DataFrame(all_data)
            return df.sort_values("year")

        return None

    def get_weather_for_date_range_daily(
        self,
        latitude: float,
        longitude: float,
        start_month: int,
        start_day: int,
        end_month: int,
        end_day: int,
        start_year: int,
        end_year: int,
    ) -> Optional[pd.DataFrame]:
        """Get daily weather data for a date range across years (for threshold analysis).

        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            start_month: Starting month (1-12)
            start_day: Starting day (1-31)
            end_month: Ending month (1-12)
            end_day: Ending day (1-31)
            start_year: Starting year
            end_year: Ending year

        Returns:
            DataFrame with all daily weather data for the date range by year
        """
        all_data = []

        for year in range(start_year, end_year + 1):
            try:
                # Determine the actual start and end dates for this year
                start_date = date(year, start_month, start_day)

                # Handle end date (might wrap to next year if end < start)
                if end_month < start_month or (
                    end_month == start_month and end_day < start_day
                ):
                    # Range wraps across year boundary
                    end_date = date(year + 1, end_month, end_day)
                else:
                    end_date = date(year, end_month, end_day)

                # Fetch data for this range
                data = self.get_historical_weather(
                    latitude, longitude, start_date, end_date
                )

                if data and "daily" in data:
                    daily_data = data["daily"]

                    # Extract all days in this range
                    if "time" in daily_data and daily_data["time"]:
                        for idx, date_str in enumerate(daily_data["time"]):
                            row_data = {
                                "year": year,
                                "date": date_str,
                                "latitude": latitude,
                                "longitude": longitude,
                            }

                            # Add all weather variables for this day
                            for var_name, values in daily_data.items():
                                if var_name != "time" and values and idx < len(values):
                                    row_data[var_name] = values[idx]

                            all_data.append(row_data)

            except Exception as e:
                print(
                    f"Error fetching daily data for {year}-{start_month:02d}-{start_day:02d} to {end_month:02d}-{end_day:02d}: {e}"
                )
                continue

        if all_data:
            df = pd.DataFrame(all_data)
            # Convert date strings to datetime
            df["date"] = pd.to_datetime(df["date"])
            return df.sort_values(["year", "date"])

        return None

    def get_weather_for_date_range_across_years(
        self,
        latitude: float,
        longitude: float,
        start_month: int,
        start_day: int,
        end_month: int,
        end_day: int,
        start_year: int,
        end_year: int,
    ) -> Optional[pd.DataFrame]:
        """Get weather data for a date range (across months/days) and years.

        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            start_month: Starting month (1-12)
            start_day: Starting day (1-31)
            end_month: Ending month (1-12)
            end_day: Ending day (1-31)
            start_year: Starting year
            end_year: Ending year

        Returns:
            DataFrame with aggregated weather data for the date range by year
        """
        all_data = []

        for year in range(start_year, end_year + 1):
            try:
                # Determine the actual start and end dates for this year
                start_date = date(year, start_month, start_day)

                # Handle end date (might wrap to next year if end < start)
                if end_month < start_month or (
                    end_month == start_month and end_day < start_day
                ):
                    # Range wraps across year boundary
                    end_date = date(year + 1, end_month, end_day)
                else:
                    end_date = date(year, end_month, end_day)

                # Fetch data for this range
                data = self.get_historical_weather(
                    latitude, longitude, start_date, end_date
                )

                if data and "daily" in data:
                    daily_data = data["daily"]

                    # Extract all days in this range
                    if "time" in daily_data and daily_data["time"]:
                        row_data = {
                            "year": year,
                            "date_start": start_date,
                            "date_end": end_date
                            if end_date.year == year
                            else date(year, end_month, end_day),
                            "latitude": latitude,
                            "longitude": longitude,
                            "num_days": len(daily_data["time"]),
                        }

                        # Aggregate weather variables across all days in the range
                        for var_name, values in daily_data.items():
                            if var_name != "time" and values:
                                # Calculate statistics for this variable
                                valid_values = [v for v in values if v is not None]
                                if valid_values:
                                    row_data[f"{var_name}_min"] = min(valid_values)
                                    row_data[f"{var_name}_max"] = max(valid_values)
                                    row_data[f"{var_name}_mean"] = sum(
                                        valid_values
                                    ) / len(valid_values)
                                    row_data[f"{var_name}_sum"] = sum(valid_values)

                        all_data.append(row_data)

            except Exception as e:
                print(
                    f"Error fetching data for {year}-{start_month:02d}-{start_day:02d} to {end_month:02d}-{end_day:02d}: {e}"
                )
                continue

        if all_data:
            df = pd.DataFrame(all_data)
            return df.sort_values("year")

        return None

    def get_weather_for_month_across_years(
        self,
        latitude: float,
        longitude: float,
        month: int,
        start_year: int,
        end_year: int,
    ) -> Optional[pd.DataFrame]:
        """Get weather data for an entire month across multiple years.

        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            month: Month (1-12)
            start_year: Starting year
            end_year: Ending year

        Returns:
            DataFrame with weather data for the entire month across years
        """
        all_data = []

        # Determine number of days in the month (use non-leap year for consistency)
        days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        num_days = days_in_month[month - 1]

        for year in range(start_year, end_year + 1):
            try:
                # Get the entire month for this year
                start_date = date(year, month, 1)
                end_date = date(year, month, num_days)

                data = self.get_historical_weather(
                    latitude, longitude, start_date, end_date
                )

                if data and "daily" in data:
                    daily_data = data["daily"]

                    # Extract all days in this month
                    if "time" in daily_data and daily_data["time"]:
                        for idx, date_str in enumerate(daily_data["time"]):
                            row_data = {
                                "year": year,
                                "date": date_str,
                                "latitude": latitude,
                                "longitude": longitude,
                            }

                            # Add all weather variables for this day
                            for var_name, values in daily_data.items():
                                if var_name != "time" and values and idx < len(values):
                                    row_data[var_name] = values[idx]

                            all_data.append(row_data)

            except Exception as e:
                print(f"Error fetching data for {year}-{month:02d}: {e}")
                continue

        if all_data:
            df = pd.DataFrame(all_data)
            # Convert date strings to datetime
            df["date"] = pd.to_datetime(df["date"])
            return df.sort_values(["year", "date"])

        return None

    def get_weather_dataframe(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        variables: Optional[List[str]] = None,
    ) -> Optional[pd.DataFrame]:
        """Get weather data as a pandas DataFrame.

        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            variables: List of weather variables to retrieve

        Returns:
            DataFrame with weather data or None if request fails
        """
        data = self.get_historical_weather(
            latitude, longitude, start_date, end_date, variables
        )

        if not data or "daily" not in data:
            return None

        daily_data = data["daily"]

        if "time" not in daily_data:
            return None

        df_data = {}

        for var_name, values in daily_data.items():
            df_data[var_name] = values

        df = pd.DataFrame(df_data)

        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"])
            df = df.set_index("time")

        df["latitude"] = latitude
        df["longitude"] = longitude

        return df


def get_weather_for_zip_and_date(
    zip_code: str, month: int, day: int, start_year: int, end_year: int
) -> Optional[pd.DataFrame]:
    """Convenience function to get weather data for a zip code and specific date across years.

    Args:
        zip_code: US postal code
        month: Month (1-12)
        day: Day (1-31)
        start_year: Starting year
        end_year: Ending year

    Returns:
        DataFrame with weather data or None if fails
    """
    from src.api.geocoding import get_coordinates_for_zip

    coords = get_coordinates_for_zip(zip_code)
    if not coords:
        print(f"Could not find coordinates for zip code: {zip_code}")
        return None

    lat, lon = coords
    client = OpenMeteoClient()

    return client.get_weather_for_date_across_years(
        lat, lon, month, day, start_year, end_year
    )
