import os
import json
import pandas as pd
import sys
from pathlib import Path
from datetime import date, datetime
from typing import Optional, List, Dict, Tuple
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from .models import WeatherData, LocationCache, create_database_engine
from src.api.geocoding import Geocoder
from src.api.weather_client import OpenMeteoClient


class WeatherCache:
    """Manages caching of weather data and location information."""

    def __init__(self, db_path: str = "data/weather_cache.db"):
        """Initialize the cache manager.

        Args:
            db_path: Path to SQLite database file
        """
        # Ensure data directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        self.db_path = db_path
        self.engine, self.SessionLocal = create_database_engine(db_path)
        self.geocoder = Geocoder()
        self.weather_client = OpenMeteoClient()

    def get_or_cache_location(self, zip_code: str) -> Optional[Tuple[float, float]]:
        """Get coordinates for zip code, using cache or fetching if needed.

        Args:
            zip_code: US postal code

        Returns:
            Tuple of (latitude, longitude) or None if not found
        """
        session = self.SessionLocal()
        try:
            # Check cache first
            cached = session.query(LocationCache).filter_by(zip_code=zip_code).first()
            if cached:
                return cached.latitude, cached.longitude

            # Fetch from geocoder
            location_info = self.geocoder.get_location_info(zip_code)
            if not location_info or not location_info.get("latitude"):
                return None

            # Cache the result
            cache_entry = LocationCache(
                zip_code=zip_code,
                latitude=location_info["latitude"],
                longitude=location_info["longitude"],
                place_name=location_info.get("place_name"),
                state_name=location_info.get("state_name"),
                state_code=location_info.get("state_code"),
                county_name=location_info.get("county_name"),
            )
            session.add(cache_entry)
            session.commit()

            return location_info["latitude"], location_info["longitude"]

        finally:
            session.close()

    def get_cached_weather_data(
        self, zip_code: str, month: int, day: int, start_year: int, end_year: int
    ) -> Optional[pd.DataFrame]:
        """Get cached weather data for specific date across years.

        Args:
            zip_code: US postal code
            month: Month (1-12)
            day: Day (1-31)
            start_year: Starting year
            end_year: Ending year

        Returns:
            DataFrame with cached data or None if not fully cached
        """
        session = self.SessionLocal()
        try:
            cached_data = (
                session.query(WeatherData)
                .filter(
                    and_(
                        WeatherData.zip_code == zip_code,
                        WeatherData.month == month,
                        WeatherData.day == day,
                        WeatherData.year >= start_year,
                        WeatherData.year <= end_year,
                    )
                )
                .order_by(WeatherData.year)
                .all()
            )

            # Check if we have data for all requested years
            cached_years = {item.year for item in cached_data}
            requested_years = set(range(start_year, end_year + 1))

            if not cached_years.issuperset(requested_years):
                return None  # Missing some years, need to fetch from API

            # Convert to DataFrame
            data_dicts = [item.to_dict() for item in cached_data]
            return pd.DataFrame(data_dicts)

        finally:
            session.close()

    def cache_weather_data(
        self, zip_code: str, latitude: float, longitude: float, df: pd.DataFrame
    ):
        """Cache weather data to database.

        Args:
            zip_code: US postal code
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            df: DataFrame with weather data to cache
        """
        session = self.SessionLocal()
        try:
            for _, row in df.iterrows():
                # Check if entry already exists
                existing = (
                    session.query(WeatherData)
                    .filter(
                        and_(
                            WeatherData.zip_code == zip_code,
                            WeatherData.year == row["year"],
                            WeatherData.month == row["date"].month,
                            WeatherData.day == row["date"].day,
                        )
                    )
                    .first()
                )

                if existing:
                    continue  # Skip if already cached

                # Create new cache entry
                weather_entry = WeatherData(
                    zip_code=zip_code,
                    latitude=latitude,
                    longitude=longitude,
                    date=row["date"],
                    year=row["year"],
                    month=row["date"].month,
                    day=row["date"].day,
                    temperature_2m_max=row.get("temperature_2m_max"),
                    temperature_2m_min=row.get("temperature_2m_min"),
                    temperature_2m_mean=row.get("temperature_2m_mean"),
                    precipitation_sum=row.get("precipitation_sum"),
                    cloud_cover_mean=row.get("cloud_cover_mean"),
                    wind_speed_10m_max=row.get("wind_speed_10m_max"),
                    wind_direction_10m_dominant=row.get("wind_direction_10m_dominant"),
                    relative_humidity_2m_mean=row.get("relative_humidity_2m_mean"),
                    raw_data=json.dumps(row.to_dict(), default=str),
                )
                session.add(weather_entry)

            session.commit()

        finally:
            session.close()

    def get_weather_data_for_date_range_daily(
        self,
        zip_code: str,
        start_month: int,
        start_day: int,
        end_month: int,
        end_day: int,
        start_year: int,
        end_year: int,
        use_cache: bool = True,
    ) -> Optional[pd.DataFrame]:
        """Get daily weather data for a date range across years (for threshold analysis).

        Args:
            zip_code: US postal code
            start_month: Starting month (1-12)
            start_day: Starting day (1-31)
            end_month: Ending month (1-12)
            end_day: Ending day (1-31)
            start_year: Starting year
            end_year: Ending year
            use_cache: Whether to use cached data

        Returns:
            DataFrame with daily weather data for the date range
        """
        # Get coordinates
        coords = self.get_or_cache_location(zip_code)
        if not coords:
            print(f"Could not find coordinates for zip code: {zip_code}")
            return None

        lat, lon = coords

        # Fetch from API
        print(
            f"Fetching daily weather data from API for {zip_code} date range {start_month:02d}-{start_day:02d} to {end_month:02d}-{end_day:02d}"
        )
        df = self.weather_client.get_weather_for_date_range_daily(
            lat, lon, start_month, start_day, end_month, end_day, start_year, end_year
        )

        # Cache the daily data
        if df is not None and not df.empty and use_cache:
            self.cache_weather_data(zip_code, lat, lon, df)
            print(f"Cached {len(df)} daily records for {zip_code}")

        return df

    def get_weather_data_for_date_range(
        self,
        zip_code: str,
        start_month: int,
        start_day: int,
        end_month: int,
        end_day: int,
        start_year: int,
        end_year: int,
        use_cache: bool = True,
    ) -> Optional[pd.DataFrame]:
        """Get aggregated weather data for a date range across years.

        Args:
            zip_code: US postal code
            start_month: Starting month (1-12)
            start_day: Starting day (1-31)
            end_month: Ending month (1-12)
            end_day: Ending day (1-31)
            start_year: Starting year
            end_year: Ending year
            use_cache: Whether to use cached data

        Returns:
            DataFrame with aggregated weather data for the date range
        """
        # Get coordinates
        coords = self.get_or_cache_location(zip_code)
        if not coords:
            print(f"Could not find coordinates for zip code: {zip_code}")
            return None

        lat, lon = coords

        # Fetch aggregated data from API (derived from daily data)
        print(
            f"Fetching weather data from API for {zip_code} date range {start_month:02d}-{start_day:02d} to {end_month:02d}-{end_day:02d}"
        )
        df = self.weather_client.get_weather_for_date_range_across_years(
            lat, lon, start_month, start_day, end_month, end_day, start_year, end_year
        )

        return df

    def get_weather_data_for_month(
        self,
        zip_code: str,
        month: int,
        start_year: int,
        end_year: int,
        use_cache: bool = True,
    ) -> Optional[pd.DataFrame]:
        """Get weather data for entire month across years, with intelligent caching.

        Args:
            zip_code: US postal code
            month: Month (1-12)
            start_year: Starting year
            end_year: Ending year
            use_cache: Whether to use cached data

        Returns:
            DataFrame with all days' weather data for the month or None if failed
        """
        # Get coordinates
        coords = self.get_or_cache_location(zip_code)
        if not coords:
            print(f"Could not find coordinates for zip code: {zip_code}")
            return None

        lat, lon = coords

        # Fetch from API
        print(f"Fetching weather data from API for {zip_code} month {month:02d}")
        df = self.weather_client.get_weather_for_month_across_years(
            lat, lon, month, start_year, end_year
        )

        return df

    def get_weather_data_for_date(
        self,
        zip_code: str,
        month: int,
        day: int,
        start_year: int,
        end_year: int,
        use_cache: bool = True,
    ) -> Optional[pd.DataFrame]:
        """Get weather data for specific date across years, with intelligent caching.

        Args:
            zip_code: US postal code
            month: Month (1-12)
            day: Day (1-31)
            start_year: Starting year
            end_year: Ending year
            use_cache: Whether to use cached data

        Returns:
            DataFrame with weather data or None if failed
        """
        # Try cache first if enabled
        if use_cache:
            cached_df = self.get_cached_weather_data(
                zip_code, month, day, start_year, end_year
            )
            if cached_df is not None:
                print(f"Using cached data for {zip_code} {month:02d}-{day:02d}")
                return cached_df

        # Get coordinates
        coords = self.get_or_cache_location(zip_code)
        if not coords:
            print(f"Could not find coordinates for zip code: {zip_code}")
            return None

        lat, lon = coords

        # Fetch from API
        print(f"Fetching weather data from API for {zip_code} {month:02d}-{day:02d}")
        df = self.weather_client.get_weather_for_date_across_years(
            lat, lon, month, day, start_year, end_year
        )

        if df is not None and not df.empty:
            # Cache the results
            if use_cache:
                self.cache_weather_data(zip_code, lat, lon, df)

            return df

        return None

    def clear_cache(self, zip_code: str = None):
        """Clear cached data.

        Args:
            zip_code: If provided, clear only data for this zip code.
                     If None, clear all cached data.
        """
        session = self.SessionLocal()
        try:
            if zip_code:
                session.query(WeatherData).filter_by(zip_code=zip_code).delete()
                session.query(LocationCache).filter_by(zip_code=zip_code).delete()
            else:
                session.query(WeatherData).delete()
                session.query(LocationCache).delete()

            session.commit()

        finally:
            session.close()

    def get_cache_stats(self) -> Dict:
        """Get statistics about cached data."""
        session = self.SessionLocal()
        try:
            weather_count = session.query(WeatherData).count()
            location_count = session.query(LocationCache).count()

            # Get unique zip codes
            unique_zips = session.query(WeatherData.zip_code).distinct().count()

            # Get date range
            min_year = (
                session.query(WeatherData.year).order_by(WeatherData.year.asc()).first()
            )
            max_year = (
                session.query(WeatherData.year)
                .order_by(WeatherData.year.desc())
                .first()
            )

            return {
                "total_weather_records": weather_count,
                "total_locations_cached": location_count,
                "unique_zip_codes": unique_zips,
                "year_range": {
                    "min_year": min_year[0] if min_year else None,
                    "max_year": max_year[0] if max_year else None,
                },
                "database_path": self.db_path,
            }

        finally:
            session.close()
