import eel
import os
import sys
from pathlib import Path

# Add project root to path so we can import our modules
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.cache.cache_manager import WeatherCache


class WeatherApp:
    """Main application class for the Weather Data Analyzer."""

    def __init__(self):
        self.cache = WeatherCache()

    def setup_eel(self):
        """Initialize Eel with web folder."""
        web_folder = Path(__file__).parent.parent / "web"
        eel.init(str(web_folder))

        # Register Python functions that can be called from JavaScript
        eel.expose(self.get_weather_data)
        eel.expose(self.get_weather_data_for_date_range)
        eel.expose(self.get_weather_data_for_month)
        eel.expose(self.get_cache_stats)
        eel.expose(self.clear_cache)
        eel.expose(self.get_location_info)

    def get_weather_data(
        self,
        zip_code: str,
        start_month: int,
        start_day: int,
        end_month: int,
        end_day: int,
        start_year: int,
        end_year: int,
    ):
        """Get weather data for a date range (both aggregated and daily).

        Args:
            zip_code: US postal code
            start_month: Starting month (1-12)
            start_day: Starting day (1-31)
            end_month: Ending month (1-12)
            end_day: Ending day (1-31)
            start_year: Starting year
            end_year: Ending year

        Returns:
            Dictionary with success status, aggregated data, and daily data for threshold analysis
        """
        try:
            print(
                f"Fetching weather data for {zip_code} from {start_month:02d}-{start_day:02d} to {end_month:02d}-{end_day:02d} from {start_year} to {end_year}"
            )

            # Get aggregated weather data for plotting
            df_aggregated = self.cache.get_weather_data_for_date_range(
                zip_code,
                start_month,
                start_day,
                end_month,
                end_day,
                start_year,
                end_year,
            )

            if df_aggregated is None or df_aggregated.empty:
                return {
                    "success": False,
                    "error": f"No weather data found for ZIP code {zip_code}",
                }

            # Get daily data for threshold analysis
            df_daily = self.cache.get_weather_data_for_date_range_daily(
                zip_code,
                start_month,
                start_day,
                end_month,
                end_day,
                start_year,
                end_year,
            )

            # Get location name for display
            coords = self.cache.get_or_cache_location(zip_code)
            location_info = None
            if coords:
                # Try to get cached location info
                location_info = self.cache.geocoder.get_location_info(zip_code)

            location_name = None
            if location_info:
                parts = []
                if location_info.get("place_name"):
                    parts.append(location_info["place_name"])
                if location_info.get("state_code"):
                    parts.append(location_info["state_code"])
                location_name = ", ".join(parts) if parts else None

            # Convert DataFrames to list of dictionaries for JSON serialization
            aggregated_data = df_aggregated.to_dict("records")
            daily_data = df_daily.to_dict("records") if df_daily is not None else []

            # Debug: print sample of daily data
            if daily_data:
                print(f"Sample daily record (first record): {daily_data[0]}")

            # Convert any NaN values to None for JSON serialization
            for records in [aggregated_data, daily_data]:
                for record in records:
                    for key, value in record.items():
                        if value != value:  # Check for NaN
                            record[key] = None
                        elif hasattr(value, "isoformat"):  # Convert dates to strings
                            record[key] = value.isoformat()

            print(
                f"Successfully fetched {len(aggregated_data)} aggregated records and {len(daily_data)} daily records"
            )

            return {
                "success": True,
                "data": aggregated_data,
                "daily_data": daily_data,
                "location_name": location_name,
                "record_count": len(aggregated_data),
            }

        except Exception as e:
            print(f"Error fetching weather data: {e}")
            return {"success": False, "error": str(e)}

            # Get location name for display
            coords = self.cache.get_or_cache_location(zip_code)
            location_info = None
            if coords:
                # Try to get cached location info
                location_info = self.cache.geocoder.get_location_info(zip_code)

            location_name = None
            if location_info:
                parts = []
                if location_info.get("place_name"):
                    parts.append(location_info["place_name"])
                if location_info.get("state_code"):
                    parts.append(location_info["state_code"])
                location_name = ", ".join(parts) if parts else None

            # Convert DataFrame to list of dictionaries for JSON serialization
            data = df.to_dict("records")

            # Convert any NaN values to None for JSON serialization
            for record in data:
                for key, value in record.items():
                    if value != value:  # Check for NaN
                        record[key] = None
                    elif hasattr(value, "isoformat"):  # Convert dates to strings
                        record[key] = value.isoformat()

            print(f"Successfully fetched {len(data)} records")

            return {
                "success": True,
                "data": data,
                "location_name": location_name,
                "record_count": len(data),
            }

        except Exception as e:
            print(f"Error fetching weather data: {e}")
            return {"success": False, "error": str(e)}

    def get_weather_data_for_date_range(
        self,
        zip_code: str,
        start_month: int,
        start_day: int,
        end_month: int,
        end_day: int,
        start_year: int,
        end_year: int,
    ):
        """Get weather data for entire date range across years.

        Args:
            zip_code: US postal code
            start_month: Starting month (1-12)
            start_day: Starting day (1-31)
            end_month: Ending month (1-12)
            end_day: Ending day (1-31)
            start_year: Starting year
            end_year: Ending year

        Returns:
            Dictionary with success status and data or error message
        """
        try:
            print(
                f"Fetching weather data for {zip_code} date range {start_month:02d}-{start_day:02d} to {end_month:02d}-{end_day:02d} from {start_year} to {end_year}"
            )

            # Get weather data using cache manager
            df = self.cache.get_weather_data_for_date_range(
                zip_code,
                start_month,
                start_day,
                end_month,
                end_day,
                start_year,
                end_year,
            )

            if df is None or df.empty:
                return {
                    "success": False,
                    "error": f"No weather data found for ZIP code {zip_code}",
                }

            # Get location name for display
            coords = self.cache.get_or_cache_location(zip_code)
            location_info = None
            if coords:
                # Try to get cached location info
                location_info = self.cache.geocoder.get_location_info(zip_code)

            location_name = None
            if location_info:
                parts = []
                if location_info.get("place_name"):
                    parts.append(location_info["place_name"])
                if location_info.get("state_code"):
                    parts.append(location_info["state_code"])
                location_name = ", ".join(parts) if parts else None

            # Convert DataFrame to list of dictionaries for JSON serialization
            data = df.to_dict("records")

            # Convert any NaN values to None for JSON serialization
            for record in data:
                for key, value in record.items():
                    if value != value:  # Check for NaN
                        record[key] = None
                    elif hasattr(value, "isoformat"):  # Convert dates to strings
                        record[key] = value.isoformat()

            print(f"Successfully fetched {len(data)} records for date range")

            return {
                "success": True,
                "data": data,
                "location_name": location_name,
                "record_count": len(data),
            }

        except Exception as e:
            print(f"Error fetching weather data: {e}")
            return {"success": False, "error": str(e)}

    def get_weather_data_for_month(
        self, zip_code: str, month: int, start_year: int, end_year: int
    ):
        """Get weather data for entire month across years.

        Args:
            zip_code: US postal code
            month: Month (1-12)
            start_year: Starting year
            end_year: Ending year

        Returns:
            Dictionary with success status and data or error message
        """
        try:
            print(
                f"Fetching weather data for {zip_code} month {month:02d} from {start_year} to {end_year}"
            )

            # Get weather data using cache manager
            df = self.cache.get_weather_data_for_month(
                zip_code, month, start_year, end_year
            )

            if df is None or df.empty:
                return {
                    "success": False,
                    "error": f"No weather data found for ZIP code {zip_code}",
                }

            # Get location name for display
            coords = self.cache.get_or_cache_location(zip_code)
            location_info = None
            if coords:
                # Try to get cached location info
                location_info = self.cache.geocoder.get_location_info(zip_code)

            location_name = None
            if location_info:
                parts = []
                if location_info.get("place_name"):
                    parts.append(location_info["place_name"])
                if location_info.get("state_code"):
                    parts.append(location_info["state_code"])
                location_name = ", ".join(parts) if parts else None

            # Convert DataFrame to list of dictionaries for JSON serialization
            data = df.to_dict("records")

            # Convert any NaN values to None for JSON serialization
            for record in data:
                for key, value in record.items():
                    if value != value:  # Check for NaN
                        record[key] = None
                    elif hasattr(value, "isoformat"):  # Convert dates to strings
                        record[key] = value.isoformat()

            print(f"Successfully fetched {len(data)} records for month")

            return {
                "success": True,
                "data": data,
                "location_name": location_name,
                "record_count": len(data),
            }

        except Exception as e:
            print(f"Error fetching weather data: {e}")
            return {"success": False, "error": str(e)}

    def get_cache_stats(self):
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        try:
            return self.cache.get_cache_stats()
        except Exception as e:
            print(f"Error getting cache stats: {e}")
            return None

    def clear_cache(self, zip_code: str = None):
        """Clear cached data.

        Args:
            zip_code: Optional ZIP code to clear specific data

        Returns:
            Dictionary with success status
        """
        try:
            self.cache.clear_cache(zip_code)
            return {"success": True}
        except Exception as e:
            print(f"Error clearing cache: {e}")
            return {"success": False, "error": str(e)}

    def get_location_info(self, zip_code: str):
        """Get location information for a ZIP code.

        Args:
            zip_code: US postal code

        Returns:
            Dictionary with location information or error
        """
        try:
            coords = self.cache.get_or_cache_location(zip_code)
            if not coords:
                return {
                    "success": False,
                    "error": f"Could not find location for ZIP code {zip_code}",
                }

            location_info = self.cache.geocoder.get_location_info(zip_code)
            if location_info:
                return {"success": True, "data": location_info}
            else:
                return {
                    "success": True,
                    "data": {
                        "zip_code": zip_code,
                        "latitude": coords[0],
                        "longitude": coords[1],
                    },
                }

        except Exception as e:
            print(f"Error getting location info: {e}")
            return {"success": False, "error": str(e)}

    def run(self, debug: bool = False, port: int = 8000):
        """Start the Eel application.

        Args:
            debug: Whether to run in debug mode
            port: Port to run the application on
        """
        self.setup_eel()

        print("Starting Weather Data Analyzer...")
        print(f"Web interface will be available at: http://localhost:{port}")
        print("Close the browser window to exit the application.")

        # Start Eel application
        try:
            eel.start("index.html", size=(1200, 900), port=port)
        except (SystemExit, MemoryError, KeyboardInterrupt):
            print("\nApplication closed.")
        except Exception as e:
            print(f"Error starting application: {e}")


def main():
    """Main entry point for the application."""
    app = WeatherApp()

    # Check for debug flag
    debug = "--debug" in sys.argv

    # Check for custom port
    port = 8000
    for arg in sys.argv:
        if arg.startswith("--port="):
            try:
                port = int(arg.split("=")[1])
            except ValueError:
                print("Invalid port number, using default 8000")

    app.run(debug=debug, port=port)


if __name__ == "__main__":
    main()
