import pgeocode
from typing import Tuple, Optional


class Geocoder:
    def __init__(self):
        self.nomi = pgeocode.Nominatim('us')
    
    def zip_to_coordinates(self, zip_code: str) -> Optional[Tuple[float, float]]:
        """Convert US zip code to latitude and longitude coordinates.
        
        Args:
            zip_code: US postal code as string
            
        Returns:
            Tuple of (latitude, longitude) or None if zip code not found
        """
        try:
            result = self.nomi.query_postal_code(zip_code)
            
            if result is not None and not result.isna().all():
                lat = result.latitude
                lon = result.longitude
                
                if not (lat is None or lon is None or 
                       (hasattr(lat, 'isna') and lat.isna()) or
                       (hasattr(lon, 'isna') and lon.isna())):
                    return float(lat), float(lon)
            
            return None
            
        except Exception as e:
            print(f"Error geocoding zip code {zip_code}: {e}")
            return None
    
    def get_location_info(self, zip_code: str) -> Optional[dict]:
        """Get detailed location information for a zip code.
        
        Args:
            zip_code: US postal code as string
            
        Returns:
            Dictionary with location details or None if zip code not found
        """
        try:
            result = self.nomi.query_postal_code(zip_code)
            
            if result is not None and not result.isna().all():
                return {
                    'zip_code': zip_code,
                    'latitude': float(result.latitude) if result.latitude is not None else None,
                    'longitude': float(result.longitude) if result.longitude is not None else None,
                    'place_name': result.place_name if hasattr(result, 'place_name') else None,
                    'state_name': result.state_name if hasattr(result, 'state_name') else None,
                    'state_code': result.state_code if hasattr(result, 'state_code') else None,
                    'county_name': result.county_name if hasattr(result, 'county_name') else None
                }
            
            return None
            
        except Exception as e:
            print(f"Error getting location info for zip code {zip_code}: {e}")
            return None


def get_coordinates_for_zip(zip_code: str) -> Optional[Tuple[float, float]]:
    """Convenience function to get coordinates for a zip code.
    
    Args:
        zip_code: US postal code as string
        
    Returns:
        Tuple of (latitude, longitude) or None if zip code not found
    """
    geocoder = Geocoder()
    return geocoder.zip_to_coordinates(zip_code)