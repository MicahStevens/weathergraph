from sqlalchemy import create_engine, Column, Integer, Float, String, Date, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json

Base = declarative_base()


class WeatherData(Base):
    """SQLAlchemy model for storing weather data."""
    
    __tablename__ = 'weather_data'
    
    id = Column(Integer, primary_key=True)
    zip_code = Column(String(10), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    date = Column(Date, nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    
    # Temperature data (Fahrenheit)
    temperature_2m_max = Column(Float)
    temperature_2m_min = Column(Float)
    temperature_2m_mean = Column(Float)
    
    # Precipitation (inches)
    precipitation_sum = Column(Float)
    
    # Other weather data
    cloud_cover_mean = Column(Float)
    wind_speed_10m_max = Column(Float)
    wind_direction_10m_dominant = Column(Float)
    relative_humidity_2m_mean = Column(Float)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    raw_data = Column(Text)  # Store original API response as JSON
    
    def to_dict(self):
        """Convert model instance to dictionary."""
        return {
            'id': self.id,
            'zip_code': self.zip_code,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'date': self.date.isoformat() if self.date else None,
            'year': self.year,
            'month': self.month,
            'day': self.day,
            'temperature_2m_max': self.temperature_2m_max,
            'temperature_2m_min': self.temperature_2m_min,
            'temperature_2m_mean': self.temperature_2m_mean,
            'precipitation_sum': self.precipitation_sum,
            'cloud_cover_mean': self.cloud_cover_mean,
            'wind_speed_10m_max': self.wind_speed_10m_max,
            'wind_direction_10m_dominant': self.wind_direction_10m_dominant,
            'relative_humidity_2m_mean': self.relative_humidity_2m_mean,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }


class LocationCache(Base):
    """SQLAlchemy model for caching zip code to coordinates lookups."""
    
    __tablename__ = 'location_cache'
    
    id = Column(Integer, primary_key=True)
    zip_code = Column(String(10), unique=True, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    place_name = Column(String(100))
    state_name = Column(String(50))
    state_code = Column(String(2))
    county_name = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    def to_dict(self):
        """Convert model instance to dictionary."""
        return {
            'zip_code': self.zip_code,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'place_name': self.place_name,
            'state_name': self.state_name,
            'state_code': self.state_code,
            'county_name': self.county_name,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }


def create_database_engine(db_path: str):
    """Create SQLAlchemy engine and setup tables."""
    engine = create_engine(f'sqlite:///{db_path}', echo=False)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return engine, SessionLocal