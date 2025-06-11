from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime, date

from lib.Utils import Utils

class WeatherRecord(BaseModel):
    uf: str
    city: str
    timestamp: datetime
    interval: int  # seconds
    is_day: int  # 1 or 0
    temperature: float
    weathercode: int
    winddirection: int
    windspeed: float
    lat: float = Field(..., alias="lat")
    long: float = Field(..., alias="long")

    @field_validator("timestamp")
    def convert_to_datetime(cls, value):
        """Convert date to datetime if necessary."""
        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime(value.year, value.month, value.day)
        if isinstance(value, str):
            return Utils.to_datetime(value)
        return value