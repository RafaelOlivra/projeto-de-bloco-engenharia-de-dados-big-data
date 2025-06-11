from pydantic import BaseModel, Field, field_validator
from datetime import datetime, date
from typing import Literal
from lib import Utils

class AirQualityRecord(BaseModel):
    uf: str
    city: str
    timestamp: datetime
    parameter: str
    value: float
    unit: Literal["AQI"]
    source: Literal["WAQI"]
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
