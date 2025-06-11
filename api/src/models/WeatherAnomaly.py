from pydantic import BaseModel, Field, field_validator
from typing import Literal, Union
from datetime import datetime, date

from lib.Utils import Utils

class WeatherAnomalyRecord(BaseModel):
    uf: str
    city: str
    timestamp: datetime
    temperature: float
    avg_temp: float
    temp_z_score: float
    anomaly_level: Union[Literal["extrema", "severa", "moderada"], None] = None
    is_iqr_anomaly: bool
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
