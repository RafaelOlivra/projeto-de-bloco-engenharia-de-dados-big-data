from pydantic import BaseModel, Field, field_validator
from datetime import datetime, date
from typing import Literal, Union
from lib import Utils

class AirQualityAnomalyRecord(BaseModel):
    uf: str
    city: str
    timestamp: datetime
    parameter: str
    value: float
    unit: str
    avg_value: float
    std_value: float
    aqi_z_score: float
    anomaly_level: Union[Literal["extrema", "severa", "moderada"], None] = None
    Q1: float
    Q3: float
    IQR: float
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
