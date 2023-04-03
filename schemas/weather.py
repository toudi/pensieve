from schema import TimeSeries
from pydantic import Field
from decimal import Decimal
from enum import Enum


class Description(Enum):
    SUNNY = 0
    CLOUDY = 1
    SNOWY = 2


class Weather(TimeSeries):
    city: str = Field(max_length=32)
    temperature: Decimal = Field(max_digits=10, decimal_places=2)
    rainfall: int
    description: Description

    class Meta:
        dimensions = ["city"]
        table = "weather"

    def __repr__(self) -> str:
        return f"Weather at {self.timestamp}: {self.description}; {self.temperature} deg C; rainfall: {self.rainfall}"
