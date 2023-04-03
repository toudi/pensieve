from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Type

from storage import Storage
from pydantic import BaseModel


@dataclass
class Data:
    timestamp: datetime
    dimensions: Dict[str, Any]
    attributes: Dict[str, Any]


class TimeSeries(BaseModel):
    timestamp: datetime

    @classmethod
    def filter(
        cls,
        dimensions: Dict[str, Any],
        start_time: datetime,
        end_time: Optional[datetime] = None,
    ) -> Iterable["TimeSeries"]:
        storage = Storage()

        print("filtering")

        for point in storage.backend.query(
            cls=cls,
            dimensions=dimensions,
            start_time=start_time,
            end_time=end_time,
        ):
            yield cls(**point)

    @property
    def data(self):
        values = self.dict()
        timestamp = values.pop("timestamp")

        data = Data(timestamp=timestamp, dimensions={}, attributes={})
        for dimension_name in self.Meta.dimensions:
            dimension_value = values.pop(dimension_name)
            data.dimensions[dimension_name] = dimension_value
        data.attributes = values

        return data

    class Meta:
        table: str
        dimensions: List[str]
