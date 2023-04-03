from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional, Type

if TYPE_CHECKING:
    from schemas import TimeSeries

from .backend import Backend


class PrintBackend(Backend):
    def persist(self, point: "TimeSeries") -> None:
        data = point.dict()
        print(
            f"INSERT INTO {point.Meta.table} ({', '.join(list(data.keys()))}) VALUES ({', '.join(map(str, data.values()))})"
        )

    def query(
        self,
        cls: Type["TimeSeries"],
        dimensions: Dict[str, Any],
        start_time: datetime,
        end_time: Optional[datetime] = None,
    ):
        whereClause = []
        for dimension, value in dimensions.items():
            whereClause.append(f"{dimension} = {value}")
        whereClause.append(f"timestamp >= {start_time}")
        if end_time:
            whereClause.append(f"timestamp <= {end_time}")
        print(f"SELECT * FROM {cls.Meta.table} WHERE ({' AND '.join(whereClause)})")
        return []
