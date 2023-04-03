from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Type

if TYPE_CHECKING:
    from schemas import TimeSeries


class Backend:
    def persist(self, point: "TimeSeries") -> None:
        raise NotImplementedError(
            f"Please implement persist() method on {self.__class__.__name__}"
        )

    def query(
        self,
        cls: Type["TimeSeries"],
        dimensions: Dict[str, Any],
        start_time: datetime,
        end_time: Optional[datetime] = None,
    ) -> Iterable["TimeSeries"]:
        raise NotImplementedError(
            f"Please implement query() method on {self.__class__.__name__}"
        )

    def commit(self) -> None:
        pass
