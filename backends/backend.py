from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Type

if TYPE_CHECKING:
    from schemas import TimeSeries


class Backend:
    def prepare_type(self, data_type: Type["TimeSeries"]) -> None:
        """In this method, the backend may inspect the data type and prepare
        some underlying structures."""
        pass

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
