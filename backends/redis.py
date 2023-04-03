# note: this module requires redis server with timeseries extension.
# you can launch it from docker like so:
# docker run -p 6379:6379 redis/redis-stack-server:latest
# I think that the docker image limits the number of data you can
# put in but for testing purposes it's more then enough.
from collections import defaultdict
from datetime import datetime
from operator import itemgetter
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Type

from redis import Redis

if TYPE_CHECKING:
    from schemas import TimeSeries

from .backend import Backend


class RedisBackend(Backend):
    def persist(self, point: "TimeSeries") -> None:
        connection = Redis()
        data = point.data
        timestamp_ms = int(data.timestamp.timestamp() * 1000)

        # let's create a key suffix per dimension
        for dimension_name, dimension_value in data.dimensions.items():
            labels = {}
            labels[dimension_name] = dimension_value
            # now let's create a key per attribute
            for attribute_name, attribute_value in data.attributes.items():
                labels["attribute"] = attribute_name
                key_name = f"{point.Meta.table}:{attribute_name}:{dimension_name}-{dimension_value}"

                connection.ts().add(
                    key=key_name,
                    timestamp=timestamp_ms,
                    labels=labels,
                    value=attribute_value,
                )

    def query(
        self,
        cls: Type["TimeSeries"],
        dimensions: Dict[str, Any],
        start_time: datetime,
        end_time: Optional[datetime] = None,
    ) -> Iterable:
        connection = Redis()
        filters = []

        # note: we can make this way more efficient if we ask for just a single
        # property, because then we don't need to reconstruct the objects, we
        # can simply return the data as it comes from redis.

        output_values = defaultdict(dict)
        output = []

        for dimension_name, dimension_value in dimensions.items():
            filters.append(f"{dimension_name}={dimension_value}")

            for item in connection.ts().mrange(
                from_time=(max(int(start_time.timestamp() * 1000), 0) or "-"),
                to_time=(
                    int(end_time.timestamp() * 1000) if end_time is not None else "+"
                ),
                with_labels=True,
                filters=filters,
            ):
                # _ is the key name which we don't need.
                for _, values in item.items():
                    labels, points = values
                    for point in points:
                        timestamp, attrib_value = point

                        output_values[timestamp][labels["attribute"]] = attrib_value
                        output_values[timestamp][dimension_name] = dimension_value

            for timestamp, values in output_values.items():
                values["timestamp"] = timestamp
                output.append(values)

            output.sort(key=itemgetter("timestamp"))

            return output
