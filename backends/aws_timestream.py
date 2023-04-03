from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Type

import boto3
from botocore.config import Config

if TYPE_CHECKING:
    from schemas import TimeSeries

from .backend import Backend


def TimeStreamType(value_type: Type) -> str:
    mapping = {
        Decimal: "DOUBLE",
        int: "BIGINT",
        str: "VARCHAR",
        bool: "BOOLEAN",
        datetime: "TIMESTAMP",
    }

    if value_type not in mapping:
        raise ValueError(f"Unknown mapping for {value_type}")

    return mapping[value_type]


class TimeStreamBackend(Backend):
    session: boto3.Session

    def __init__(self):
        self.session = boto3.Session()
        self.writer = self.session.client(
            "timestream-write",
            config=Config(
                read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
            ),
        )

    def persist(self, point: "TimeSeries") -> None:
        print("Writing records")
        data = point.data

        dimensions = [
            {"Name": dimension_name, "Value": str(dimension_value)}
            for dimension_name, dimension_value in data.dimensions.items()
        ]

        records = []

        for attribute_name, attribute_value in data.attributes.items():
            records.append(
                {
                    "Dimensions": dimensions,
                    "Time": str(int(point.timestamp.timestamp()) * 1000),
                    "MeasureName": attribute_name,
                    "MeasureValue": str(attribute_value),
                    "MeasureValueType": TimeStreamType(type(attribute_value)),
                }
            )

        try:
            result = self.writer.write_records(
                DatabaseName="toudi-test",
                TableName=point.Meta.table,
                Records=records,
                CommonAttributes={},
            )
            print(
                "WriteRecords Status: [%s]"
                % result["ResponseMetadata"]["HTTPStatusCode"]
            )
        except self.writer.exceptions.RejectedRecordsException as err:
            self._print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

    @staticmethod
    def _print_rejected_records_exceptions(err):
        print("RejectedRecords: ", err)
        for rr in err.response["RejectedRecords"]:
            print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
            if "ExistingVersion" in rr:
                print("Rejected record existing version: ", rr["ExistingVersion"])

    def query(
        self,
        cls: Type["TimeSeries"],
        dimensions: Dict[str, Any],
        start_time: datetime,
        end_time: Optional[datetime] = None,
    ) -> Iterable:
        pass
