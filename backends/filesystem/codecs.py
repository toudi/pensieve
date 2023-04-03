from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Type

if TYPE_CHECKING:
    from pydantic.fields import FieldInfo, ModelField


class Codec:
    """The codec struct is used for encoding raw python values into more
    primitive types.

    For instance, the high-level python data type might be a string, but
    because the strings are variadic in length we can use pydantic's
    annotation to our advantage and create a fixed-size binary field.

    then the codec's encode() method will take this string as input and
    return the same string encoded to utf-8, which would be sufficient
    to pass it to struct.fmt

    decode() method does the exact reverse. meaning - it takes this
    sequence of bytes, does decode("utf-8") and returns a python string.
    """

    # pydantic's ModelField, which contains field name itself
    model_field: "ModelField"
    # pydantic's FieldInfo, which contains carious annotations about the field
    field_info: "FieldInfo"

    fmt: str

    def __init__(self, model_field: "ModelField", field_info: "FieldInfo") -> None:
        self.model_field = model_field
        self.field_info = field_info
        self.init()

    def init(self) -> None:
        pass

    def encode(self, value: Any) -> Any:
        """The default implementation does not touch the value"""
        return value

    def decode(self, value: Any) -> Any:
        """The default implementation does not touch the value"""
        return value


class StringCodec(Codec):
    def init(self) -> str:
        self.fmt = f"{self.field_info.max_length}s"

    def encode(self, value: str) -> bytes:
        return value.encode("utf-8")

    def decode(self, value: bytes) -> str:
        return value.replace(b"\x00", b"").decode("utf-8")


class DecimalCodec(Codec):
    def init(self):
        self.fmt = "q"
        max_digits = self.field_info.max_digits
        if max_digits <= 10:
            self.fmt = "i"
        if max_digits <= 5:
            self.fmt = "h"
        if max_digits <= 2:
            self.fmt = "b"

    def encode(self, value: Decimal) -> int:
        return int(value * 10**self.field_info.decimal_places)

    def decode(self, value: int) -> Decimal:
        return Decimal(str(value / 10**self.field_info.decimal_places))


class EnumCodec(Codec):
    fmt = "H"

    def encode(self, value: Enum) -> int:
        return value.value


class IntCodec(Codec):
    fmt = "I"


class FloatCodec(Codec):
    fmt = "f"
