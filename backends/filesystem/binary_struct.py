import struct
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Type

from pydantic import ConstrainedDecimal, ConstrainedStr

from .codecs import Codec, DecimalCodec, EnumCodec, FloatCodec, IntCodec, StringCodec

if TYPE_CHECKING:
    from schema import TimeSeries


class BinaryStruct:
    fmt: str
    codec_by_field: Dict[str, Codec]
    codec_by_index: List[Codec]
    data_type: Type["TimeSeries"]

    def __init__(self, data_type: Type["TimeSeries"], endianess: str) -> None:
        self.data_type = data_type
        self.codec_by_field = {}
        self.codec_by_index = []
        # timestamp field, which always comes first
        self.fmt = endianess
        codec: "Codec"
        codec_class: Type["Codec"]

        for field, model_field in data_type.__fields__.items():
            field_info = model_field.field_info

            if model_field.name == "timestamp":
                codec_class = IntCodec
            elif issubclass(model_field.type_, ConstrainedStr):
                codec_class = StringCodec
            elif issubclass(model_field.type_, ConstrainedDecimal):
                # let's encode a Decimal as int, where the value is
                # multiplied by number of decimal places
                # we can later optimize this by the number of max digits
                codec_class = DecimalCodec
            elif issubclass(model_field.type_, Enum):
                codec_class = EnumCodec
            elif model_field.type_ == int:
                codec_class = IntCodec
            elif model_field.type_ == float:
                codec_class = FloatCodec
            else:
                raise ValueError("Unsupported codec")

            codec = codec_class(model_field=model_field, field_info=field_info)
            self.codec_by_field[field] = codec
            self.codec_by_index.append(codec)

            self.fmt += codec.fmt

    def decode_point(self, data: bytes) -> Dict[str, Any]:
        """This method first decodes the whole binary payload according to the structore's
        fmt. However this is not the end as we're serializing some high-level python
        data types as more primitive. Therefore we need to pass these values by codecs
        and only then can we return the appropriate type"""
        _raw_data = struct.unpack(self.fmt, data)
        _dict = {}
        for index, codec in enumerate(self.codec_by_index):
            _dict[codec.model_field.name] = codec.decode(_raw_data[index])

        return self.data_type(**_dict)

    def encode_point(self, point: "TimeSeries") -> bytes:
        """This method encodes the point as specified by the struct fmt.

        It passes each field by the codec to convert the internal python type into
        one of the base types used for binary representation"""
        raw_data = [int(point.timestamp.timestamp())]

        data = point.dict()
        data.pop("timestamp")

        for field, value in data.items():
            raw_data.append(self.codec_by_field[field].encode(value))

        print(f"fmt={self.fmt}; raw_data={raw_data}")

        return struct.pack(self.fmt, *raw_data)
