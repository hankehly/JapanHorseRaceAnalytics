from typing import List
from pyspark.sql import Row
from JRDBDataParsingTools.data_schema import FieldModel


def parse_line(line, schema: List[FieldModel]):
    parsed_fields = []
    for field in schema:
        if field.repeat_factor == 1:
            start = field.relative - 1
            end = start + field.byte_length
            parsed_fields.append(line[start:end].decode("cp932").strip())
        else:
            repeated_fields = []
            start = field.relative - 1
            for _ in range(field.repeat_factor):
                end = start + field.byte_length
                repeated_fields.append(line[start:end].decode("cp932").strip())
                start = end
            parsed_fields.append(repeated_fields)

    return Row(*parsed_fields)
