import yaml
from typing import List
from pydantic import BaseModel, TypeAdapter
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


class FieldModel(BaseModel):
    relative: int
    byte_length: int
    name: str
    repeat_factor: int
    comments: str


def create_pyspark_schema(schema: List[FieldModel]) -> StructType:
    fields = []
    for field in schema:
        field_name = field.name
        metadata = {"comments": field.comments}
        if field.repeat_factor > 1:
            data_type = ArrayType(StringType(), True)
        else:
            data_type = StringType()
        fields.append(StructField(field_name, data_type, True, metadata))
    return StructType(fields)


def parse_line(line, schema: List[FieldModel]):
    parsed_fields = []
    for field in schema:
        if field.repeat_factor == 1:
            start = field.relative - 1  # Adjust for zero-based indexing
            end = start + field.byte_length
            parsed_fields.append(line[start:end].decode("cp932").strip())
        else:
            repeated_fields = []
            start = field.relative - 1
            for _ in range(field.repeat_factor):
                end = start + field.byte_length
                repeated_fields.append(line[start:end].decode("cp932").strip())
                start = end  # Update start for next iteration
            parsed_fields.append(repeated_fields)

    return Row(*parsed_fields)


def decode_cp932(line):
    return line.decode("cp932", errors="ignore")


def load_schema(file_path: str) -> List[FieldModel]:
    with open(file_path, "r") as file:
        schema_data = yaml.safe_load(file)
    schema = TypeAdapter(List[FieldModel]).validate_python(schema_data)
    return schema
