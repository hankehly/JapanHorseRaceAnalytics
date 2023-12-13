import yaml
from typing import List
from pydantic import BaseModel, TypeAdapter
from pyspark.sql import Row


class FieldModel(BaseModel):
    relative: int
    byte_length: int
    name: str


def parse_line(line, schema):
    parsed_fields = []
    for field in schema:
        start = field.relative - 1  # Adjust for zero-based indexing
        end = start + field.byte_length
        parsed_fields.append(line[start:end].decode("cp932").strip())
    return Row(*parsed_fields)


def decode_cp932(line):
    return line.decode("cp932", errors="ignore")


def load_schema(file_path: str) -> List[FieldModel]:
    with open(file_path, "r") as file:
        schema_data = yaml.safe_load(file)

    schema = TypeAdapter(List[FieldModel]).validate_python(schema_data)
    return schema
