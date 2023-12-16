import yaml
from typing import List, Optional
from pydantic import BaseModel, TypeAdapter
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


class SubFieldModel(BaseModel):
    name: str
    byte_length: int


class FieldModel(BaseModel):
    relative: int
    byte_length: int
    name: str
    repeat_factor: int
    comments: str
    struct_schema: List[SubFieldModel] = None


def create_struct_type(subfields: List[SubFieldModel]) -> StructType:
    return StructType(
        [StructField(subfield.name, StringType(), True) for subfield in subfields]
    )


def create_pyspark_schema(schema: List[FieldModel]) -> StructType:
    fields = []
    for field in schema:
        field_name = field.name
        metadata = {"comments": field.comments}
        if field.repeat_factor > 1:
            if field.struct_schema is not None:
                data_type = ArrayType(create_struct_type(field.struct_schema), True)
            else:
                data_type = ArrayType(StringType(), True)
        else:
            data_type = StringType()
        fields.append(StructField(field_name, data_type, True, metadata))
    return StructType(fields)


def load_schema(file_path: str) -> List[FieldModel]:
    with open(file_path, "r") as file:
        schema_data = yaml.safe_load(file)
    schema = TypeAdapter(List[FieldModel]).validate_python(schema_data)
    return schema
