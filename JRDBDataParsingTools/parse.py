import yaml
from typing import List
from pyspark.sql import functions as f
from pyspark.sql.types import StringType
from pydantic import BaseModel, TypeAdapter
from pyspark.sql import DataFrame


class FieldModel(BaseModel):
    relative: int
    byte_length: int
    name: str


def parse_text(df: DataFrame, schema: List[FieldModel]) -> DataFrame:
    new_df = df
    for field in schema:
        new_df = new_df.withColumn(
            field.name,
            f.substring(f.col("value"), field.relative, field.byte_length).cast(
                StringType()
            ),
        )
    return new_df


def decode_cp932(line):
    return line.decode("cp932", errors="ignore")


def load_schema(file_path: str) -> List[FieldModel]:
    with open(file_path, "r") as file:
        schema_data = yaml.safe_load(file)

    schema = TypeAdapter(List[FieldModel]).validate_python(schema_data)
    return schema
