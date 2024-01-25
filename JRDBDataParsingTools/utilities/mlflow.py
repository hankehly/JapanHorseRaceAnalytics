import pandas as pd
from mlflow.types.schema import ColSpec


def get_mlflow_col_spec_type_for_pandas_dtype(dtype) -> str:
    if str(dtype).startswith("float"):
        return "double"
    elif str(dtype) == "category":
        return "string"
    else:
        raise ValueError(f"Unexpected dtype: {dtype}")


def get_colspecs(df: pd.DataFrame) -> list[ColSpec]:
    return [
        ColSpec(get_mlflow_col_spec_type_for_pandas_dtype(dtype), col)
        for col, dtype in df.dtypes.items()
    ]
