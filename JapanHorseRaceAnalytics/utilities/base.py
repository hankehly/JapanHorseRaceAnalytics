from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from JapanHorseRaceAnalytics.utilities.structured_logger import logger


def get_base_dir() -> Path:
    return Path(__file__).parent.parent.parent.resolve()


def get_data_dir() -> Path:
    return get_base_dir() / "data"


def read_sql_table(table_name, schema, use_cache=True):
    save_path = get_data_dir() / "sql_tables" / f"{table_name}.snappy.parquet"
    save_path.parent.mkdir(exist_ok=True, parents=True)
    if save_path.exists() and use_cache:
        logger.info(f"Read from parquet {save_path}")
        return pd.read_parquet(save_path)
    logger.info(f"Read from sql {schema}.{table_name}")
    engine = create_engine("postgresql://admin:admin@localhost:5432/jrdb")
    data = pd.read_sql_table(table_name, engine, schema=schema)
    data.to_parquet(save_path, index=False, compression="snappy")
    return data
