from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
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


def read_hive_table(
    table_name: str,
    schema: str,
    spark_session: SparkSession,
    use_cache: bool = True,
    parse_dates: list = None,
):
    save_path = get_data_dir() / "sql_tables" / f"{table_name}.snappy.parquet"
    if use_cache and save_path.exists():
        logger.info(f"Read from parquet {save_path} to pandas")
        return pd.read_parquet(save_path)
    logger.info(f"Read from hive {schema}.{table_name}")
    spark_df = spark_session.read.table(f"{schema}.{table_name}")
    logger.info(f"Write to parquet {save_path}")
    spark_df.write.mode("overwrite").parquet(str(save_path))
    logger.info(f"Read from parquet {save_path} to pandas")
    data = pd.read_parquet(save_path)
    if parse_dates:
        for col in parse_dates:
            data[col] = pd.to_datetime(data[col])
    return data


def get_spark_session() -> SparkSession:
    warehouse_dir = f"{get_base_dir()}/spark-warehouse"
    postgres_driver_path = f"{get_base_dir()}/jars/postgresql-42.7.1.jar"
    result = (
        SparkSession.builder.appName("JapanHorseRaceAnalytics")
        .config("spark.driver.memory", "20g")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.jars", postgres_driver_path)
        .config("spark.executor.extraClassPath", postgres_driver_path)
        .config("spark.driver.extraClassPath", postgres_driver_path)
        .enableHiveSupport()
        .getOrCreate()
    )
    return result


def get_random_sample(arr, sample_size=None):
    if sample_size is None:
        sample_size = len(arr)
    if isinstance(arr, pd.DataFrame):
        arr = arr.values
    if len(arr) > sample_size:
        sample_indices = np.random.choice(len(arr), size=sample_size, replace=False)
    else:
        sample_indices = np.arange(len(arr))
    return arr[sample_indices], sample_indices
