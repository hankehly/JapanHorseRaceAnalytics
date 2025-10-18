import os
from dataclasses import dataclass
from pathlib import Path

# Root of the repository (parent of this package directory)
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

JRDB_USERNAME = os.getenv("JRDB_USERNAME")
JRDB_PASSWORD = os.getenv("JRDB_PASSWORD")
SPARK_WAREHOUSE_DIR = os.environ.get("SPARK_WAREHOUSE_DIR")
POSTGRES_JDBC_JAR = os.environ.get("POSTGRES_JDBC_JAR")
HIVE_METASTORE_URL = os.environ.get("HIVE_METASTORE_URL")


@dataclass(frozen=True)
class Settings:
    # fmt: off
    # Common directories
    PROJECT_ROOT: Path  = PROJECT_ROOT
    SCHEMAS_DIR: Path   = PROJECT_ROOT / "schemas"
    DATA_DIR: Path      = PROJECT_ROOT / "data"
    JRDB_DATA_DIR: Path = PROJECT_ROOT / "data" / "jrdb"
    # fmt: on

    # JRDB credentials
    JRDB_USERNAME: str | None = JRDB_USERNAME
    JRDB_PASSWORD: str | None = JRDB_PASSWORD

    # Spark and Metastore settings
    SPARK_WAREHOUSE_DIR: str | None = SPARK_WAREHOUSE_DIR
    POSTGRES_JDBC_JAR: str | None = POSTGRES_JDBC_JAR
    HIVE_METASTORE_URL: str | None = HIVE_METASTORE_URL
    SCHEMA_NAME: str = "jhra_raw"


settings = Settings()
