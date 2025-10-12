-- Analytical warehouse (schemas used by dbt models)
CREATE DATABASE jhra OWNER admin;
CREATE SCHEMA IF NOT EXISTS jhra_raw AUTHORIZATION admin;
CREATE SCHEMA IF NOT EXISTS jhra_staging AUTHORIZATION admin;
CREATE SCHEMA IF NOT EXISTS jhra_intermediate AUTHORIZATION admin;
CREATE SCHEMA IF NOT EXISTS jhra_curated AUTHORIZATION admin;

-- MLflow experiment tracking metadata
CREATE DATABASE mlflow OWNER admin;

-- Hive Metastore (Spark Thrift Server catalogs) -> referenced as jdbc:postgresql://postgres/metastore
CREATE DATABASE metastore OWNER admin;
