-- JRDB analytical warehouse (schemas used by dbt models)
CREATE DATABASE jrdb OWNER admin;
CREATE SCHEMA IF NOT EXISTS jrdb_raw AUTHORIZATION admin;
CREATE SCHEMA IF NOT EXISTS jrdb_intermediate AUTHORIZATION admin;
CREATE SCHEMA IF NOT EXISTS jrdb_curated AUTHORIZATION admin;

-- MLflow experiment tracking metadata
CREATE DATABASE mlflow OWNER admin;

-- Hive Metastore (Spark Thrift Server catalogs) -> referenced as jdbc:postgresql://postgres/metastore
CREATE DATABASE metastore OWNER admin;
