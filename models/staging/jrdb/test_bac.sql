{{ config(file_format='parquet', materialized='table') }}

SELECT
  *
FROM
  parquet.`/Users/hankehly/Projects/JapanHorseRaceAnalytics/BAC`

-- select 1 as foo
