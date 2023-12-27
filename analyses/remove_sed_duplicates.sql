-- Run this DELETE query after importing the SED table.
WITH duplicates AS (
  SELECT
    レースキー_場コード,
    レースキー_年,
    レースキー_回,
    レースキー_日,
    レースキー_Ｒ,
    馬番
  FROM
    jrdb_raw.sed
  GROUP BY
    レースキー_場コード,
    レースキー_年,
    レースキー_回,
    レースキー_日,
    レースキー_Ｒ,
    馬番
  HAVING
    COUNT(*) > 1
)
-- Delete all rows that have a duplicate and a non-empty 発走時間.
-- DELETE FROM jrdb_raw.sed
-- WHERE
--   (
--     レースキー_場コード,
--     レースキー_年,
--     レースキー_回,
--     レースキー_日,
--     レースキー_Ｒ,
--     馬番
--   ) IN (
--     SELECT
--       *
--     FROM
--       duplicates
--   )
--   AND 発走時間 = '';
