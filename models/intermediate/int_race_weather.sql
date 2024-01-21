{{
  config(
    materialized='table',
    schema='intermediate',
    indexes=[{'columns': ['レースキー'], 'unique': True}]
  )
}}

WITH
  bac AS (
  SELECT
    *
  FROM
    {{ ref('stg_jrdb__bac') }}
  ),

  weather_hourly AS (
  SELECT
    *
  FROM
    {{ ref('stg_jma__weather_hourly') }}
  ),

  final as (
  SELECT
    bac.レースキー,
    bac.開催キー,
    レースキー_場コード as 場コード,
    jrdb_racetrack_jma_station_mapping.jma_station_name as 場名,
    レースキー_年 as 年,
    レースキー_回 as 回,
    レースキー_日 as 日,
    レースキー_Ｒ as Ｒ,
    bac.年月日,
    bac.発走時間,
    -- 気温
    CASE
      WHEN w1.気温 IS NOT NULL AND w2.気温 IS NOT NULL THEN w1.気温 + (w2.気温 - w1.気温) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
      ELSE COALESCE(w1.気温, w2.気温)
    END as temperature,
    -- 降水量
    CASE
      WHEN w1.降水量 IS NOT NULL AND w2.降水量 IS NOT NULL THEN w1.降水量 + (w2.降水量 - w1.降水量) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
      ELSE COALESCE(w1.降水量, w2.降水量)
    END as precipitation,
    -- 降雪
    CASE
      WHEN w1.降雪 IS NOT NULL AND w2.降雪 IS NOT NULL THEN w1.降雪 + (w2.降雪 - w1.降雪) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
      ELSE COALESCE(w1.降雪, w2.降雪)
    END as snowfall,
    -- 積雪
    CASE
      WHEN w1.積雪 IS NOT NULL AND w2.積雪 IS NOT NULL THEN w1.積雪 + (w2.積雪 - w1.積雪) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
      ELSE COALESCE(w1.積雪, w2.積雪)
    END as snow_depth,
    -- 日照時間
    -- CASE
    --   WHEN w1.日照時間 IS NOT NULL AND w2.日照時間 IS NOT NULL THEN w1.日照時間 + (w2.日照時間 - w1.日照時間) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
    --   ELSE COALESCE(w1.日照時間, w2.日照時間)
    -- END as sunshine,
    -- 風速
    CASE
      WHEN w1.風速 IS NOT NULL AND w2.風速 IS NOT NULL THEN w1.風速 + (w2.風速 - w1.風速) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
      ELSE COALESCE(w1.風速, w2.風速)
    END as wind_speed,
    -- 風向 (西,北,東,南　など) 一番時間が近い行を選択
    CASE
      WHEN w1.風速_風向 IS NOT NULL AND w2.風速_風向 IS NOT NULL THEN
        CASE
          WHEN ABS(EXTRACT(MINUTE FROM bac.発走時間)::float - EXTRACT(MINUTE FROM w1.年月日時)::float) < ABS(EXTRACT(MINUTE FROM bac.発走時間)::float - EXTRACT(MINUTE FROM w2.年月日時)::float) THEN w1.風速_風向
          ELSE w2.風速_風向
        END
      ELSE COALESCE(w1.風速_風向, w2.風速_風向)
    END as wind_direction,
    -- 日射量
    CASE
      WHEN w1.日射量 IS NOT NULL AND w2.日射量 IS NOT NULL THEN w1.日射量 + (w2.日射量 - w1.日射量) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
      ELSE COALESCE(w1.日射量, w2.日射量)
    END as solar_radiation,
    -- 現地気圧
    CASE
      WHEN w1.現地気圧 IS NOT NULL AND w2.現地気圧 IS NOT NULL THEN w1.現地気圧 + (w2.現地気圧 - w1.現地気圧) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
      ELSE COALESCE(w1.現地気圧, w2.現地気圧)
    END as local_air_pressure,
    -- 海面気圧
    CASE
      WHEN w1.海面気圧 IS NOT NULL AND w2.海面気圧 IS NOT NULL THEN w1.海面気圧 + (w2.海面気圧 - w1.海面気圧) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
      ELSE COALESCE(w1.海面気圧, w2.海面気圧)
    END as sea_level_air_pressure,
    -- 相対湿度
    CASE
      WHEN w1.相対湿度 IS NOT NULL AND w2.相対湿度 IS NOT NULL THEN w1.相対湿度 + (w2.相対湿度 - w1.相対湿度) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
      ELSE COALESCE(w1.相対湿度, w2.相対湿度)
    END as relative_humidity,
    -- 蒸気圧
    CASE
      WHEN w1.蒸気圧 IS NOT NULL AND w2.蒸気圧 IS NOT NULL THEN w1.蒸気圧 + (w2.蒸気圧 - w1.蒸気圧) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
      ELSE COALESCE(w1.蒸気圧, w2.蒸気圧)
    END as vapor_pressure,
    -- 露点温度
    CASE
      WHEN w1.露点温度 IS NOT NULL AND w2.露点温度 IS NOT NULL THEN w1.露点温度 + (w2.露点温度 - w1.露点温度) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
      ELSE COALESCE(w1.露点温度, w2.露点温度)
    END as dew_point_temperature,
    -- 天気 (一番時間が近い行を選択)
    CASE
      WHEN w1.天気 IS NOT NULL AND w2.天気 IS NOT NULL THEN
        CASE
          WHEN ABS(EXTRACT(MINUTE FROM bac.発走時間)::float - EXTRACT(MINUTE FROM w1.年月日時)::float) < ABS(EXTRACT(MINUTE FROM bac.発走時間)::float - EXTRACT(MINUTE FROM w2.年月日時)::float) THEN w1.天気
          ELSE w2.天気
        END
      ELSE COALESCE(w1.天気, w2.天気)
    END as weather,
    -- 雲量
    -- CASE
    --   WHEN w1.雲量 IS NOT NULL AND w2.雲量 IS NOT NULL THEN w1.雲量 + (w2.雲量 - w1.雲量) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
    --   ELSE COALESCE(w1.雲量, w2.雲量)
    -- END as cloud_cover,
    -- 視程
    CASE
      WHEN w1.視程 IS NOT NULL AND w2.視程 IS NOT NULL THEN w1.視程 + (w2.視程 - w1.視程) * (EXTRACT(MINUTE FROM bac.発走時間)::float / 60)
      ELSE COALESCE(w1.視程, w2.視程)
    END as visibility
  FROM
    bac

  LEFT JOIN
    {{ ref('jrdb_racetrack_jma_station_mapping') }} jrdb_racetrack_jma_station_mapping
  ON
    bac.レースキー_場コード = jrdb_racetrack_jma_station_mapping.jrdb_racetrack_code

  LEFT JOIN 
    weather_hourly w1
  ON
    w1.年月日時 = (bac.年月日 || ' ' || LPAD((EXTRACT(HOUR FROM bac.発走時間)::int)::text, 2, '0') || ':00:00')::timestamp
    AND w1.station_name = jrdb_racetrack_jma_station_mapping.jma_station_name

  LEFT JOIN 
    weather_hourly w2
  ON
    w2.年月日時 = (bac.年月日 || ' ' || LPAD((EXTRACT(HOUR FROM bac.発走時間)::int + 1)::text, 2, '0') || ':00:00')::timestamp
    AND w2.station_name = jrdb_racetrack_jma_station_mapping.jma_station_name
  )

select * from final
