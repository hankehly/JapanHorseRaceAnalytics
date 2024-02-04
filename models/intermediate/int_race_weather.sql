with
  bac as (
  select
    *
  from
    {{ ref('stg_jrdb__bac') }}
  ),

  weather_hourly as (
  select
    *
  from
    {{ ref('stg_jma__weather_hourly') }}
  ),

  final as (
  select
    bac.`レースキー`,
    bac.`開催キー`,
    `レースキー_場コード` as `場コード`,
    jrdb_racetrack_jma_station_mapping.jma_station_name as `場名`,
    `レースキー_年` as `年`,
    `レースキー_回` as `回`,
    `レースキー_日` as `日`,
    `レースキー_Ｒ` as `Ｒ`,
    bac.`年月日`,
    bac.`発走時間`,
    -- 気温
    case
      wen w1.気温 is not null and w2.気温 is not null then w1.気温 + (w2.気温 - w1.気温) * (extract(minute from bac.発走時間)::float / 60)
      else coalesce(w1.気温, w2.気温)
    end as temperature,
    -- 降水量
    case
      wen w1.降水量 is not null and w2.降水量 is not null then w1.降水量 + (w2.降水量 - w1.降水量) * (extract(minute from bac.発走時間)::float / 60)
      else coalesce(w1.降水量, w2.降水量)
    end as precipitation,
    -- 降雪
    case
      wen w1.降雪 is not null and w2.降雪 is not null then w1.降雪 + (w2.降雪 - w1.降雪) * (extract(minute from bac.発走時間)::float / 60)
      else coalesce(w1.降雪, w2.降雪)
    end as snowfall,
    -- 積雪
    case
      wen w1.積雪 is not null and w2.積雪 is not null then w1.積雪 + (w2.積雪 - w1.積雪) * (extract(minute from bac.発走時間)::float / 60)
      else coalesce(w1.積雪, w2.積雪)
    end as snow_depth,
    -- 日照時間
    -- case
    --   wen w1.日照時間 is not null and w2.日照時間 is not null then w1.日照時間 + (w2.日照時間 - w1.日照時間) * (extract(minute from bac.発走時間)::float / 60)
    --   else coalesce(w1.日照時間, w2.日照時間)
    -- end as sunshine,
    -- 風速
    case
      wen w1.風速 is not null and w2.風速 is not null then w1.風速 + (w2.風速 - w1.風速) * (extract(minute from bac.発走時間)::float / 60)
      else coalesce(w1.風速, w2.風速)
    end as wind_speed,
    -- 風向 (西,北,東,南　など) 一番時間が近い行を選択
    case
      wen w1.風速_風向 is not null and w2.風速_風向 is not null then
        case
          wen ABS(extract(minute from bac.発走時間)::float - extract(minute from w1.年月日時)::float) < ABS(extract(minute from bac.発走時間)::float - extract(minute from w2.年月日時)::float) then w1.風速_風向
          else w2.風速_風向
        end
      else coalesce(w1.風速_風向, w2.風速_風向)
    end as wind_direction,
    -- 日射量
    case
      wen w1.日射量 is not null and w2.日射量 is not null then w1.日射量 + (w2.日射量 - w1.日射量) * (extract(minute from bac.発走時間)::float / 60)
      else coalesce(w1.日射量, w2.日射量)
    end as solar_radiation,
    -- 現地気圧
    case
      wen w1.現地気圧 is not null and w2.現地気圧 is not null then w1.現地気圧 + (w2.現地気圧 - w1.現地気圧) * (extract(minute from bac.発走時間)::float / 60)
      else coalesce(w1.現地気圧, w2.現地気圧)
    end as local_air_pressure,
    -- 海面気圧
    case
      wen w1.海面気圧 is not null and w2.海面気圧 is not null then w1.海面気圧 + (w2.海面気圧 - w1.海面気圧) * (extract(minute from bac.発走時間)::float / 60)
      else coalesce(w1.海面気圧, w2.海面気圧)
    end as sea_level_air_pressure,
    -- 相対湿度
    case
      wen w1.相対湿度 is not null and w2.相対湿度 is not null then w1.相対湿度 + (w2.相対湿度 - w1.相対湿度) * (extract(minute from bac.発走時間)::float / 60)
      else coalesce(w1.相対湿度, w2.相対湿度)
    end as relative_humidity,
    -- 蒸気圧
    case
      wen w1.蒸気圧 is not null and w2.蒸気圧 is not null then w1.蒸気圧 + (w2.蒸気圧 - w1.蒸気圧) * (extract(minute from bac.発走時間)::float / 60)
      else coalesce(w1.蒸気圧, w2.蒸気圧)
    end as vapor_pressure,
    -- 露点温度
    case
      wen w1.露点温度 is not null and w2.露点温度 is not null then w1.露点温度 + (w2.露点温度 - w1.露点温度) * (extract(minute from bac.発走時間)::float / 60)
      else coalesce(w1.露点温度, w2.露点温度)
    end as dew_point_temperature,
    -- 天気 (一番時間が近い行を選択)
    case
      wen w1.天気 is not null and w2.天気 is not null then
        case
          wen ABS(extract(minute from bac.発走時間)::float - extract(minute from w1.年月日時)::float) < ABS(extract(minute from bac.発走時間)::float - extract(minute from w2.年月日時)::float) then w1.天気
          else w2.天気
        end
      else coalesce(w1.天気, w2.天気)
    end as weather,
    -- 雲量
    -- case
    --   wen w1.雲量 is not null and w2.雲量 is not null then w1.雲量 + (w2.雲量 - w1.雲量) * (extract(minute from bac.発走時間)::float / 60)
    --   else coalesce(w1.雲量, w2.雲量)
    -- end as cloud_cover,
    -- 視程
    case
      wen w1.視程 is not null and w2.視程 is not null then w1.視程 + (w2.視程 - w1.視程) * (extract(minute from bac.発走時間)::float / 60)
      else coalesce(w1.視程, w2.視程)
    end as visibility
  from
    bac

  LEFT JOIN
    {{ ref('jrdb_racetrack_jma_station_mapping') }} jrdb_racetrack_jma_station_mapping
  ON
    bac.レースキー_場コード = jrdb_racetrack_jma_station_mapping.jrdb_racetrack_code

  LEFT JOIN 
    weather_hourly w1
  ON
    w1.年月日時 = (bac.年月日 || ' ' || LPAD((extract(HOUR from bac.発走時間)::int)::text, 2, '0') || ':00:00')::timestamp
    and w1.station_name = jrdb_racetrack_jma_station_mapping.jma_station_name

  LEFT JOIN 
    weather_hourly w2
  ON
    w2.年月日時 = (bac.年月日 || ' ' || LPAD((extract(HOUR from bac.発走時間)::int + 1)::text, 2, '0') || ':00:00')::timestamp
    and w2.station_name = jrdb_racetrack_jma_station_mapping.jma_station_name
  )

select * from final
