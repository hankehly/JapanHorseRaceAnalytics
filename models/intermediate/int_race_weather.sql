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

  base as (
  select
    bac.`レースキー`,

    -- 気温
    case
      when w1.`気温` is not null and w2.`気温` is not null then w1.`気温` + (w2.`気温` - w1.`気温`) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
      else coalesce(w1.`気温`, w2.`気温`)
    end as temperature,
    -- 降水量
    case
      when w1.`降水量` is not null and w2.`降水量` is not null then w1.`降水量` + (w2.`降水量` - w1.`降水量`) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
      else coalesce(w1.`降水量`, w2.`降水量`)
    end as precipitation,
    -- 降雪
    case
      when w1.`降雪` is not null and w2.`降雪` is not null then w1.`降雪` + (w2.`降雪` - w1.`降雪`) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
      else coalesce(w1.`降雪`, w2.`降雪`)
    end as snowfall,
    -- 積雪
    case
      when w1.`積雪` is not null and w2.`積雪` is not null then w1.`積雪` + (w2.`積雪` - w1.`積雪`) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
      else coalesce(w1.`積雪`, w2.`積雪`)
    end as snow_depth,
    -- 日照時間
    -- case
    --   when w1.日照時間 is not null and w2.日照時間 is not null then w1.日照時間 + (w2.日照時間 - w1.日照時間) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
    --   else coalesce(w1.日照時間, w2.日照時間)
    -- end as sunshine,
    -- 風速
    case
      when w1.`風速` is not null and w2.`風速` is not null then w1.`風速` + (w2.`風速` - w1.`風速`) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
      else coalesce(w1.`風速`, w2.`風速`)
    end as wind_speed,
    -- 風向 (西,北,東,南　など) 一番時間が近い行を選択
    case
      when w1.`風速_風向` is not null and w2.`風速_風向` is not null then
        case
          when ABS(cast(extract(minute from bac.`発走日時`) as double) - cast(extract(minute from w1.`年月日時`) as double)) < ABS(cast(extract(minute from bac.`発走日時`) as double) - cast(extract(minute from w2.`年月日時`) as double)) then w1.`風速_風向`
          else w2.`風速_風向`
        end
      else coalesce(w1.`風速_風向`, w2.`風速_風向`)
    end as wind_direction,
    -- 日射量
    case
      when w1.`日射量` is not null and w2.`日射量` is not null then w1.`日射量` + (w2.`日射量` - w1.`日射量`) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
      else coalesce(w1.`日射量`, w2.`日射量`)
    end as solar_radiation,
    -- 現地気圧
    case
      when w1.`現地気圧` is not null and w2.`現地気圧` is not null then w1.`現地気圧` + (w2.`現地気圧` - w1.`現地気圧`) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
      else coalesce(w1.`現地気圧`, w2.`現地気圧`)
    end as local_air_pressure,
    -- 海面気圧
    case
      when w1.`海面気圧` is not null and w2.`海面気圧` is not null then w1.`海面気圧` + (w2.`海面気圧` - w1.`海面気圧`) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
      else coalesce(w1.`海面気圧`, w2.`海面気圧`)
    end as sea_level_air_pressure,
    -- 相対湿度
    case
      when w1.`相対湿度` is not null and w2.`相対湿度` is not null then w1.`相対湿度` + (w2.`相対湿度` - w1.`相対湿度`) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
      else coalesce(w1.`相対湿度`, w2.`相対湿度`)
    end as relative_humidity,
    -- 蒸気圧
    case
      when w1.`蒸気圧` is not null and w2.`蒸気圧` is not null then w1.`蒸気圧` + (w2.`蒸気圧` - w1.`蒸気圧`) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
      else coalesce(w1.`蒸気圧`, w2.`蒸気圧`)
    end as vapor_pressure,
    -- 露点温度
    case
      when w1.`露点温度` is not null and w2.`露点温度` is not null then w1.`露点温度` + (w2.`露点温度` - w1.`露点温度`) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
      else coalesce(w1.`露点温度`, w2.`露点温度`)
    end as dew_point_temperature,
    -- 天気 (一番時間が近い行を選択)
    case
      when w1.`天気` is not null and w2.`天気` is not null then
        case
          when ABS(cast(extract(minute from bac.`発走日時`) as double) - cast(extract(minute from w1.`年月日時`) as double)) < ABS(cast(extract(minute from bac.`発走日時`) as double) - cast(extract(minute from w2.`年月日時`) as double)) then w1.`天気`
          else w2.`天気`
        end
      else coalesce(w1.`天気`, w2.`天気`)
    end as weather,
    -- 雲量
    -- case
    --   when w1.雲量 is not null and w2.雲量 is not null then w1.雲量 + (w2.雲量 - w1.雲量) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
    --   else coalesce(w1.雲量, w2.雲量)
    -- end as cloud_cover,
    -- 視程
    case
      when w1.`視程` is not null and w2.`視程` is not null then w1.`視程` + (w2.`視程` - w1.`視程`) * (cast(extract(minute from bac.`発走日時`) as double) / 60)
      else coalesce(w1.`視程`, w2.`視程`)
    end as visibility
  from
    bac

  left join
    {{ ref('jrdb_racetrack_jma_station_mapping') }} jrdb_racetrack_jma_station_mapping
  on
    bac.`レースキー_場コード` = jrdb_racetrack_jma_station_mapping.jrdb_racetrack_code

  left join 
    weather_hourly w1
  on
    w1.`年月日時` = to_timestamp(
      bac.`年月日` || ' ' || LPAD(
        cast(cast(extract(hour from bac.`発走日時`) as integer) as string), 2, '0'
      ) || ':00:00', 'yyyy-MM-dd HH:mm:ss'
    )
    and w1.station_name = jrdb_racetrack_jma_station_mapping.jma_station_name

  left join 
    weather_hourly w2
  on
    w2.`年月日時` = to_timestamp(
      bac.`年月日` || ' ' || LPAD(
        cast(cast(extract(hour from bac.`発走日時`) as integer) + 1 as string), 2, '0'
      ) || ':00:00', 'yyyy-MM-dd HH:mm:ss'
    )
    and w2.station_name = jrdb_racetrack_jma_station_mapping.jma_station_name
  ),

  final as (
  select
    `レースキー` as `meta_int_race_weather_レースキー`,
    temperature as num_temperature,
    precipitation as num_precipitation,
    snowfall as num_snowfall,
    snow_depth as num_snow_depth,
    -- sunshine,
    wind_speed as num_wind_speed,
    wind_direction as cat_wind_direction,
    solar_radiation as num_solar_radiation,
    local_air_pressure as num_local_air_pressure,
    sea_level_air_pressure as num_sea_level_air_pressure,
    relative_humidity as num_relative_humidity,
    vapor_pressure as num_vapor_pressure,
    dew_point_temperature as num_dew_point_temperature,
    weather as cat_weather,
    -- cloud_cover,
    visibility as num_visibility
  from
    base
  )

select * from final
