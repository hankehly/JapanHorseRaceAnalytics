{{
  config(
    materialized='table',
    schema='intermediate',
    indexes=[{'columns': ['レースキー', '馬番'], 'unique': True}]
  )
}}

with
  bac as (
  select
    *
  from
    {{ ref('stg_jrdb__bac') }}
  ),

  kyi as (
  select
    *
  from
    {{ ref('stg_jrdb__kyi') }}
  ),

  sed as (
  select
    *
  from
    {{ ref('stg_jrdb__sed') }}
  ),

  tyb as (
  select
    *
  from
    {{ ref('stg_jrdb__tyb') }}
  ),

  base as (
  select
    concat(kyi.`レースキー`, kyi.`馬番`) as `unique_key`,
    kyi.`レースキー`,
    kyi.`馬番`,
    bac.`年月日`,
    kyi.`レースキー_場コード` as `場コード`,
    coalesce(tyb.`騎手コード`, kyi.`騎手コード`) as `騎手コード`,
    kyi.`レースキー_Ｒ`,
    bac.`レース条件_距離` as `距離`,
    sed.`本賞金`,
    sed.`馬成績_着順` as `着順`,
    -- jockey_runs
    coalesce(cast(count(*) over (partition by coalesce(tyb.`騎手コード`, kyi.`騎手コード`) order by bac.`年月日`, kyi.`レースキー_Ｒ`) - 1 as integer), 0) as `騎手レース数`,
    -- jockey_wins
    coalesce(cast(sum(case when sed.`馬成績_着順` = 1 then 1 else 0 end) over (partition by coalesce(tyb.`騎手コード`, kyi.`騎手コード`) order by bac.`年月日`, kyi.`レースキー_Ｒ` rows between unbounded preceding and 1 preceding) as integer), 0) as `騎手1位完走`
  from
    kyi

  -- 前日系は inner join
  inner join
    bac
  on
    kyi.`レースキー` = bac.`レースキー`

  -- 実績系はレースキーがないかもしれないから left join
  left join
    sed
  on
    kyi.`レースキー` = sed.`レースキー`
    and kyi.`馬番` = sed.`馬番`

  -- TYBが公開される前に予測する可能性があるから left join
  left join
    tyb
  on
    kyi.`レースキー` = tyb.`レースキー`
    and kyi.`馬番` = tyb.`馬番`
  ),

  jockey_features as (
  select
    `レースキー`,
    `馬番`,
    `騎手レース数`,
    `騎手1位完走`,

    -- jockey_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `騎手コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding) as integer), 0) as `騎手トップ3完走`,

    -- ratio_win_jockey
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `騎手コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `騎手コード` order by `年月日`, `レースキー_Ｒ`) - 1 as float)'
      )
    }}, 0) as `騎手1位完走率`,

    -- ratio_place_jockey
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `騎手コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `騎手コード` order by `年月日`, `レースキー_Ｒ`) - 1 as float)'
      )
    }}, 0) as `騎手トップ3完走率`,

    -- Jockey Win Percent: Jockey’s win percent over the past 5 races.
    -- jockey_win_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `騎手コード` order by `年月日`, `レースキー_Ｒ` rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by `騎手コード` order by `年月日`, `レースキー_Ｒ` rows between 5 preceding and 1 preceding) - 1 as float)'
      )
    }}, 0) as `騎手過去5走勝率`,

    -- jockey_place_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `騎手コード` order by `年月日`, `レースキー_Ｒ` rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by `騎手コード` order by `年月日`, `レースキー_Ｒ` rows between 5 preceding and 1 preceding) - 1 as float)'
      )
    }}, 0) as `騎手過去5走トップ3完走率`,

    -- jockey_venue_runs
    coalesce(cast(count(*) over (partition by `騎手コード`, `場コード` order by `年月日`, `レースキー_Ｒ`) - 1 as integer), 0) as `騎手場所レース数`,

    -- jockey_venue_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by `騎手コード`, `場コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding) as integer), 0) as `騎手場所1位完走`,

    -- jockey_venue_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `騎手コード`, `場コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding) as integer), 0) as `騎手場所トップ3完走`,

    -- ratio_win_jockey_venue
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `騎手コード`, `場コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `騎手コード`, `場コード` order by `年月日`, `レースキー_Ｒ`) - 1 as float)'
      )
    }}, 0) as `騎手場所1位完走率`,

    -- ratio_place_jockey_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `騎手コード`, `場コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `騎手コード`, `場コード` order by `年月日`, `レースキー_Ｒ`) - 1 as float)'
      )
    }}, 0) as `騎手場所トップ3完走率`,

    -- jockey_distance_runs
    coalesce(cast(count(*) over (partition by `騎手コード`, `距離` order by `年月日`, `レースキー_Ｒ`) - 1 as integer), 0) as `騎手距離レース数`,

    -- jockey_distance_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by `騎手コード`, `距離` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding) as integer), 0) as `騎手距離1位完走`,

    -- jockey_distance_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `騎手コード`, `距離` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding) as integer), 0) as `騎手距離トップ3完走`,

    -- ratio_win_jockey_distance
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `騎手コード`, `距離` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `騎手コード`, `距離` order by `年月日`, `レースキー_Ｒ`) - 1 as float)'
      )
    }}, 0) as `騎手距離1位完走率`,

    -- ratio_place_jockey_distance
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `騎手コード`, `距離` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `騎手コード`, `距離` order by `年月日`, `レースキー_Ｒ`) - 1 as float)'
      )
    }}, 0) as `騎手距離トップ3完走率`,

    -- prize_jockey_cumulative
    coalesce(sum(`本賞金`) over (partition by `騎手コード` order by `年月日` rows between unbounded preceding and 1 preceding), 0) as `騎手本賞金累計`,

    -- avg_prize_wins_jockey
    coalesce({{
      dbt_utils.safe_divide(
        'sum(`本賞金`) over (partition by `騎手コード` order by `年月日` rows between unbounded preceding and 1 preceding)',
        '`騎手1位完走`'
      )
    }}, 0) as `騎手1位完走平均賞金`,

    -- avg_prize_runs_jockey
    coalesce({{
      dbt_utils.safe_divide(
        'sum(`本賞金`) over (partition by `騎手コード` order by `年月日` rows between unbounded preceding and 1 preceding)',
        '`騎手レース数`'
      )
    }}, 0) as `騎手レース数平均賞金`
  from
    base
  ),

  final as (
  select
    base.`unique_key`,
    base.`レースキー`,
    base.`馬番`,
    jockey_features.`騎手レース数`, -- jockey_runs
    jockey_features.`騎手1位完走`, -- jockey_wins
    jockey_features.`騎手トップ3完走`, -- jockey_places
    jockey_features.`騎手1位完走率`, -- ratio_win_jockey
    jockey_features.`騎手トップ3完走率`, -- ratio_place_jockey
    jockey_features.`騎手過去5走勝率`,
    jockey_features.`騎手過去5走トップ3完走率`,
    jockey_features.`騎手場所レース数`, -- jockey_venue_runs
    jockey_features.`騎手場所1位完走`, -- jockey_venue_wins
    jockey_features.`騎手場所トップ3完走`, -- jockey_venue_places
    jockey_features.`騎手場所1位完走率`, -- ratio_win_jockey_venue
    jockey_features.`騎手場所トップ3完走率`, -- ratio_place_jockey_venue
    jockey_features.`騎手距離レース数`, -- jockey_distance_runs
    jockey_features.`騎手距離1位完走`, -- jockey_distance_wins
    jockey_features.`騎手距離トップ3完走`, -- jockey_distance_places
    jockey_features.`騎手距離1位完走率`, -- ratio_win_jockey_distance
    jockey_features.`騎手距離トップ3完走率`, -- ratio_place_jockey_distance
    jockey_features.`騎手本賞金累計`, -- prize_jockey_cumulative
    jockey_features.`騎手1位完走平均賞金`, -- avg_prize_wins_jockey
    jockey_features.`騎手レース数平均賞金` -- avg_prize_runs_jockey

  from
    base
  inner join
    jockey_features
  on
    base.`レースキー` = jockey_features.`レースキー`
    and base.`馬番` = jockey_features.`馬番`
  )

select
  *
from
  final
