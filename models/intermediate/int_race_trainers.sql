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

  base as (
  select
    concat(kyi.`レースキー`, kyi.`馬番`) as `unique_key`,
    kyi.`レースキー`,
    kyi.`馬番`,
    bac.`年月日`,
    kyi.`レースキー_場コード` as `場コード`,
    kyi.`調教師コード`,
    kyi.`レースキー_Ｒ`,
    sed.`本賞金`,
    sed.`馬成績_着順` as `着順`,
    -- trainer_runs
    coalesce(cast(count(*) over (partition by kyi.`調教師コード` order by bac.`年月日`, kyi.`レースキー_Ｒ`) - 1 as integer), 0) as `調教師レース数`,
    -- trainer_wins
    coalesce(cast(sum(case when sed.`馬成績_着順` = 1 then 1 else 0 end) over (partition by kyi.`調教師コード` order by bac.`年月日`, kyi.`レースキー_Ｒ` rows between unbounded preceding and 1 preceding) as integer), 0) as `調教師1位完走`
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
  ),

  trainer_features as (
  select
    `レースキー`,
    `馬番`,
    -- trainer_runs
    `調教師レース数`,
    -- trainer_wins
    `調教師1位完走`,

    -- trainer_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `調教師コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding) as integer), 0) as `調教師トップ3完走`,

    -- ratio_win_trainer
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `調教師コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `調教師コード` order by `年月日`, `レースキー_Ｒ`) - 1 as float)'
      )
    }}, 0) as `調教師1位完走率`,

    -- ratio_place_trainer
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `調教師コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `調教師コード` order by `年月日`, `レースキー_Ｒ`) - 1 as float)'
      )
    }}, 0) as `調教師トップ3完走率`,

    -- trainer_venue_runs
    coalesce(cast(count(*) over (partition by `調教師コード`, `場コード` order by `年月日`, `レースキー_Ｒ`) - 1 as integer), 0) as `調教師場所レース数`,

    -- trainer_venue_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by `調教師コード`, `場コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding) as integer), 0) as `調教師場所1位完走`,

    -- trainer_venue_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `調教師コード`, `場コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding) as integer), 0) as `調教師場所トップ3完走`,

    -- ratio_win_trainer_venue
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `調教師コード`, `場コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `調教師コード`, `場コード` order by `年月日`, `レースキー_Ｒ`) - 1 as float)'
      )
    }}, 0) as `調教師場所1位完走率`,

    -- ratio_place_trainer_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `調教師コード`, `場コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `調教師コード`, `場コード` order by `年月日`, `レースキー_Ｒ`) - 1 as float)'
      )
    }}, 0) as `調教師場所トップ3完走率`,

    -- prize_trainer_cumulative
    coalesce(sum(`本賞金`) over (partition by `調教師コード` order by `年月日` rows between unbounded preceding and 1 preceding), 0) as `調教師本賞金累計`,

    -- avg_prize_wins_trainer
    coalesce({{
      dbt_utils.safe_divide(
        'sum(`本賞金`) over (partition by `調教師コード` order by `年月日` rows between unbounded preceding and 1 preceding)',
        '`調教師1位完走`'
      )
    }}, 0) as `調教師1位完走平均賞金`,

    -- avg_prize_runs_trainer
    coalesce({{
      dbt_utils.safe_divide(
        'sum(`本賞金`) over (partition by `調教師コード` order by `年月日` rows between unbounded preceding and 1 preceding)',
        '`調教師レース数`'
      )
    }}, 0) as `調教師レース数平均賞金`
  from
    base
  ),

  final as (
  select
    base.`unique_key`,
    base.`レースキー`,
    base.`馬番`,
    trainer_features.`調教師レース数`, -- trainer_runs
    trainer_features.`調教師1位完走`, -- trainer_wins
    trainer_features.`調教師トップ3完走`, -- trainer_places
    trainer_features.`調教師1位完走率`, -- ratio_win_trainer
    trainer_features.`調教師トップ3完走率`, -- ratio_place_trainer
    trainer_features.`調教師場所レース数`, -- trainer_venue_runs
    trainer_features.`調教師場所1位完走`, -- trainer_venue_wins
    trainer_features.`調教師場所トップ3完走`, -- trainer_venue_places
    trainer_features.`調教師場所1位完走率`, -- ratio_win_trainer_venue
    trainer_features.`調教師場所トップ3完走率`, -- ratio_place_trainer_venue
    trainer_features.`調教師本賞金累計`, -- prize_trainer_cumulative
    trainer_features.`調教師1位完走平均賞金`, -- avg_prize_wins_trainer
    trainer_features.`調教師レース数平均賞金` -- avg_prize_runs_trainer
  from
    base
  inner join
    trainer_features
  on
    base.`レースキー` = trainer_features.`レースキー`
    and base.`馬番` = trainer_features.`馬番`
  )

select
  *
from
  final
