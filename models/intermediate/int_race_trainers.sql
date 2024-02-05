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

  race_trainers_base as (
  select
    concat(kyi.`レースキー`, kyi.`馬番`) as `unique_key`,
    kyi.`レースキー`,
    kyi.`馬番`,
    bac.`年月日`,
    kyi.`レースキー_場コード` as `場コード`,
    kyi.`調教師コード`,
    cast(kyi.`レースキー_Ｒ` as integer) `レースキー_Ｒ`,
    sed.`本賞金`,
    sed.`馬成績_着順` as `着順`,
    -- trainer_runs
    coalesce(cast(count(*) over (partition by kyi.`調教師コード` order by bac.`年月日`, cast(kyi.`レースキー_Ｒ` as integer)) - 1 as integer), 0) as `調教師レース数`,
    -- trainer_wins
    coalesce(cast(sum(case when sed.`馬成績_着順` = 1 then 1 else 0 end) over (partition by kyi.`調教師コード` order by bac.`年月日`, cast(kyi.`レースキー_Ｒ` as integer) rows between unbounded preceding and 1 preceding) as integer), 0) as `調教師1位完走`,
    case when sed.`馬成績_着順` = 1 then 1 else 0 end as is_win,
    case when sed.`馬成績_着順` <= 3 then 1 else 0 end as is_place
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

  -- LOOKAHEAD PROBLEM: The same trainer can have multiple horses in the same race.
  -- So when we calculate how many wins the trainer has has so far, we can't consider horses in the same race.

  -- race_trainers_streaks as (
  -- select
  --   `レースキー`,
  --   `馬番`,
  --   sum(is_win) over (partition by `調教師コード`, win_group order by `調教師レース数`) as `調教師連続1着`,
  --   sum(is_place) over (partition by `調教師コード`, place_group order by `調教師レース数`) as `調教師連続3着内`
  -- from (
  --   select
  --     `レースキー`,
  --     `馬番`,
  --     `調教師コード`,
  --     is_win,
  --     is_place,
  --     `調教師レース数`,
  --     sum(case when is_win = 0 then 1 else 0 end) over (partition by `調教師コード` order by `調教師レース数`) as win_group,
  --     sum(case when is_place = 0 then 1 else 0 end) over (partition by `調教師コード` order by `調教師レース数`) as place_group
  --   from
  --     race_trainers_base
  --   )
  -- ),

  race_trainers as (
  select
    base.`unique_key`,
    base.`レースキー`,
    base.`馬番`,
    base.`年月日`,
    base.`レースキー_Ｒ`,
    base.`調教師コード`,
    base.`着順` as `先読み注意_着順`,
    base.`調教師レース数`,
    base.`調教師1位完走`,

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
    coalesce(sum(`本賞金`) over (partition by `調教師コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding), 0) as `調教師本賞金累計`,

    -- avg_prize_wins_trainer
    coalesce({{
      dbt_utils.safe_divide(
        'sum(`本賞金`) over (partition by `調教師コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding)',
        '`調教師1位完走`'
      )
    }}, 0) as `調教師1位完走平均賞金`,

    -- avg_prize_runs_trainer
    coalesce({{
      dbt_utils.safe_divide(
        'sum(`本賞金`) over (partition by `調教師コード` order by `年月日`, `レースキー_Ｒ` rows between unbounded preceding and 1 preceding)',
        '`調教師レース数`'
      )
    }}, 0) as `調教師レース数平均賞金`,

    lag(race_trainers_streaks.`調教師連続1着`, 1, 0) over (partition by base.`調教師コード` order by `年月日`, `レースキー_Ｒ`) as `調教師連続1着`,
    lag(race_trainers_streaks.`調教師連続3着内`, 1, 0) over (partition by base.`調教師コード` order by `年月日`, `レースキー_Ｒ`) as `調教師連続3着内`
  from
    race_trainers_base base
  inner join
    race_trainers_streaks
  on
    base.`レースキー` = race_trainers_streaks.`レースキー`
    and base.`馬番` = race_trainers_streaks.`馬番`
  ),

  competitors as (
  select
    a.`レースキー`,
    a.`馬番`,
    -- `調教師レース数`
    max(b.`調教師レース数`) as `競争相手最高調教師レース数`,
    min(b.`調教師レース数`) as `競争相手最低調教師レース数`,
    avg(b.`調教師レース数`) as `競争相手平均調教師レース数`,
    stddev_pop(b.`調教師レース数`) as `競争相手調教師レース数標準偏差`,
    -- `調教師1位完走`
    max(b.`調教師1位完走`) as `競争相手最高調教師1位完走`,
    min(b.`調教師1位完走`) as `競争相手最低調教師1位完走`,
    avg(b.`調教師1位完走`) as `競争相手平均調教師1位完走`,
    stddev_pop(b.`調教師1位完走`) as `競争相手調教師1位完走標準偏差`,
    -- `調教師トップ3完走`
    max(b.`調教師トップ3完走`) as `競争相手最高調教師トップ3完走`,
    min(b.`調教師トップ3完走`) as `競争相手最低調教師トップ3完走`,
    avg(b.`調教師トップ3完走`) as `競争相手平均調教師トップ3完走`,
    stddev_pop(b.`調教師トップ3完走`) as `競争相手調教師トップ3完走標準偏差`,
    -- `調教師1位完走率`
    max(b.`調教師1位完走率`) as `競争相手最高調教師1位完走率`,
    min(b.`調教師1位完走率`) as `競争相手最低調教師1位完走率`,
    avg(b.`調教師1位完走率`) as `競争相手平均調教師1位完走率`,
    stddev_pop(b.`調教師1位完走率`) as `競争相手調教師1位完走率標準偏差`,
    -- `調教師トップ3完走率`
    max(b.`調教師トップ3完走率`) as `競争相手最高調教師トップ3完走率`,
    min(b.`調教師トップ3完走率`) as `競争相手最低調教師トップ3完走率`,
    avg(b.`調教師トップ3完走率`) as `競争相手平均調教師トップ3完走率`,
    stddev_pop(b.`調教師トップ3完走率`) as `競争相手調教師トップ3完走率標準偏差`,
    -- `調教師場所レース数`
    max(b.`調教師場所レース数`) as `競争相手最高調教師場所レース数`,
    min(b.`調教師場所レース数`) as `競争相手最低調教師場所レース数`,
    avg(b.`調教師場所レース数`) as `競争相手平均調教師場所レース数`,
    stddev_pop(b.`調教師場所レース数`) as `競争相手調教師場所レース数標準偏差`,
    -- `調教師場所1位完走`
    max(b.`調教師場所1位完走`) as `競争相手最高調教師場所1位完走`,
    min(b.`調教師場所1位完走`) as `競争相手最低調教師場所1位完走`,
    avg(b.`調教師場所1位完走`) as `競争相手平均調教師場所1位完走`,
    stddev_pop(b.`調教師場所1位完走`) as `競争相手調教師場所1位完走標準偏差`,
    -- `調教師場所トップ3完走`
    max(b.`調教師場所トップ3完走`) as `競争相手最高調教師場所トップ3完走`,
    min(b.`調教師場所トップ3完走`) as `競争相手最低調教師場所トップ3完走`,
    avg(b.`調教師場所トップ3完走`) as `競争相手平均調教師場所トップ3完走`,
    stddev_pop(b.`調教師場所トップ3完走`) as `競争相手調教師場所トップ3完走標準偏差`,
    -- `調教師場所1位完走率`
    max(b.`調教師場所1位完走率`) as `競争相手最高調教師場所1位完走率`,
    min(b.`調教師場所1位完走率`) as `競争相手最低調教師場所1位完走率`,
    avg(b.`調教師場所1位完走率`) as `競争相手平均調教師場所1位完走率`,
    stddev_pop(b.`調教師場所1位完走率`) as `競争相手調教師場所1位完走率標準偏差`,
    -- `調教師場所トップ3完走率`
    max(b.`調教師場所トップ3完走率`) as `競争相手最高調教師場所トップ3完走率`,
    min(b.`調教師場所トップ3完走率`) as `競争相手最低調教師場所トップ3完走率`,
    avg(b.`調教師場所トップ3完走率`) as `競争相手平均調教師場所トップ3完走率`,
    stddev_pop(b.`調教師場所トップ3完走率`) as `競争相手調教師場所トップ3完走率標準偏差`,
    -- `調教師本賞金累計`
    max(b.`調教師本賞金累計`) as `競争相手最高調教師本賞金累計`,
    min(b.`調教師本賞金累計`) as `競争相手最低調教師本賞金累計`,
    avg(b.`調教師本賞金累計`) as `競争相手平均調教師本賞金累計`,
    stddev_pop(b.`調教師本賞金累計`) as `競争相手調教師本賞金累計標準偏差`,
    -- `調教師1位完走平均賞金`
    max(b.`調教師1位完走平均賞金`) as `競争相手最高調教師1位完走平均賞金`,
    min(b.`調教師1位完走平均賞金`) as `競争相手最低調教師1位完走平均賞金`,
    avg(b.`調教師1位完走平均賞金`) as `競争相手平均調教師1位完走平均賞金`,
    stddev_pop(b.`調教師1位完走平均賞金`) as `競争相手調教師1位完走平均賞金標準偏差`,
    -- `調教師レース数平均賞金`
    max(b.`調教師レース数平均賞金`) as `競争相手最高調教師レース数平均賞金`,
    min(b.`調教師レース数平均賞金`) as `競争相手最低調教師レース数平均賞金`,
    avg(b.`調教師レース数平均賞金`) as `競争相手平均調教師レース数平均賞金`,
    stddev_pop(b.`調教師レース数平均賞金`) as `競争相手調教師レース数平均賞金標準偏差`,
    -- `調教師連続1着`
    max(b.`調教師連続1着`) as `競争相手最高調教師連続1着`,
    min(b.`調教師連続1着`) as `競争相手最低調教師連続1着`,
    avg(b.`調教師連続1着`) as `競争相手平均調教師連続1着`,
    stddev_pop(b.`調教師連続1着`) as `競争相手調教師連続1着標準偏差`,
    -- `調教師連続3着内`
    max(b.`調教師連続3着内`) as `競争相手最高調教師連続3着内`,
    min(b.`調教師連続3着内`) as `競争相手最低調教師連続3着内`,
    avg(b.`調教師連続3着内`) as `競争相手平均調教師連続3着内`,
    stddev_pop(b.`調教師連続3着内`) as `競争相手調教師連続3着内標準偏差`
  from
    race_trainers a
  inner join
    race_trainers b
  on
    a.`レースキー` = b.`レースキー`
    and a.`馬番` <> b.`馬番`
  group by
    a.`レースキー`,
    a.`馬番`
  ),

  final as (
  select
    -- Metadata fields (not used for prediction)
    race_trainers.`unique_key`,
    race_trainers.`レースキー`,
    race_trainers.`馬番`,
    race_trainers.`年月日`,
    race_trainers.`レースキー_Ｒ`,
    race_trainers.`調教師コード`,
    race_trainers.`先読み注意_着順`,

    -- Base features
    race_trainers.`調教師レース数`, -- trainer_runs
    race_trainers.`調教師1位完走`, -- trainer_wins
    race_trainers.`調教師トップ3完走`, -- trainer_places
    race_trainers.`調教師1位完走率`, -- ratio_win_trainer
    race_trainers.`調教師トップ3完走率`, -- ratio_place_trainer
    race_trainers.`調教師場所レース数`, -- trainer_venue_runs
    race_trainers.`調教師場所1位完走`, -- trainer_venue_wins
    race_trainers.`調教師場所トップ3完走`, -- trainer_venue_places
    race_trainers.`調教師場所1位完走率`, -- ratio_win_trainer_venue
    race_trainers.`調教師場所トップ3完走率`, -- ratio_place_trainer_venue
    race_trainers.`調教師本賞金累計`, -- prize_trainer_cumulative
    race_trainers.`調教師1位完走平均賞金`, -- avg_prize_wins_trainer
    race_trainers.`調教師レース数平均賞金`, -- avg_prize_runs_trainer
    race_trainers.`調教師連続1着`,
    race_trainers.`調教師連続3着内`,

    -- Competitors features
    competitors.`競争相手最高調教師レース数`,
    competitors.`競争相手最低調教師レース数`,
    competitors.`競争相手平均調教師レース数`,
    competitors.`競争相手調教師レース数標準偏差`,
    competitors.`競争相手最高調教師1位完走`,
    competitors.`競争相手最低調教師1位完走`,
    competitors.`競争相手平均調教師1位完走`,
    competitors.`競争相手調教師1位完走標準偏差`,
    competitors.`競争相手最高調教師トップ3完走`,
    competitors.`競争相手最低調教師トップ3完走`,
    competitors.`競争相手平均調教師トップ3完走`,
    competitors.`競争相手調教師トップ3完走標準偏差`,
    competitors.`競争相手最高調教師1位完走率`,
    competitors.`競争相手最低調教師1位完走率`,
    competitors.`競争相手平均調教師1位完走率`,
    competitors.`競争相手調教師1位完走率標準偏差`,
    competitors.`競争相手最高調教師トップ3完走率`,
    competitors.`競争相手最低調教師トップ3完走率`,
    competitors.`競争相手平均調教師トップ3完走率`,
    competitors.`競争相手調教師トップ3完走率標準偏差`,
    competitors.`競争相手最高調教師場所レース数`,
    competitors.`競争相手最低調教師場所レース数`,
    competitors.`競争相手平均調教師場所レース数`,
    competitors.`競争相手調教師場所レース数標準偏差`,
    competitors.`競争相手最高調教師場所1位完走`,
    competitors.`競争相手最低調教師場所1位完走`,
    competitors.`競争相手平均調教師場所1位完走`,
    competitors.`競争相手調教師場所1位完走標準偏差`,
    competitors.`競争相手最高調教師場所トップ3完走`,
    competitors.`競争相手最低調教師場所トップ3完走`,
    competitors.`競争相手平均調教師場所トップ3完走`,
    competitors.`競争相手調教師場所トップ3完走標準偏差`,
    competitors.`競争相手最高調教師場所1位完走率`,
    competitors.`競争相手最低調教師場所1位完走率`,
    competitors.`競争相手平均調教師場所1位完走率`,
    competitors.`競争相手調教師場所1位完走率標準偏差`,
    competitors.`競争相手最高調教師場所トップ3完走率`,
    competitors.`競争相手最低調教師場所トップ3完走率`,
    competitors.`競争相手平均調教師場所トップ3完走率`,
    competitors.`競争相手調教師場所トップ3完走率標準偏差`,
    competitors.`競争相手最高調教師本賞金累計`,
    competitors.`競争相手最低調教師本賞金累計`,
    competitors.`競争相手平均調教師本賞金累計`,
    competitors.`競争相手調教師本賞金累計標準偏差`,
    competitors.`競争相手最高調教師1位完走平均賞金`,
    competitors.`競争相手最低調教師1位完走平均賞金`,
    competitors.`競争相手平均調教師1位完走平均賞金`,
    competitors.`競争相手調教師1位完走平均賞金標準偏差`,
    competitors.`競争相手最高調教師レース数平均賞金`,
    competitors.`競争相手最低調教師レース数平均賞金`,
    competitors.`競争相手平均調教師レース数平均賞金`,
    competitors.`競争相手調教師レース数平均賞金標準偏差`,
    competitors.`競争相手最高調教師連続1着`,
    competitors.`競争相手最低調教師連続1着`,
    competitors.`競争相手平均調教師連続1着`,
    competitors.`競争相手調教師連続1着標準偏差`,
    competitors.`競争相手最高調教師連続3着内`,
    competitors.`競争相手最低調教師連続3着内`,
    competitors.`競争相手平均調教師連続3着内`,
    competitors.`競争相手調教師連続3着内標準偏差`,

    -- Relative features
    race_trainers.`調教師レース数` - competitors.`競争相手平均調教師レース数` as `競争相手平均調教師レース数差`,
    race_trainers.`調教師1位完走` - competitors.`競争相手平均調教師1位完走` as `競争相手平均調教師1位完走差`,
    race_trainers.`調教師トップ3完走` - competitors.`競争相手平均調教師トップ3完走` as `競争相手平均調教師トップ3完走差`,
    race_trainers.`調教師1位完走率` - competitors.`競争相手平均調教師1位完走率` as `競争相手平均調教師1位完走率差`,
    race_trainers.`調教師トップ3完走率` - competitors.`競争相手平均調教師トップ3完走率` as `競争相手平均調教師トップ3完走率差`,
    race_trainers.`調教師場所レース数` - competitors.`競争相手平均調教師場所レース数` as `競争相手平均調教師場所レース数差`,
    race_trainers.`調教師場所1位完走` - competitors.`競争相手平均調教師場所1位完走` as `競争相手平均調教師場所1位完走差`,
    race_trainers.`調教師場所トップ3完走` - competitors.`競争相手平均調教師場所トップ3完走` as `競争相手平均調教師場所トップ3完走差`,
    race_trainers.`調教師場所1位完走率` - competitors.`競争相手平均調教師場所1位完走率` as `競争相手平均調教師場所1位完走率差`,
    race_trainers.`調教師場所トップ3完走率` - competitors.`競争相手平均調教師場所トップ3完走率` as `競争相手平均調教師場所トップ3完走率差`,
    race_trainers.`調教師本賞金累計` - competitors.`競争相手平均調教師本賞金累計` as `競争相手平均調教師本賞金累計差`,
    race_trainers.`調教師1位完走平均賞金` - competitors.`競争相手平均調教師1位完走平均賞金` as `競争相手平均調教師1位完走平均賞金差`,
    race_trainers.`調教師レース数平均賞金` - competitors.`競争相手平均調教師レース数平均賞金` as `競争相手平均調教師レース数平均賞金差`

  from
    race_trainers
  inner join
    competitors
  on
    race_trainers.`レースキー` = competitors.`レースキー`
    and race_trainers.`馬番` = competitors.`馬番`
  )

select
  *
from
  final
