with
  race_jockeys_base as (
  select
    kyi.`レースキー`,
    kyi.`馬番`,
    bac.`発走日時`,
    kyi.`レースキー_場コード` as `場コード`,
    coalesce(tyb.`騎手コード`, kyi.`騎手コード`) as `騎手コード`,
    bac.`レース条件_距離` as `距離`,
    sed.`本賞金`,
    sed.`馬成績_着順` as `着順`,
    -- jockey_runs
    coalesce(cast(count(*) over (partition by coalesce(tyb.`騎手コード`, kyi.`騎手コード`) order by bac.`発走日時`) - 1 as integer), 0) as `騎手レース数`,
    -- jockey_wins
    coalesce(cast(sum(case when sed.`馬成績_着順` = 1 then 1 else 0 end) over (partition by coalesce(tyb.`騎手コード`, kyi.`騎手コード`) order by bac.`発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `騎手1位完走`,
    case when sed.`馬成績_着順` = 1 then 1 else 0 end as is_win,
    case when sed.`馬成績_着順` <= 3 then 1 else 0 end as is_place
  from
    {{ ref('stg_jrdb__kyi') }} kyi

  -- 前日系は inner join
  inner join
    {{ ref('stg_jrdb__bac') }} bac
  on
    kyi.`レースキー` = bac.`レースキー`

  -- 実績系はレースキーがないかもしれないから left join
  left join
    {{ ref('stg_jrdb__sed') }} sed
  on
    kyi.`レースキー` = sed.`レースキー`
    and kyi.`馬番` = sed.`馬番`

  -- TYBが公開される前に予測する可能性があるから left join
  left join
    {{ ref('stg_jrdb__tyb') }} tyb
  on
    kyi.`レースキー` = tyb.`レースキー`
    and kyi.`馬番` = tyb.`馬番`
  ),

  race_jockeys_streaks as (
  select
    `レースキー`,
    `馬番`,
    cast(sum(is_win) over (partition by `騎手コード`, win_group order by `騎手レース数`) as integer) as `騎手連続1着`,
    cast(sum(is_place) over (partition by `騎手コード`, place_group order by `騎手レース数`) as integer) as `騎手連続3着内`
  from (
    select
      `レースキー`,
      `馬番`,
      `騎手コード`,
      is_win,
      is_place,
      `騎手レース数`,
      sum(case when is_win = 0 then 1 else 0 end) over (partition by `騎手コード` order by `騎手レース数`) as win_group,
      sum(case when is_place = 0 then 1 else 0 end) over (partition by `騎手コード` order by `騎手レース数`) as place_group
    from
      race_jockeys_base
    )
  ),

  race_jockeys as (
  select
    base.`レースキー`,
    base.`馬番`,
    base.`発走日時`,
    base.`騎手コード`,
    base.`着順` as `先読み注意_着順`,
    base.`本賞金` as `先読み注意_本賞金`,
    base.`騎手レース数`,
    base.`騎手1位完走`,

    -- jockey_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `騎手トップ3完走`,

    -- ratio_win_jockey
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`騎手コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `騎手1位完走率`,

    -- ratio_place_jockey
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`騎手コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `騎手トップ3完走率`,

    -- Jockey Win Percent: Jockey’s win percent over the past 5 races.
    -- jockey_win_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`騎手コード` order by `発走日時` rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`騎手コード` order by `発走日時` rows between 5 preceding and 1 preceding) - 1 as double)'
      )
    }}, 0) as `騎手過去5走勝率`,

    -- jockey_place_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`騎手コード` order by `発走日時` rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`騎手コード` order by `発走日時` rows between 5 preceding and 1 preceding) - 1 as double)'
      )
    }}, 0) as `騎手過去5走トップ3完走率`,

    -- jockey_venue_runs
    coalesce(cast(count(*) over (partition by base.`騎手コード`, `場コード` order by `発走日時`) - 1 as integer), 0) as `騎手場所レース数`,

    -- jockey_venue_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `騎手場所1位完走`,

    -- jockey_venue_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `騎手場所トップ3完走`,

    -- ratio_win_jockey_venue
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`騎手コード`, `場コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `騎手場所1位完走率`,

    -- ratio_place_jockey_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`騎手コード`, `場コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `騎手場所トップ3完走率`,

    -- jockey_distance_runs
    coalesce(cast(count(*) over (partition by base.`騎手コード`, `距離` order by `発走日時`) - 1 as integer), 0) as `騎手距離レース数`,

    -- jockey_distance_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`騎手コード`, `距離` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `騎手距離1位完走`,

    -- jockey_distance_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`騎手コード`, `距離` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `騎手距離トップ3完走`,

    -- ratio_win_jockey_distance
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`騎手コード`, `距離` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`騎手コード`, `距離` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `騎手距離1位完走率`,

    -- ratio_place_jockey_distance
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`騎手コード`, `距離` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`騎手コード`, `距離` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `騎手距離トップ3完走率`,

    -- prize_jockey_cumulative
    coalesce(sum(`本賞金`) over (partition by base.`騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding), 0) as `騎手本賞金累計`,

    -- avg_prize_wins_jockey
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then `本賞金` else 0 end) over (partition by base.`騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        '`騎手1位完走`'
      )
    }}, 0) as `騎手1位完走平均賞金`,

    -- avg_prize_runs_jockey
    coalesce({{
      dbt_utils.safe_divide(
        'sum(`本賞金`) over (partition by base.`騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        '`騎手レース数`'
      )
    }}, 0) as `騎手レース数平均賞金`,

    lag(race_jockeys_streaks.`騎手連続1着`, 1, 0) over (partition by base.`騎手コード` order by `発走日時`) as `騎手連続1着`,
    lag(race_jockeys_streaks.`騎手連続3着内`, 1, 0) over (partition by base.`騎手コード` order by `発走日時`) as `騎手連続3着内`
  from
    race_jockeys_base base
  inner join
    race_jockeys_streaks
  on
    base.`レースキー` = race_jockeys_streaks.`レースキー`
    and base.`馬番` = race_jockeys_streaks.`馬番`
  ),

  competitors as (
  select
    a.`レースキー`,
    a.`馬番`,
    -- 騎手レース数
    max(b.`騎手レース数`) as `競争相手最大騎手レース数`,
    min(b.`騎手レース数`) as `競争相手最小騎手レース数`,
    avg(b.`騎手レース数`) as `競争相手平均騎手レース数`,
    stddev_pop(b.`騎手レース数`) as `競争相手騎手レース数標準偏差`,
    -- 騎手1位完走
    max(b.`騎手1位完走`) as `競争相手最大騎手1位完走`,
    min(b.`騎手1位完走`) as `競争相手最小騎手1位完走`,
    avg(b.`騎手1位完走`) as `競争相手平均騎手1位完走`,
    stddev_pop(b.`騎手1位完走`) as `競争相手騎手1位完走標準偏差`,
    -- 騎手トップ3完走
    max(b.`騎手トップ3完走`) as `競争相手最大騎手トップ3完走`,
    min(b.`騎手トップ3完走`) as `競争相手最小騎手トップ3完走`,
    avg(b.`騎手トップ3完走`) as `競争相手平均騎手トップ3完走`,
    stddev_pop(b.`騎手トップ3完走`) as `競争相手騎手トップ3完走標準偏差`,
    -- 騎手1位完走率
    max(b.`騎手1位完走率`) as `競争相手最大騎手1位完走率`,
    min(b.`騎手1位完走率`) as `競争相手最小騎手1位完走率`,
    avg(b.`騎手1位完走率`) as `競争相手平均騎手1位完走率`,
    stddev_pop(b.`騎手1位完走率`) as `競争相手騎手1位完走率標準偏差`,
    -- 騎手トップ3完走率
    max(b.`騎手トップ3完走率`) as `競争相手最大騎手トップ3完走率`,
    min(b.`騎手トップ3完走率`) as `競争相手最小騎手トップ3完走率`,
    avg(b.`騎手トップ3完走率`) as `競争相手平均騎手トップ3完走率`,
    stddev_pop(b.`騎手トップ3完走率`) as `競争相手騎手トップ3完走率標準偏差`,
    -- 騎手過去5走勝率
    max(b.`騎手過去5走勝率`) as `競争相手最大騎手過去5走勝率`,
    min(b.`騎手過去5走勝率`) as `競争相手最小騎手過去5走勝率`,
    avg(b.`騎手過去5走勝率`) as `競争相手平均騎手過去5走勝率`,
    stddev_pop(b.`騎手過去5走勝率`) as `競争相手騎手過去5走勝率標準偏差`,
    -- 騎手過去5走トップ3完走率
    max(b.`騎手過去5走トップ3完走率`) as `競争相手最大騎手過去5走トップ3完走率`,
    min(b.`騎手過去5走トップ3完走率`) as `競争相手最小騎手過去5走トップ3完走率`,
    avg(b.`騎手過去5走トップ3完走率`) as `競争相手平均騎手過去5走トップ3完走率`,
    stddev_pop(b.`騎手過去5走トップ3完走率`) as `競争相手騎手過去5走トップ3完走率標準偏差`,
    -- 騎手場所レース数
    max(b.`騎手場所レース数`) as `競争相手最大騎手場所レース数`,
    min(b.`騎手場所レース数`) as `競争相手最小騎手場所レース数`,
    avg(b.`騎手場所レース数`) as `競争相手平均騎手場所レース数`,
    stddev_pop(b.`騎手場所レース数`) as `競争相手騎手場所レース数標準偏差`,
    -- 騎手場所1位完走
    max(b.`騎手場所1位完走`) as `競争相手最大騎手場所1位完走`,
    min(b.`騎手場所1位完走`) as `競争相手最小騎手場所1位完走`,
    avg(b.`騎手場所1位完走`) as `競争相手平均騎手場所1位完走`,
    stddev_pop(b.`騎手場所1位完走`) as `競争相手騎手場所1位完走標準偏差`,
    -- 騎手場所トップ3完走
    max(b.`騎手場所トップ3完走`) as `競争相手最大騎手場所トップ3完走`,
    min(b.`騎手場所トップ3完走`) as `競争相手最小騎手場所トップ3完走`,
    avg(b.`騎手場所トップ3完走`) as `競争相手平均騎手場所トップ3完走`,
    stddev_pop(b.`騎手場所トップ3完走`) as `競争相手騎手場所トップ3完走標準偏差`,
    -- 騎手場所1位完走率
    max(b.`騎手場所1位完走率`) as `競争相手最大騎手場所1位完走率`,
    min(b.`騎手場所1位完走率`) as `競争相手最小騎手場所1位完走率`,
    avg(b.`騎手場所1位完走率`) as `競争相手平均騎手場所1位完走率`,
    stddev_pop(b.`騎手場所1位完走率`) as `競争相手騎手場所1位完走率標準偏差`,
    -- 騎手場所トップ3完走率
    max(b.`騎手場所トップ3完走率`) as `競争相手最大騎手場所トップ3完走率`,
    min(b.`騎手場所トップ3完走率`) as `競争相手最小騎手場所トップ3完走率`,
    avg(b.`騎手場所トップ3完走率`) as `競争相手平均騎手場所トップ3完走率`,
    stddev_pop(b.`騎手場所トップ3完走率`) as `競争相手騎手場所トップ3完走率標準偏差`,
    -- 騎手距離レース数
    max(b.`騎手距離レース数`) as `競争相手最大騎手距離レース数`,
    min(b.`騎手距離レース数`) as `競争相手最小騎手距離レース数`,
    avg(b.`騎手距離レース数`) as `競争相手平均騎手距離レース数`,
    stddev_pop(b.`騎手距離レース数`) as `競争相手騎手距離レース数標準偏差`,
    -- 騎手距離1位完走
    max(b.`騎手距離1位完走`) as `競争相手最大騎手距離1位完走`,
    min(b.`騎手距離1位完走`) as `競争相手最小騎手距離1位完走`,
    avg(b.`騎手距離1位完走`) as `競争相手平均騎手距離1位完走`,
    stddev_pop(b.`騎手距離1位完走`) as `競争相手騎手距離1位完走標準偏差`,
    -- 騎手距離トップ3完走
    max(b.`騎手距離トップ3完走`) as `競争相手最大騎手距離トップ3完走`,
    min(b.`騎手距離トップ3完走`) as `競争相手最小騎手距離トップ3完走`,
    avg(b.`騎手距離トップ3完走`) as `競争相手平均騎手距離トップ3完走`,
    stddev_pop(b.`騎手距離トップ3完走`) as `競争相手騎手距離トップ3完走標準偏差`,
    -- 騎手距離1位完走率
    max(b.`騎手距離1位完走率`) as `競争相手最大騎手距離1位完走率`,
    min(b.`騎手距離1位完走率`) as `競争相手最小騎手距離1位完走率`,
    avg(b.`騎手距離1位完走率`) as `競争相手平均騎手距離1位完走率`,
    stddev_pop(b.`騎手距離1位完走率`) as `競争相手騎手距離1位完走率標準偏差`,
    -- 騎手距離トップ3完走率
    max(b.`騎手距離トップ3完走率`) as `競争相手最大騎手距離トップ3完走率`,
    min(b.`騎手距離トップ3完走率`) as `競争相手最小騎手距離トップ3完走率`,
    avg(b.`騎手距離トップ3完走率`) as `競争相手平均騎手距離トップ3完走率`,
    stddev_pop(b.`騎手距離トップ3完走率`) as `競争相手騎手距離トップ3完走率標準偏差`,
    -- 騎手本賞金累計
    max(b.`騎手本賞金累計`) as `競争相手最大騎手本賞金累計`,
    min(b.`騎手本賞金累計`) as `競争相手最小騎手本賞金累計`,
    avg(b.`騎手本賞金累計`) as `競争相手平均騎手本賞金累計`,
    stddev_pop(b.`騎手本賞金累計`) as `競争相手騎手本賞金累計標準偏差`,
    -- 騎手1位完走平均賞金
    max(b.`騎手1位完走平均賞金`) as `競争相手最大騎手1位完走平均賞金`,
    min(b.`騎手1位完走平均賞金`) as `競争相手最小騎手1位完走平均賞金`,
    avg(b.`騎手1位完走平均賞金`) as `競争相手平均騎手1位完走平均賞金`,
    stddev_pop(b.`騎手1位完走平均賞金`) as `競争相手騎手1位完走平均賞金標準偏差`,
    -- 騎手レース数平均賞金
    max(b.`騎手レース数平均賞金`) as `競争相手最大騎手レース数平均賞金`,
    min(b.`騎手レース数平均賞金`) as `競争相手最小騎手レース数平均賞金`,
    avg(b.`騎手レース数平均賞金`) as `競争相手平均騎手レース数平均賞金`,
    stddev_pop(b.`騎手レース数平均賞金`) as `競争相手騎手レース数平均賞金標準偏差`,
    -- 騎手連続1着
    max(b.`騎手連続1着`) as `競争相手最高騎手連続1着`,
    min(b.`騎手連続1着`) as `競争相手最低騎手連続1着`,
    avg(b.`騎手連続1着`) as `競争相手平均騎手連続1着`,
    stddev_pop(b.`騎手連続1着`) as `競争相手騎手連続1着標準偏差`,
    -- 騎手連続3着内
    max(b.`騎手連続3着内`) as `競争相手最高騎手連続3着内`,
    min(b.`騎手連続3着内`) as `競争相手最低騎手連続3着内`,
    avg(b.`騎手連続3着内`) as `競争相手平均騎手連続3着内`,
    stddev_pop(b.`騎手連続3着内`) as `競争相手騎手連続3着内標準偏差`
  from
    race_jockeys a
  inner join
    race_jockeys b
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
    race_jockeys.`レースキー` as `meta_int_race_jockeys_レースキー`,
    race_jockeys.`馬番` as `meta_int_race_jockeys_馬番`,
    race_jockeys.`騎手コード` as `meta_int_race_jockeys_騎手コード`,

    -- Base features
    race_jockeys.`騎手レース数` as `num_騎手レース数`, -- jockey_runs
    race_jockeys.`騎手1位完走` as `num_騎手1位完走`, -- jockey_wins
    race_jockeys.`騎手トップ3完走` as `num_騎手トップ3完走`, -- jockey_places
    race_jockeys.`騎手1位完走率` as `num_騎手1位完走率`, -- ratio_win_jockey
    race_jockeys.`騎手トップ3完走率` as `num_騎手トップ3完走率`, -- ratio_place_jockey
    race_jockeys.`騎手過去5走勝率` as `num_騎手過去5走勝率`,
    race_jockeys.`騎手過去5走トップ3完走率` as `num_騎手過去5走トップ3完走率`,
    race_jockeys.`騎手場所レース数` as `num_騎手場所レース数`, -- jockey_venue_runs
    race_jockeys.`騎手場所1位完走` as `num_騎手場所1位完走`, -- jockey_venue_wins
    race_jockeys.`騎手場所トップ3完走` as `num_騎手場所トップ3完走`, -- jockey_venue_places
    race_jockeys.`騎手場所1位完走率` as `num_騎手場所1位完走率`, -- ratio_win_jockey_venue
    race_jockeys.`騎手場所トップ3完走率` as `num_騎手場所トップ3完走率`, -- ratio_place_jockey_venue
    race_jockeys.`騎手距離レース数` as `num_騎手距離レース数`, -- jockey_distance_runs
    race_jockeys.`騎手距離1位完走` as `num_騎手距離1位完走`, -- jockey_distance_wins
    race_jockeys.`騎手距離トップ3完走` as `num_騎手距離トップ3完走`, -- jockey_distance_places
    race_jockeys.`騎手距離1位完走率` as `num_騎手距離1位完走率`, -- ratio_win_jockey_distance
    race_jockeys.`騎手距離トップ3完走率` as `num_騎手距離トップ3完走率`, -- ratio_place_jockey_distance
    race_jockeys.`騎手本賞金累計` as `num_騎手本賞金累計`, -- prize_jockey_cumulative
    race_jockeys.`騎手1位完走平均賞金` as `num_騎手1位完走平均賞金`, -- avg_prize_wins_jockey
    race_jockeys.`騎手レース数平均賞金` as `num_騎手レース数平均賞金`, -- avg_prize_runs_jockey
    race_jockeys.`騎手連続1着` as `num_騎手連続1着`,
    race_jockeys.`騎手連続3着内` as `num_騎手連続3着内`,

    -- Competitors features
    competitors.`競争相手最大騎手レース数` as `num_競争相手最大騎手レース数`,
    competitors.`競争相手最小騎手レース数` as `num_競争相手最小騎手レース数`,
    competitors.`競争相手平均騎手レース数` as `num_競争相手平均騎手レース数`,
    competitors.`競争相手騎手レース数標準偏差` as `num_競争相手騎手レース数標準偏差`,
    competitors.`競争相手最大騎手1位完走` as `num_競争相手最大騎手1位完走`,
    competitors.`競争相手最小騎手1位完走` as `num_競争相手最小騎手1位完走`,
    competitors.`競争相手平均騎手1位完走` as `num_競争相手平均騎手1位完走`,
    competitors.`競争相手騎手1位完走標準偏差` as `num_競争相手騎手1位完走標準偏差`,
    competitors.`競争相手最大騎手トップ3完走` as `num_競争相手最大騎手トップ3完走`,
    competitors.`競争相手最小騎手トップ3完走` as `num_競争相手最小騎手トップ3完走`,
    competitors.`競争相手平均騎手トップ3完走` as `num_競争相手平均騎手トップ3完走`,
    competitors.`競争相手騎手トップ3完走標準偏差` as `num_競争相手騎手トップ3完走標準偏差`,
    competitors.`競争相手最大騎手1位完走率` as `num_競争相手最大騎手1位完走率`,
    competitors.`競争相手最小騎手1位完走率` as `num_競争相手最小騎手1位完走率`,
    competitors.`競争相手平均騎手1位完走率` as `num_競争相手平均騎手1位完走率`,
    competitors.`競争相手騎手1位完走率標準偏差` as `num_競争相手騎手1位完走率標準偏差`,
    competitors.`競争相手最大騎手トップ3完走率` as `num_競争相手最大騎手トップ3完走率`,
    competitors.`競争相手最小騎手トップ3完走率` as `num_競争相手最小騎手トップ3完走率`,
    competitors.`競争相手平均騎手トップ3完走率` as `num_競争相手平均騎手トップ3完走率`,
    competitors.`競争相手騎手トップ3完走率標準偏差` as `num_競争相手騎手トップ3完走率標準偏差`,
    competitors.`競争相手最大騎手過去5走勝率` as `num_競争相手最大騎手過去5走勝率`,
    competitors.`競争相手最小騎手過去5走勝率` as `num_競争相手最小騎手過去5走勝率`,
    competitors.`競争相手平均騎手過去5走勝率` as `num_競争相手平均騎手過去5走勝率`,
    competitors.`競争相手騎手過去5走勝率標準偏差` as `num_競争相手騎手過去5走勝率標準偏差`,
    competitors.`競争相手最大騎手過去5走トップ3完走率` as `num_競争相手最大騎手過去5走トップ3完走率`,
    competitors.`競争相手最小騎手過去5走トップ3完走率` as `num_競争相手最小騎手過去5走トップ3完走率`,
    competitors.`競争相手平均騎手過去5走トップ3完走率` as `num_競争相手平均騎手過去5走トップ3完走率`,
    competitors.`競争相手騎手過去5走トップ3完走率標準偏差` as `num_競争相手騎手過去5走トップ3完走率標準偏差`,
    competitors.`競争相手最大騎手場所レース数` as `num_競争相手最大騎手場所レース数`,
    competitors.`競争相手最小騎手場所レース数` as `num_競争相手最小騎手場所レース数`,
    competitors.`競争相手平均騎手場所レース数` as `num_競争相手平均騎手場所レース数`,
    competitors.`競争相手騎手場所レース数標準偏差` as `num_競争相手騎手場所レース数標準偏差`,
    competitors.`競争相手最大騎手場所1位完走` as `num_競争相手最大騎手場所1位完走`,
    competitors.`競争相手最小騎手場所1位完走` as `num_競争相手最小騎手場所1位完走`,
    competitors.`競争相手平均騎手場所1位完走` as `num_競争相手平均騎手場所1位完走`,
    competitors.`競争相手騎手場所1位完走標準偏差` as `num_競争相手騎手場所1位完走標準偏差`,
    competitors.`競争相手最大騎手場所トップ3完走` as `num_競争相手最大騎手場所トップ3完走`,
    competitors.`競争相手最小騎手場所トップ3完走` as `num_競争相手最小騎手場所トップ3完走`,
    competitors.`競争相手平均騎手場所トップ3完走` as `num_競争相手平均騎手場所トップ3完走`,
    competitors.`競争相手騎手場所トップ3完走標準偏差` as `num_競争相手騎手場所トップ3完走標準偏差`,
    competitors.`競争相手最大騎手場所1位完走率` as `num_競争相手最大騎手場所1位完走率`,
    competitors.`競争相手最小騎手場所1位完走率` as `num_競争相手最小騎手場所1位完走率`,
    competitors.`競争相手平均騎手場所1位完走率` as `num_競争相手平均騎手場所1位完走率`,
    competitors.`競争相手騎手場所1位完走率標準偏差` as `num_競争相手騎手場所1位完走率標準偏差`,
    competitors.`競争相手最大騎手場所トップ3完走率` as `num_競争相手最大騎手場所トップ3完走率`,
    competitors.`競争相手最小騎手場所トップ3完走率` as `num_競争相手最小騎手場所トップ3完走率`,
    competitors.`競争相手平均騎手場所トップ3完走率` as `num_競争相手平均騎手場所トップ3完走率`,
    competitors.`競争相手騎手場所トップ3完走率標準偏差` as `num_競争相手騎手場所トップ3完走率標準偏差`,
    competitors.`競争相手最大騎手距離レース数` as `num_競争相手最大騎手距離レース数`,
    competitors.`競争相手最小騎手距離レース数` as `num_競争相手最小騎手距離レース数`,
    competitors.`競争相手平均騎手距離レース数` as `num_競争相手平均騎手距離レース数`,
    competitors.`競争相手騎手距離レース数標準偏差` as `num_競争相手騎手距離レース数標準偏差`,
    competitors.`競争相手最大騎手距離1位完走` as `num_競争相手最大騎手距離1位完走`,
    competitors.`競争相手最小騎手距離1位完走` as `num_競争相手最小騎手距離1位完走`,
    competitors.`競争相手平均騎手距離1位完走` as `num_競争相手平均騎手距離1位完走`,
    competitors.`競争相手騎手距離1位完走標準偏差` as `num_競争相手騎手距離1位完走標準偏差`,
    competitors.`競争相手最大騎手距離トップ3完走` as `num_競争相手最大騎手距離トップ3完走`,
    competitors.`競争相手最小騎手距離トップ3完走` as `num_競争相手最小騎手距離トップ3完走`,
    competitors.`競争相手平均騎手距離トップ3完走` as `num_競争相手平均騎手距離トップ3完走`,
    competitors.`競争相手騎手距離トップ3完走標準偏差` as `num_競争相手騎手距離トップ3完走標準偏差`,
    competitors.`競争相手最大騎手距離1位完走率` as `num_競争相手最大騎手距離1位完走率`,
    competitors.`競争相手最小騎手距離1位完走率` as `num_競争相手最小騎手距離1位完走率`,
    competitors.`競争相手平均騎手距離1位完走率` as `num_競争相手平均騎手距離1位完走率`,
    competitors.`競争相手騎手距離1位完走率標準偏差` as `num_競争相手騎手距離1位完走率標準偏差`,
    competitors.`競争相手最大騎手距離トップ3完走率` as `num_競争相手最大騎手距離トップ3完走率`,
    competitors.`競争相手最小騎手距離トップ3完走率` as `num_競争相手最小騎手距離トップ3完走率`,
    competitors.`競争相手平均騎手距離トップ3完走率` as `num_競争相手平均騎手距離トップ3完走率`,
    competitors.`競争相手騎手距離トップ3完走率標準偏差` as `num_競争相手騎手距離トップ3完走率標準偏差`,
    competitors.`競争相手最大騎手本賞金累計` as `num_競争相手最大騎手本賞金累計`,
    competitors.`競争相手最小騎手本賞金累計` as `num_競争相手最小騎手本賞金累計`,
    competitors.`競争相手平均騎手本賞金累計` as `num_競争相手平均騎手本賞金累計`,
    competitors.`競争相手騎手本賞金累計標準偏差` as `num_競争相手騎手本賞金累計標準偏差`,
    competitors.`競争相手最大騎手1位完走平均賞金` as `num_競争相手最大騎手1位完走平均賞金`,
    competitors.`競争相手最小騎手1位完走平均賞金` as `num_競争相手最小騎手1位完走平均賞金`,
    competitors.`競争相手平均騎手1位完走平均賞金` as `num_競争相手平均騎手1位完走平均賞金`,
    competitors.`競争相手騎手1位完走平均賞金標準偏差` as `num_競争相手騎手1位完走平均賞金標準偏差`,
    competitors.`競争相手最大騎手レース数平均賞金` as `num_競争相手最大騎手レース数平均賞金`,
    competitors.`競争相手最小騎手レース数平均賞金` as `num_競争相手最小騎手レース数平均賞金`,
    competitors.`競争相手平均騎手レース数平均賞金` as `num_競争相手平均騎手レース数平均賞金`,
    competitors.`競争相手騎手レース数平均賞金標準偏差` as `num_競争相手騎手レース数平均賞金標準偏差`,
    competitors.`競争相手最高騎手連続1着` as `num_競争相手最高騎手連続1着`,
    competitors.`競争相手最低騎手連続1着` as `num_競争相手最低騎手連続1着`,
    competitors.`競争相手平均騎手連続1着` as `num_競争相手平均騎手連続1着`,
    competitors.`競争相手騎手連続1着標準偏差` as `num_競争相手騎手連続1着標準偏差`,
    competitors.`競争相手最高騎手連続3着内` as `num_競争相手最高騎手連続3着内`,
    competitors.`競争相手最低騎手連続3着内` as `num_競争相手最低騎手連続3着内`,
    competitors.`競争相手平均騎手連続3着内` as `num_競争相手平均騎手連続3着内`,
    competitors.`競争相手騎手連続3着内標準偏差` as `num_競争相手騎手連続3着内標準偏差`,

    -- Relative to competitors
    race_jockeys.`騎手レース数` - competitors.`競争相手平均騎手レース数` as `num_競争相手平均騎手レース数差`,
    race_jockeys.`騎手1位完走` - competitors.`競争相手平均騎手1位完走` as `num_競争相手平均騎手1位完走差`,
    race_jockeys.`騎手トップ3完走` - competitors.`競争相手平均騎手トップ3完走` as `num_競争相手平均騎手トップ3完走差`,
    race_jockeys.`騎手1位完走率` - competitors.`競争相手平均騎手1位完走率` as `num_競争相手平均騎手1位完走率差`,
    race_jockeys.`騎手トップ3完走率` - competitors.`競争相手平均騎手トップ3完走率` as `num_競争相手平均騎手トップ3完走率差`,
    race_jockeys.`騎手過去5走勝率` - competitors.`競争相手平均騎手過去5走勝率` as `num_競争相手平均騎手過去5走勝率差`,
    race_jockeys.`騎手過去5走トップ3完走率` - competitors.`競争相手平均騎手過去5走トップ3完走率` as `num_競争相手平均騎手過去5走トップ3完走率差`,
    race_jockeys.`騎手場所レース数` - competitors.`競争相手平均騎手場所レース数` as `num_競争相手平均騎手場所レース数差`,
    race_jockeys.`騎手場所1位完走` - competitors.`競争相手平均騎手場所1位完走` as `num_競争相手平均騎手場所1位完走差`,
    race_jockeys.`騎手場所トップ3完走` - competitors.`競争相手平均騎手場所トップ3完走` as `num_競争相手平均騎手場所トップ3完走差`,
    race_jockeys.`騎手場所1位完走率` - competitors.`競争相手平均騎手場所1位完走率` as `num_競争相手平均騎手場所1位完走率差`,
    race_jockeys.`騎手場所トップ3完走率` - competitors.`競争相手平均騎手場所トップ3完走率` as `num_競争相手平均騎手場所トップ3完走率差`,
    race_jockeys.`騎手距離レース数` - competitors.`競争相手平均騎手距離レース数` as `num_競争相手平均騎手距離レース数差`,
    race_jockeys.`騎手距離1位完走` - competitors.`競争相手平均騎手距離1位完走` as `num_競争相手平均騎手距離1位完走差`,
    race_jockeys.`騎手距離トップ3完走` - competitors.`競争相手平均騎手距離トップ3完走` as `num_競争相手平均騎手距離トップ3完走差`,
    race_jockeys.`騎手距離1位完走率` - competitors.`競争相手平均騎手距離1位完走率` as `num_競争相手平均騎手距離1位完走率差`,
    race_jockeys.`騎手距離トップ3完走率` - competitors.`競争相手平均騎手距離トップ3完走率` as `num_競争相手平均騎手距離トップ3完走率差`,
    race_jockeys.`騎手本賞金累計` - competitors.`競争相手平均騎手本賞金累計` as `num_競争相手平均騎手本賞金累計差`,
    race_jockeys.`騎手1位完走平均賞金` - competitors.`競争相手平均騎手1位完走平均賞金` as `num_競争相手平均騎手1位完走平均賞金差`,
    race_jockeys.`騎手レース数平均賞金` - competitors.`競争相手平均騎手レース数平均賞金` as `num_競争相手平均騎手レース数平均賞金差`,
    race_jockeys.`騎手連続1着` - competitors.`競争相手平均騎手連続1着` as `num_競争相手平均騎手連続1着差`,
    race_jockeys.`騎手連続3着内` - competitors.`競争相手平均騎手連続3着内` as `num_競争相手平均騎手連続3着内差`
  from
    race_jockeys
  inner join
    competitors
  on
    race_jockeys.`レースキー` = competitors.`レースキー`
    and race_jockeys.`馬番` = competitors.`馬番`
  )

select
  *
from
  final
