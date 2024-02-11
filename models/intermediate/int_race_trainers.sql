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
    kyi.`レースキー`,
    kyi.`馬番`,
    bac.`発走日時`,
    kyi.`レースキー_場コード` as `場コード`,
    kyi.`調教師コード`,
    cast(kyi.`レースキー_ｒ` as integer) `レースキー_ｒ`,
    sed.`本賞金`,
    sed.`馬成績_着順` as `着順`,
    -- trainer_runs
    -- When you have two races on the same date, the -1 logic doesn't work (because it should be -2)
    -- The group by logic works correctly but you have to find a way to do that same thing with 場コード and others as well (new ctes?)
    -- coalesce(cast(count(*) over (partition by kyi.`調教師コード` order by bac.`発走日時`) - 1 as integer), 0) as `調教師レース数`,
    -- coalesce(cast(sum(case when sed.`馬成績_着順` = 1 then 1 else 0 end) over (partition by kyi.`調教師コード` order by bac.`発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `調教師1位完走`,
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
  inner join
    sed
  on
    kyi.`レースキー` = sed.`レースキー`
    and kyi.`馬番` = sed.`馬番`
  ),

  -- lookahead problem: the same trainer can have multiple horses in the same race.
  -- so when we calculate how many wins the trainer has has so far, we can't consider horses in the same race.

  trainer_counts as (
  select
    `調教師コード`,
    `発走日時`,
    count(*) as races,
    sum(is_win) as wins,
    sum(is_place) as placings,
    sum(`本賞金`) as `本賞金`,
    sum(case when is_win = 1 then `本賞金` else 0 end) as `本賞金1着`
  from
    race_trainers_base
  group by
    `調教師コード`,
    `発走日時`
  ),

  -- Do this later..
  -- race_trainers_streaks as (
  --
  -- ),

  trainer_cumulatives as (
  select
    `発走日時`,
    `調教師コード`,
    cast(sum(races) over (partition by `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer) as `調教師レース数`,
    cast(sum(wins) over (partition by `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer) as `調教師1位完走`,
    cast(sum(placings) over (partition by `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer) as `調教師トップ3完走`,
    cast(sum(`本賞金`) over (partition by `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as double) as `調教師本賞金累計`,
    cast(sum(`本賞金1着`) over (partition by `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as double) as `調教師本賞金1着累計`
  from
    trainer_counts
  ),

  trainer_venue_counts as (
  select
    `調教師コード`,
    `場コード`,
    `発走日時`,
    count(*) as races,
    sum(is_win) as wins,
    sum(is_place) as placings
  from
    race_trainers_base
  group by
    `調教師コード`,
    `場コード`,
    `発走日時`
  ),

  trainer_venue_cumulatives as (
  select
    `発走日時`,
    `調教師コード`,
    `場コード`,
    sum(races) over (partition by `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as `調教師場所レース数`,
    sum(wins) over (partition by `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as `調教師場所1位完走`,
    sum(placings) over (partition by `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as `調教師場所トップ3完走`
  from
    trainer_venue_counts
  ),

  race_trainers as (
  select
    yt.`レースキー`,
    yt.`馬番`,
    yt.`場コード`,
    yt.`調教師コード`,
    yt.`発走日時`,
    yt.`着順` as `先読み注意_着順`,
    yt.`本賞金` as `先読み注意_本賞金`,
    coalesce(cp.`調教師レース数`, 0) as `調教師レース数`, -- trainer_runs
    coalesce(cp.`調教師1位完走`, 0) as `調教師1位完走`, -- trainer_wins
    coalesce(cp.`調教師トップ3完走`, 0) as `調教師トップ3完走`, -- trainer_places
    coalesce(cp.`調教師1位完走` / cp.`調教師レース数`, 0) as `調教師1位完走率`, -- ratio_win_trainer
    coalesce(cp.`調教師トップ3完走` / cp.`調教師レース数`, 0) as `調教師トップ3完走率`, -- ratio_place_trainer
    coalesce(tv.`調教師場所レース数`, 0) as `調教師場所レース数`, -- trainer_venue_runs
    coalesce(tv.`調教師場所1位完走`, 0) as `調教師場所1位完走`, -- trainer_venue_wins
    coalesce(tv.`調教師場所トップ3完走`, 0) as `調教師場所トップ3完走`, -- trainer_venue_places
    coalesce(tv.`調教師場所1位完走` / tv.`調教師場所レース数`, 0) as `調教師場所1位完走率`, -- ratio_win_trainer_venue
    coalesce(tv.`調教師場所トップ3完走` / tv.`調教師場所レース数`, 0) as `調教師場所トップ3完走率`, -- ratio_place_trainer_venue
    coalesce(cp.`調教師本賞金累計`, 0) as `調教師本賞金累計`, -- prize_trainer_cumulative
    -- When you win, how much money do you win on average?
    coalesce(cp.`調教師本賞金1着累計` / cp.`調教師1位完走`, 0) as `調教師1位完走平均賞金`, -- avg_prize_wins_trainer
    coalesce(cp.`調教師本賞金累計` / cp.`調教師レース数`, 0) as `調教師レース数平均賞金`  -- avg_prize_runs_trainer
    -- Do this later
    -- 調教師連続1着
    -- 調教師連続3着内
  from
    race_trainers_base yt
  inner join
    trainer_cumulatives cp
  on
    yt.`発走日時` = cp.`発走日時`
    and yt.`調教師コード` = cp.`調教師コード`
  inner join
    trainer_venue_cumulatives tv
  on
    -- Careful with the join here, it's not the same as the previous one
    -- We need to join on all group by fields to avoid duplicates
    yt.`発走日時` = tv.`発走日時`
    and yt.`調教師コード` = tv.`調教師コード`
    and yt.`場コード` = tv.`場コード`
  ),

  competitors as (
  select
    a.`レースキー`,
    a.`調教師コード`,
    -- 調教師レース数
    max(b.`調教師レース数`) as `競争相手最高調教師レース数`,
    min(b.`調教師レース数`) as `競争相手最低調教師レース数`,
    avg(b.`調教師レース数`) as `競争相手平均調教師レース数`,
    stddev_pop(b.`調教師レース数`) as `競争相手調教師レース数標準偏差`,
    -- 調教師1位完走
    max(b.`調教師1位完走`) as `競争相手最高調教師1位完走`,
    min(b.`調教師1位完走`) as `競争相手最低調教師1位完走`,
    avg(b.`調教師1位完走`) as `競争相手平均調教師1位完走`,
    stddev_pop(b.`調教師1位完走`) as `競争相手調教師1位完走標準偏差`,
    -- 調教師トップ3完走
    max(b.`調教師トップ3完走`) as `競争相手最高調教師トップ3完走`,
    min(b.`調教師トップ3完走`) as `競争相手最低調教師トップ3完走`,
    avg(b.`調教師トップ3完走`) as `競争相手平均調教師トップ3完走`,
    stddev_pop(b.`調教師トップ3完走`) as `競争相手調教師トップ3完走標準偏差`,
    -- 調教師1位完走率
    max(b.`調教師1位完走率`) as `競争相手最高調教師1位完走率`,
    min(b.`調教師1位完走率`) as `競争相手最低調教師1位完走率`,
    avg(b.`調教師1位完走率`) as `競争相手平均調教師1位完走率`,
    stddev_pop(b.`調教師1位完走率`) as `競争相手調教師1位完走率標準偏差`,
    -- 調教師トップ3完走率
    max(b.`調教師トップ3完走率`) as `競争相手最高調教師トップ3完走率`,
    min(b.`調教師トップ3完走率`) as `競争相手最低調教師トップ3完走率`,
    avg(b.`調教師トップ3完走率`) as `競争相手平均調教師トップ3完走率`,
    stddev_pop(b.`調教師トップ3完走率`) as `競争相手調教師トップ3完走率標準偏差`,
    -- 調教師場所レース数
    max(b.`調教師場所レース数`) as `競争相手最高調教師場所レース数`,
    min(b.`調教師場所レース数`) as `競争相手最低調教師場所レース数`,
    avg(b.`調教師場所レース数`) as `競争相手平均調教師場所レース数`,
    stddev_pop(b.`調教師場所レース数`) as `競争相手調教師場所レース数標準偏差`,
    -- 調教師場所1位完走
    max(b.`調教師場所1位完走`) as `競争相手最高調教師場所1位完走`,
    min(b.`調教師場所1位完走`) as `競争相手最低調教師場所1位完走`,
    avg(b.`調教師場所1位完走`) as `競争相手平均調教師場所1位完走`,
    stddev_pop(b.`調教師場所1位完走`) as `競争相手調教師場所1位完走標準偏差`,
    -- 調教師場所トップ3完走
    max(b.`調教師場所トップ3完走`) as `競争相手最高調教師場所トップ3完走`,
    min(b.`調教師場所トップ3完走`) as `競争相手最低調教師場所トップ3完走`,
    avg(b.`調教師場所トップ3完走`) as `競争相手平均調教師場所トップ3完走`,
    stddev_pop(b.`調教師場所トップ3完走`) as `競争相手調教師場所トップ3完走標準偏差`,
    -- 調教師場所1位完走率
    max(b.`調教師場所1位完走率`) as `競争相手最高調教師場所1位完走率`,
    min(b.`調教師場所1位完走率`) as `競争相手最低調教師場所1位完走率`,
    avg(b.`調教師場所1位完走率`) as `競争相手平均調教師場所1位完走率`,
    stddev_pop(b.`調教師場所1位完走率`) as `競争相手調教師場所1位完走率標準偏差`,
    -- 調教師場所トップ3完走率
    max(b.`調教師場所トップ3完走率`) as `競争相手最高調教師場所トップ3完走率`,
    min(b.`調教師場所トップ3完走率`) as `競争相手最低調教師場所トップ3完走率`,
    avg(b.`調教師場所トップ3完走率`) as `競争相手平均調教師場所トップ3完走率`,
    stddev_pop(b.`調教師場所トップ3完走率`) as `競争相手調教師場所トップ3完走率標準偏差`,
    -- 調教師本賞金累計
    max(b.`調教師本賞金累計`) as `競争相手最高調教師本賞金累計`,
    min(b.`調教師本賞金累計`) as `競争相手最低調教師本賞金累計`,
    avg(b.`調教師本賞金累計`) as `競争相手平均調教師本賞金累計`,
    stddev_pop(b.`調教師本賞金累計`) as `競争相手調教師本賞金累計標準偏差`,
    -- 調教師1位完走平均賞金
    max(b.`調教師1位完走平均賞金`) as `競争相手最高調教師1位完走平均賞金`,
    min(b.`調教師1位完走平均賞金`) as `競争相手最低調教師1位完走平均賞金`,
    avg(b.`調教師1位完走平均賞金`) as `競争相手平均調教師1位完走平均賞金`,
    stddev_pop(b.`調教師1位完走平均賞金`) as `競争相手調教師1位完走平均賞金標準偏差`,
    -- 調教師レース数平均賞金
    max(b.`調教師レース数平均賞金`) as `競争相手最高調教師レース数平均賞金`,
    min(b.`調教師レース数平均賞金`) as `競争相手最低調教師レース数平均賞金`,
    avg(b.`調教師レース数平均賞金`) as `競争相手平均調教師レース数平均賞金`,
    stddev_pop(b.`調教師レース数平均賞金`) as `競争相手調教師レース数平均賞金標準偏差`
    -- 調教師連続1着
    -- max(b.`調教師連続1着`) as `競争相手最高調教師連続1着`,
    -- min(b.`調教師連続1着`) as `競争相手最低調教師連続1着`,
    -- avg(b.`調教師連続1着`) as `競争相手平均調教師連続1着`,
    -- stddev_pop(b.`調教師連続1着`) as `競争相手調教師連続1着標準偏差`,
    -- 調教師連続3着内
    -- max(b.`調教師連続3着内`) as `競争相手最高調教師連続3着内`,
    -- min(b.`調教師連続3着内`) as `競争相手最低調教師連続3着内`,
    -- avg(b.`調教師連続3着内`) as `競争相手平均調教師連続3着内`,
    -- stddev_pop(b.`調教師連続3着内`) as `競争相手調教師連続3着内標準偏差`
  from
    race_trainers a
  inner join
    race_trainers b
  on
    a.`レースキー` = b.`レースキー`
    and a.`調教師コード` <> b.`調教師コード`
  group by
    a.`レースキー`,
    a.`調教師コード`
  ),

  final as (
  select
    -- Metadata fields (not used for prediction)
    race_trainers.`レースキー`,
    race_trainers.`馬番`,
    race_trainers.`場コード`,
    race_trainers.`調教師コード`,
    race_trainers.`発走日時`,
    race_trainers.`先読み注意_着順`,
    race_trainers.`先読み注意_本賞金`,

    -- Base features
    race_trainers.`調教師レース数`,
    race_trainers.`調教師1位完走`,
    race_trainers.`調教師トップ3完走`,
    race_trainers.`調教師1位完走率`,
    race_trainers.`調教師トップ3完走率`,
    race_trainers.`調教師場所レース数`,
    race_trainers.`調教師場所1位完走`,
    race_trainers.`調教師場所トップ3完走`,
    race_trainers.`調教師場所1位完走率`,
    race_trainers.`調教師場所トップ3完走率`,
    race_trainers.`調教師本賞金累計`,
    race_trainers.`調教師1位完走平均賞金`,
    race_trainers.`調教師レース数平均賞金`,

    -- Competitor features
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
    -- competitors.`競争相手最高調教師連続1着`,
    -- competitors.`競争相手最低調教師連続1着`,
    -- competitors.`競争相手平均調教師連続1着`,
    -- competitors.`競争相手調教師連続1着標準偏差`,
    -- competitors.`競争相手最高調教師連続3着内`,
    -- competitors.`競争相手最低調教師連続3着内`,
    -- competitors.`競争相手平均調教師連続3着内`,
    -- competitors.`競争相手調教師連続3着内標準偏差`

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
    and race_trainers.`調教師コード` = competitors.`調教師コード`
  )
select * from final
