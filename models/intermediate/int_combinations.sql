with
  base as (
  select
    kyi.`レースキー`,
    kyi.`馬番`,
    kyi.`血統登録番号`,
    tyb.`騎手コード`,
    bac.`発走日時`,
    kyi.`調教師コード`,
    kyi.`レースキー_場コード` as `場コード`,
    sed.`馬成績_着順` as `着順`
  from
    {{ ref('stg_jrdb__kyi') }} kyi
  inner join
    {{ ref('stg_jrdb__bac') }} bac
  on
    kyi.`レースキー` = bac.`レースキー`
  left join
    {{ ref('stg_jrdb__sed') }} sed
  on
    kyi.`レースキー` = sed.`レースキー`
    and kyi.`馬番` = sed.`馬番`
  left join
    {{ ref('stg_jrdb__tyb') }} tyb
  on
    kyi.`レースキー` = tyb.`レースキー`
    and kyi.`馬番` = tyb.`馬番`
  ),

  features as (
  select
    base.`レースキー`,
    base.`馬番`,

    -- runs_horse_jockey
    coalesce(cast(count(*) over (partition by `血統登録番号`, `騎手コード` order by `発走日時`) - 1 as integer), 0) as `馬騎手レース数`,

    -- wins_horse_jockey
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬騎手1位完走`,

    -- ratio_win_horse_jockey
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `騎手コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬騎手1位完走率`,

    -- places_horse_jockey
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬騎手トップ3完走`,

    -- ratio_place_horse_jockey
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `騎手コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬騎手トップ3完走率`,

    -- first_second_jockey
    case when cast(count(*) over (partition by `血統登録番号`, `騎手コード` order by `発走日時`) - 1 as integer) < 2 then true else false end as `馬騎手初二走`,

    -- same_last_jockey (horse jockey combination was same last race)
    case when lag(`騎手コード`) over (partition by `血統登録番号` order by `発走日時`) = `騎手コード` then true else false end as `馬騎手同騎手`,

    -- runs_horse_jockey_venue
    coalesce(cast(count(*) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時`) - 1 as integer), 0) as `馬騎手場所レース数`,

    -- wins_horse_jockey_venue
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬騎手場所1位完走`,

    -- ratio_win_horse_jockey_venue
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬騎手場所1位完走率`,

    -- places_horse_jockey_venue
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬騎手場所トップ3完走`,

    -- ratio_place_horse_jockey_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬騎手場所トップ3完走率`,

    -- runs_horse_trainer
    coalesce(cast(count(*) over (partition by `血統登録番号`, `調教師コード` order by `発走日時`) - 1 as integer), 0) as `馬調教師レース数`,

    -- wins_horse_trainer
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬調教師1位完走`,

    -- ratio_win_horse_trainer
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `調教師コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬調教師1位完走率`,

    -- places_horse_trainer
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬調教師トップ3完走`,

    -- ratio_place_horse_trainer
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `調教師コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬調教師トップ3完走率`,

    -- first_second_trainer
    case when cast(count(*) over (partition by `血統登録番号`, `調教師コード` order by `発走日時`) - 1 as integer) < 2 then true else false end as `馬調教師初二走`,

    -- same_last_trainer
    case when lag(`調教師コード`) over (partition by `血統登録番号` order by `発走日時`) = `調教師コード` then true else false end as `馬調教師同調教師`,

    -- runs_horse_trainer_venue
    coalesce(cast(count(*) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時`) - 1 as integer), 0) as `馬調教師場所レース数`,

    -- wins_horse_trainer_venue
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬調教師場所1位完走`,

    -- ratio_win_horse_trainer_venue
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬調教師場所1位完走率`,

    -- places_horse_trainer_venue
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬調教師場所トップ3完走`,

    -- ratio_place_horse_trainer_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬調教師場所トップ3完走率`
  from
    base
  ),

  final as (
  select
    base.`レースキー` as `meta_int_combinations_レースキー`,
    base.`馬番` as `meta_int_combinations_馬番`,
    features.`馬騎手レース数` as `num_馬騎手レース数`,
    features.`馬騎手1位完走` as `num_馬騎手1位完走`,
    features.`馬騎手1位完走率` as `num_馬騎手1位完走率`,
    features.`馬騎手トップ3完走` as `num_馬騎手トップ3完走`,
    features.`馬騎手トップ3完走率` as `num_馬騎手トップ3完走率`,
    features.`馬騎手初二走` as `num_馬騎手初二走`,
    features.`馬騎手同騎手` as `num_馬騎手同騎手`,
    features.`馬騎手場所レース数` as `num_馬騎手場所レース数`,
    features.`馬騎手場所1位完走` as `num_馬騎手場所1位完走`,
    features.`馬騎手場所1位完走率` as `num_馬騎手場所1位完走率`,
    features.`馬騎手場所トップ3完走` as `num_馬騎手場所トップ3完走`,
    features.`馬騎手場所トップ3完走率` as `num_馬騎手場所トップ3完走率`,
    features.`馬調教師レース数` as `num_馬調教師レース数`,
    features.`馬調教師1位完走` as `num_馬調教師1位完走`,
    features.`馬調教師1位完走率` as `num_馬調教師1位完走率`,
    features.`馬調教師トップ3完走` as `num_馬調教師トップ3完走`,
    features.`馬調教師トップ3完走率` as `num_馬調教師トップ3完走率`,
    features.`馬調教師初二走` as `num_馬調教師初二走`,
    features.`馬調教師同調教師` as `num_馬調教師同調教師`,
    features.`馬調教師場所レース数` as `num_馬調教師場所レース数`,
    features.`馬調教師場所1位完走` as `num_馬調教師場所1位完走`,
    features.`馬調教師場所1位完走率` as `num_馬調教師場所1位完走率`,
    features.`馬調教師場所トップ3完走` as `num_馬調教師場所トップ3完走`,
    features.`馬調教師場所トップ3完走率` as `num_馬調教師場所トップ3完走率`
  from
    base
  inner join
    features
  using
    (`レースキー`, `馬番`)
  )

select * from final