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

  kab as (
  select
    *
  from
    {{ ref('stg_jrdb__kab') }}
  ),

  ukc as (
  select
    *
  from
    {{ ref('stg_jrdb__ukc') }}
  ),

  -- Get the latest data for each horse (scd type 2)
  ukc_latest AS (
  select
    *
  from
    ukc
  where
    (`血統登録番号`, `データ年月日`) in (select `血統登録番号`, MAX(`データ年月日`) from ukc group by `血統登録番号`)
  ),

  horses_good_finish_turf as (
  select
    `競走成績キー_血統登録番号` as `血統登録番号`,
    sum(
      case
        when `ＪＲＤＢデータ_馬場差` <= -10 and `馬成績_着順` <= 3 then 1
        else 0
      end
    ) > 0 `瞬発戦好走馬`,
    sum(
      case
  	    when `ＪＲＤＢデータ_馬場差` >= 10 and `馬成績_着順` <= 3 then 1
  	    else 0
      end
    ) > 0 `消耗戦好走馬`
  from
    sed
  where
    `馬成績_異常区分` = '0'
    and `レース条件_トラック情報_芝ダ障害コード` = '芝'
  group by
    `血統登録番号`
  ),

  horses_good_finish_dirt as (
  select
    `競走成績キー_血統登録番号` as `血統登録番号`,
    sum(
      case
        when `ＪＲＤＢデータ_馬場差` <= -10 and `馬成績_着順` <= 3 then 1
        else 0
      end
    ) > 0 `瞬発戦好走馬`,
    sum(
      case
  	    when `ＪＲＤＢデータ_馬場差` >= 10 and `馬成績_着順` <= 3 then 1
  	    else 0
      end
    ) > 0 `消耗戦好走馬`
  from
    sed
  where
    `馬成績_異常区分` = '0'
    and `レース条件_トラック情報_芝ダ障害コード` = 'ダート'
  group by
    `血統登録番号`
  ),

  horses_good_finish_any as (
  select
    `競走成績キー_血統登録番号` as `血統登録番号`,
    sum(
      case
        when `ＪＲＤＢデータ_馬場差` <= -10 and `馬成績_着順` <= 3 then 1
        else 0
      end
    ) > 0 `瞬発戦好走馬`,
    sum(
      case
  	    when `ＪＲＤＢデータ_馬場差` >= 10 and `馬成績_着順` <= 3 then 1
  	    else 0
      end
    ) > 0 `消耗戦好走馬`
  from
    sed
  where
    `馬成績_異常区分` = '0'
  group by
    `血統登録番号`
  ),

  horses as (
  select
    ukc.`血統登録番号`,
    case
      when ukc.`性別コード` = '1' then '牡'
      when ukc.`性別コード` = '2' then '牝'
      else 'セン'
    end `性別`,
    ukc.`生年月日` as `生年月日`,
    horses_good_finish_turf.`瞬発戦好走馬` as `瞬発戦好走馬_芝`,
    horses_good_finish_turf.`消耗戦好走馬` as `消耗戦好走馬_芝`,
    horses_good_finish_dirt.`瞬発戦好走馬` as `瞬発戦好走馬_ダート`,
    horses_good_finish_dirt.`消耗戦好走馬` as `消耗戦好走馬_ダート`,
    horses_good_finish_any.`瞬発戦好走馬` as `瞬発戦好走馬_総合`,
    horses_good_finish_any.`消耗戦好走馬` as `消耗戦好走馬_総合`
  from
    ukc_latest as ukc
  left join
    horses_good_finish_turf 
  on
    ukc.`血統登録番号` = horses_good_finish_turf.`血統登録番号`
  left join
    horses_good_finish_dirt
  on
    ukc.`血統登録番号` = horses_good_finish_dirt.`血統登録番号`
  left join
    horses_good_finish_any
  on
    ukc.`血統登録番号` = horses_good_finish_any.`血統登録番号`
  ),

  race_horses_base as (
  select
    kyi.`レースキー`,
    kyi.`馬番`,
    kyi.`枠番`,
    bac.`発走日時`,
    kyi.`レースキー_場コード` as `場コード`,
    case
      when extract(month from bac.`発走日時`) <= 3 then 1
      when extract(month from bac.`発走日時`) <= 6 then 2
      when extract(month from bac.`発走日時`) <= 9 then 3
      when extract(month from bac.`発走日時`) <= 12 then 4
    end as `四半期`,
    kyi.`血統登録番号`,
    kyi.`入厩年月日`,
    horses.`生年月日`,
    horses.`性別`,
    horses.`瞬発戦好走馬_芝`,
    horses.`消耗戦好走馬_芝`,
    horses.`瞬発戦好走馬_ダート`,
    horses.`消耗戦好走馬_ダート`,
    horses.`瞬発戦好走馬_総合`,
    horses.`消耗戦好走馬_総合`,
    tyb.`馬体重`,
    tyb.`馬体重増減`,
    bac.`レース条件_距離` as `距離`,
    sed.`本賞金`,
    coalesce(
      tyb.`馬場状態コード`,
      case
        -- 障害コースの場合は芝馬場状態を使用する
        when bac.`レース条件_トラック情報_芝ダ障害コード` = 'ダート' then kab.`ダ馬場状態コード`
        else kab.`芝馬場状態コード`
      end
    ) as `馬場状態コード`,
    bac.`頭数`,
    bac.`レース条件_トラック情報_芝ダ障害コード` as `トラック種別`,
    sed.`馬成績_着順` as `着順`,
    lag(sed.`馬成績_着順`, 1) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `一走前着順`,
    lag(sed.`馬成績_着順`, 2) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `二走前着順`,
    lag(sed.`馬成績_着順`, 3) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `三走前着順`,
    lag(sed.`馬成績_着順`, 4) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `四走前着順`,
    lag(sed.`馬成績_着順`, 5) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `五走前着順`,
    lag(sed.`馬成績_着順`, 6) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `六走前着順`,
    -- Todo: add later
    -- ＪＲＤＢデータ_不利
    -- ＪＲＤＢデータ_前不利
    -- ＪＲＤＢデータ_中不利
    -- ＪＲＤＢデータ_後不利
    -- how many races this horse has run until now
    row_number() over (partition by kyi.`血統登録番号` order by bac.`発走日時`) - 1 as `レース数`,
    -- how many races this horse has won until now (incremented by one on the following race)
    coalesce(
      cast(
        sum(
          case
            when sed.`馬成績_着順` = 1 then 1
            else 0
          end
        ) over (partition by kyi.`血統登録番号` order by bac.`発走日時` rows between unbounded preceding and 1 preceding) as integer
      ), 0) as `1位完走`, -- horse_wins
    case when `着順` = 1 then 1 else 0 end as is_win,
    case when `着順` <= 3 then 1 else 0 end as is_place
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

  inner join
    kab
  on
    kyi.`開催キー` = kab.`開催キー`
    and bac.`年月日` = kab.`年月日`

  inner join
    horses
  on
    kyi.`血統登録番号` = horses.`血統登録番号`
  ),

  race_horses_streaks as (
  select
    `レースキー`,
    `馬番`,
    sum(is_win) over (partition by `血統登録番号`, win_group order by `レース数`) as `連続1着`,
    sum(is_place) over (partition by `血統登録番号`, place_group order by `レース数`) as `連続3着内`
  from (
    select
      `レースキー`,
      `馬番`,
      `血統登録番号`,
      is_win,
      is_place,
      `レース数`,
      sum(case when is_win = 0 then 1 else 0 end) over (partition by `血統登録番号` order by `レース数`) as win_group,
      sum(case when is_place = 0 then 1 else 0 end) over (partition by `血統登録番号` order by `レース数`) as place_group
    from
      race_horses_base
    )
  ),

  -- 参考:
  -- https://github.com/codeworks-data/mvp-horse-racing-prediction/blob/master/extract_features.py#L73
  -- https://medium.com/codeworksparis/horse-racing-prediction-a-machine-learning-approach-part-2-e9f5eb9a92e9
  race_horses as (
  select
    base.`レースキー`,
    base.`馬番`,
    base.`血統登録番号`,
    base.`発走日時`,
    base.`枠番`,
    base.`着順` as `先読み注意_着順`,
    base.`本賞金` as `先読み注意_本賞金`,
    base.`性別`,
    base.`瞬発戦好走馬_芝`,
    base.`消耗戦好走馬_芝`,
    base.`瞬発戦好走馬_ダート`,
    base.`消耗戦好走馬_ダート`,
    base.`瞬発戦好走馬_総合`,
    base.`消耗戦好走馬_総合`,

    `一走前着順`,
    `二走前着順`,
    `三走前着順`,
    `四走前着順`,
    `五走前着順`,
    `六走前着順`,

    -- whether the horse placed in the previous race
    -- last_place
    case
      when lag(`着順`) over (partition by base.`血統登録番号` order by `発走日時`) <= 3 then true
      else false
    end as `前走トップ3`,

    -- previous race draw
    lag(`枠番`) over (partition by base.`血統登録番号` order by `発走日時`) as `前走枠番`, -- last_draw

    -- horse_rest_time
    -- `発走日時` - `入厩年月日` as `入厩何日前`,
    date_diff(`発走日時`, `入厩年月日`) as `入厩何日前`,

    -- horse_rest_lest14
    -- `発走日時` - `入厩年月日` < 15 as `入厩15日未満`,
    date_diff(`発走日時`, `入厩年月日`) < 15 as `入厩15日未満`,

    -- horse_rest_over35
    -- `発走日時` - `入厩年月日` >= 35 as `入厩35日以上`,
    date_diff(`発走日時`, `入厩年月日`) >= 35 as `入厩35日以上`,

    -- declared_weight
    `馬体重`,

    -- diff_declared_weight
    -- todo: calculate yourself and confirm match
    `馬体重増減` as `馬体重増減`,

    -- distance
    `距離`,

    -- diff_distance from previous race
    coalesce(`距離` - lag(`距離`) over (partition by base.`血統登録番号` order by `発走日時`), 0) as `前走距離差`,

    -- horse_age in years
    -- extract(year from age(`発走日時`, `生年月日`)) + extract(month from age(`発走日時`, `生年月日`)) / 12 + extract(day from age(`発走日時`, `生年月日`)) / (12 * 30.44) AS `年齢`,
    -- months_between('2024-01-01', '2022-12-31') = 12.0322 / 12 = 1.0027 years
    (months_between(`発走日時`, `生年月日`) / 12) as `年齢`,

    -- horse_age_4 or less
    -- age(`発走日時`, `生年月日`) < '5 years' as `4歳以下`,
    (months_between(`発走日時`, `生年月日`) / 12) < 5 as `4歳以下`,

    sum(
      case
        when (months_between(`発走日時`, `生年月日`) / 12) < 5 then 1
        else 0
      end
    ) over (partition by base.`レースキー`) as `4歳以下頭数`,

    coalesce(
      sum(
        case
          when (months_between(`発走日時`, `生年月日`) / 12) < 5 then 1
          else 0
        end
      ) over (partition by base.`レースキー`) / cast(`頭数` as float), 0) as `4歳以下割合`,

    `レース数`, -- horse_runs
    `1位完走`, -- horse_wins

    -- how many races this horse has placed in until now (incremented by one on the following race)
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `トップ3完走`, -- horse_places

    -- ratio_win_horse
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号` order by `発走日時`) - 1 as float)'
      )
    }}, 0) as `1位完走率`,

    -- ratio_place_horse
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号` order by `発走日時`) - 1 as float)'
      )
    }}, 0) as `トップ3完走率`,

    -- Horse Win Percent: Horse’s win percent over the past 5 races.
    -- horse_win_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号` order by `発走日時` rows between 5 preceding and 1 preceding) - 1 as float)'
      )
    }}, 0) as `過去5走勝率`,

    -- horse_place_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号` order by `発走日時` rows between 5 preceding and 1 preceding) - 1 as float)'
      )
    }}, 0) as `過去5走トップ3完走率`,

    -- horse_venue_runs
    coalesce(cast(count(*) over (partition by base.`血統登録番号`, `場コード` order by `発走日時`) - 1 as integer), 0) as `場所レース数`,

    -- horse_venue_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `場所1位完走`,

    -- horse_venue_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `場所トップ3完走`,

    -- ratio_win_horse_venue
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `場コード` order by `発走日時`) - 1 as float)'
      )
    }}, 0) as `場所1位完走率`,

    -- ratio_place_horse_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `場コード` order by `発走日時`) - 1 as float)'
      )
    }}, 0) as `場所トップ3完走率`,

    -- horse_surface_runs
    coalesce(cast(count(*) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時`) - 1 as integer), 0) as `トラック種別レース数`,

    -- horse_surface_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `トラック種別1位完走`,

    -- horse_surface_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `トラック種別トップ3完走`,

    -- ratio_win_horse_surface
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時`) - 1 as float)'
      )
    }}, 0) as `トラック種別1位完走率`,

    -- ratio_place_horse_surface
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時`) - 1 as float)'
      )
    }}, 0) as `トラック種別トップ3完走率`,

    -- horse_going_runs
    coalesce(cast(count(*) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時`) - 1 as integer), 0) as `馬場状態レース数`,

    -- horse_going_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬場状態1位完走`,

    -- horse_going_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬場状態トップ3完走`,

    -- ratio_win_horse_going
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時`) - 1 as float)'
      )
    }}, 0) as `馬場状態1位完走率`,

    -- ratio_place_horse_going
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時`) - 1 as float)'
      )
    }}, 0) as `馬場状態トップ3完走率`,

    -- horse_distance_runs
    coalesce(cast(count(*) over (partition by base.`血統登録番号`, `距離` order by `発走日時`) - 1 as integer), 0) as `距離レース数`,

    -- horse_distance_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `距離` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `距離1位完走`,

    -- horse_distance_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `距離` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `距離トップ3完走`,

    -- ratio_win_horse_distance
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `距離` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `距離` order by `発走日時`) - 1 as float)'
      )
    }}, 0) as `距離1位完走率`,

    -- ratio_place_horse_distance
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `距離` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `距離` order by `発走日時`) - 1 as float)'
      )
    }}, 0) as `距離トップ3完走率`,

    -- horse_quarter_runs
    coalesce(cast(count(*) over (partition by base.`血統登録番号`, `四半期` order by `発走日時`) - 1 as integer), 0) as `四半期レース数`,

    -- horse_quarter_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `四半期` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `四半期1位完走`,

    -- horse_quarter_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `四半期` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `四半期トップ3完走`,

    -- ratio_win_horse_quarter
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `四半期` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `四半期` order by `発走日時`) - 1 as float)'
      )
    }}, 0) as `四半期1位完走率`,

    -- ratio_place_horse_quarter
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `四半期` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `四半期` order by `発走日時`) - 1 as float)'
      )
    }}, 0) as `四半期トップ3完走率`,

    -- Compute the standard rank of the horse on his last 3 races giving us an overview of his state of form
    cast(coalesce(power(`一走前着順` - 1, 2) + power(`二走前着順` - 1, 2) + power(`三走前着順` - 1, 2), 0) as integer) as `過去3走順位平方和`, -- horse_std_rank

    -- prize_horse_cumulative
    coalesce(sum(`本賞金`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding), 0) as `本賞金累計`,

    -- avg_prize_wins_horse
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then `本賞金` else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        '`1位完走`'
      )
    }}, 0) as `1位完走平均賞金`,

    -- avg_prize_runs_horse
    coalesce({{
      dbt_utils.safe_divide(
        'sum(`本賞金`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        '`レース数`'
      )
    }}, 0) as `レース数平均賞金`,

    lag(race_horses_streaks.`連続1着`, 1, 0) over (partition by base.`血統登録番号` order by `発走日時`) as `連続1着`,
    lag(race_horses_streaks.`連続3着内`, 1, 0) over (partition by base.`血統登録番号` order by `発走日時`) as `連続3着内`

  from
    race_horses_base base
  inner join
    race_horses_streaks
  on
    race_horses_streaks.`レースキー` = base.`レースキー`
    and race_horses_streaks.`馬番` = base.`馬番`
  ),

  competitors as (
  select
    a.`レースキー`,
    a.`馬番`,

    -- `性別`
    sum(case when b.`性別` = '牡' then 1 else 0 end) / cast(count(*) as float) as `競争相手性別牡割合`,
    sum(case when b.`性別` = '牝' then 1 else 0 end) / cast(count(*) as float) as `競争相手性別牝割合`,
    sum(case when b.`性別` = 'セン' then 1 else 0 end) / cast(count(*) as float) as `競争相手性別セ割合`,

    -- `一走前着順`
    max(b.`一走前着順`) as `競争相手最高一走前着順`,
    min(b.`一走前着順`) as `競争相手最低一走前着順`,
    avg(b.`一走前着順`) as `競争相手平均一走前着順`,
    stddev_pop(b.`一走前着順`) as `競争相手一走前着順標準偏差`,

    -- `二走前着順`
    max(b.`二走前着順`) as `競争相手最高二走前着順`,
    min(b.`二走前着順`) as `競争相手最低二走前着順`,
    avg(b.`二走前着順`) as `競争相手平均二走前着順`,
    stddev_pop(b.`二走前着順`) as `競争相手二走前着順標準偏差`,

    -- `三走前着順`
    max(b.`三走前着順`) as `競争相手最高三走前着順`,
    min(b.`三走前着順`) as `競争相手最低三走前着順`,
    avg(b.`三走前着順`) as `競争相手平均三走前着順`,
    stddev_pop(b.`三走前着順`) as `競争相手三走前着順標準偏差`,

    -- `四走前着順`
    max(b.`四走前着順`) as `競争相手最高四走前着順`,
    min(b.`四走前着順`) as `競争相手最低四走前着順`,
    avg(b.`四走前着順`) as `競争相手平均四走前着順`,
    stddev_pop(b.`四走前着順`) as `競争相手四走前着順標準偏差`,

    -- `五走前着順`
    max(b.`五走前着順`) as `競争相手最高五走前着順`,
    min(b.`五走前着順`) as `競争相手最低五走前着順`,
    avg(b.`五走前着順`) as `競争相手平均五走前着順`,
    stddev_pop(b.`五走前着順`) as `競争相手五走前着順標準偏差`,

    -- `六走前着順`
    max(b.`六走前着順`) as `競争相手最高六走前着順`,
    min(b.`六走前着順`) as `競争相手最低六走前着順`,
    avg(b.`六走前着順`) as `競争相手平均六走前着順`,
    stddev_pop(b.`六走前着順`) as `競争相手六走前着順標準偏差`,

    -- `前走トップ3`
    sum(case when b.`前走トップ3` then 1 else 0 end) / cast(count(*) as float) as `競争相手前走トップ3割合`,

    -- `前走枠番`

    -- `入厩何日前`
    max(b.`入厩何日前`) as `競争相手最高入厩何日前`,
    min(b.`入厩何日前`) as `競争相手最低入厩何日前`,
    avg(b.`入厩何日前`) as `競争相手平均入厩何日前`,
    stddev_pop(b.`入厩何日前`) as `競争相手入厩何日前標準偏差`,

    -- `入厩15日未満`
    -- `入厩35日以上`

    -- `馬体重`
    max(b.`馬体重`) as `競争相手最高馬体重`,
    min(b.`馬体重`) as `競争相手最低馬体重`,
    avg(b.`馬体重`) as `競争相手平均馬体重`,
    stddev_pop(b.`馬体重`) as `競争相手馬体重標準偏差`,

    -- `馬体重増減`
    max(b.`馬体重増減`) as `競争相手最高馬体重増減`,
    min(b.`馬体重増減`) as `競争相手最低馬体重増減`,
    avg(b.`馬体重増減`) as `競争相手平均馬体重増減`,
    stddev_pop(b.`馬体重増減`) as `競争相手馬体重増減標準偏差`,

    -- `距離`

    -- `前走距離差`
    max(b.`前走距離差`) as `競争相手最高前走距離差`,
    min(b.`前走距離差`) as `競争相手最低前走距離差`,
    avg(b.`前走距離差`) as `競争相手平均前走距離差`,
    stddev_pop(b.`前走距離差`) as `競争相手前走距離差標準偏差`,

    -- `年齢`
    max(b.`年齢`) as `競争相手最高年齢`,
    min(b.`年齢`) as `競争相手最低年齢`,
    avg(b.`年齢`) as `競争相手平均年齢`,
    stddev_pop(b.`年齢`) as `競争相手年齢標準偏差`,

    -- `4歳以下`
    -- `4歳以下頭数`
    -- `4歳以下割合`

    -- `レース数`
    max(b.`レース数`) as `競争相手最高レース数`,
    min(b.`レース数`) as `競争相手最低レース数`,
    avg(b.`レース数`) as `競争相手平均レース数`,
    stddev_pop(b.`レース数`) as `競争相手レース数標準偏差`,

    -- `1位完走`
    max(b.`1位完走`) as `競争相手最高1位完走`,
    min(b.`1位完走`) as `競争相手最低1位完走`,
    avg(b.`1位完走`) as `競争相手平均1位完走`,
    stddev_pop(b.`1位完走`) as `競争相手1位完走標準偏差`,

    -- `トップ3完走`
    max(b.`トップ3完走`) as `競争相手最高トップ3完走`,
    min(b.`トップ3完走`) as `競争相手最低トップ3完走`,
    avg(b.`トップ3完走`) as `競争相手平均トップ3完走`,
    stddev_pop(b.`トップ3完走`) as `競争相手トップ3完走標準偏差`,

    -- `1位完走率`
    max(b.`1位完走率`) as `競争相手最高1位完走率`,
    min(b.`1位完走率`) as `競争相手最低1位完走率`,
    avg(b.`1位完走率`) as `競争相手平均1位完走率`,
    stddev_pop(b.`1位完走率`) as `競争相手1位完走率標準偏差`,

    -- `トップ3完走率`
    max(b.`トップ3完走率`) as `競争相手最高トップ3完走率`,
    min(b.`トップ3完走率`) as `競争相手最低トップ3完走率`,
    avg(b.`トップ3完走率`) as `競争相手平均トップ3完走率`,
    stddev_pop(b.`トップ3完走率`) as `競争相手トップ3完走率標準偏差`,

    -- `過去5走勝率`
    max(b.`過去5走勝率`) as `競争相手最高過去5走勝率`,
    min(b.`過去5走勝率`) as `競争相手最低過去5走勝率`,
    avg(b.`過去5走勝率`) as `競争相手平均過去5走勝率`,
    stddev_pop(b.`過去5走勝率`) as `競争相手過去5走勝率標準偏差`,

    -- `過去5走トップ3完走率`
    max(b.`過去5走トップ3完走率`) as `競争相手最高過去5走トップ3完走率`,
    min(b.`過去5走トップ3完走率`) as `競争相手最低過去5走トップ3完走率`,
    avg(b.`過去5走トップ3完走率`) as `競争相手平均過去5走トップ3完走率`,
    stddev_pop(b.`過去5走トップ3完走率`) as `競争相手過去5走トップ3完走率標準偏差`,

    -- `場所レース数`
    max(b.`場所レース数`) as `競争相手最高場所レース数`,
    min(b.`場所レース数`) as `競争相手最低場所レース数`,
    avg(b.`場所レース数`) as `競争相手平均場所レース数`,
    stddev_pop(b.`場所レース数`) as `競争相手場所レース数標準偏差`,

    -- `場所1位完走`
    max(b.`場所1位完走`) as `競争相手最高場所1位完走`,
    min(b.`場所1位完走`) as `競争相手最低場所1位完走`,
    avg(b.`場所1位完走`) as `競争相手平均場所1位完走`,
    stddev_pop(b.`場所1位完走`) as `競争相手場所1位完走標準偏差`,

    -- `場所トップ3完走`
    max(b.`場所トップ3完走`) as `競争相手最高場所トップ3完走`,
    min(b.`場所トップ3完走`) as `競争相手最低場所トップ3完走`,
    avg(b.`場所トップ3完走`) as `競争相手平均場所トップ3完走`,
    stddev_pop(b.`場所トップ3完走`) as `競争相手場所トップ3完走標準偏差`,

    -- `場所1位完走率`
    max(b.`場所1位完走率`) as `競争相手最高場所1位完走率`,
    min(b.`場所1位完走率`) as `競争相手最低場所1位完走率`,
    avg(b.`場所1位完走率`) as `競争相手平均場所1位完走率`,
    stddev_pop(b.`場所1位完走率`) as `競争相手場所1位完走率標準偏差`,

    -- `場所トップ3完走率`
    max(b.`場所トップ3完走率`) as `競争相手最高場所トップ3完走率`,
    min(b.`場所トップ3完走率`) as `競争相手最低場所トップ3完走率`,
    avg(b.`場所トップ3完走率`) as `競争相手平均場所トップ3完走率`,
    stddev_pop(b.`場所トップ3完走率`) as `競争相手場所トップ3完走率標準偏差`,

    -- `トラック種別レース数`
    max(b.`トラック種別レース数`) as `競争相手最高トラック種別レース数`,
    min(b.`トラック種別レース数`) as `競争相手最低トラック種別レース数`,
    avg(b.`トラック種別レース数`) as `競争相手平均トラック種別レース数`,
    stddev_pop(b.`トラック種別レース数`) as `競争相手トラック種別レース数標準偏差`,

    -- `トラック種別1位完走`
    max(b.`トラック種別1位完走`) as `競争相手最高トラック種別1位完走`,
    min(b.`トラック種別1位完走`) as `競争相手最低トラック種別1位完走`,
    avg(b.`トラック種別1位完走`) as `競争相手平均トラック種別1位完走`,
    stddev_pop(b.`トラック種別1位完走`) as `競争相手トラック種別1位完走標準偏差`,

    -- `トラック種別トップ3完走`
    max(b.`トラック種別トップ3完走`) as `競争相手最高トラック種別トップ3完走`,
    min(b.`トラック種別トップ3完走`) as `競争相手最低トラック種別トップ3完走`,
    avg(b.`トラック種別トップ3完走`) as `競争相手平均トラック種別トップ3完走`,
    stddev_pop(b.`トラック種別トップ3完走`) as `競争相手トラック種別トップ3完走標準偏差`,

    -- `馬場状態レース数`
    max(b.`馬場状態レース数`) as `競争相手最高馬場状態レース数`,
    min(b.`馬場状態レース数`) as `競争相手最低馬場状態レース数`,
    avg(b.`馬場状態レース数`) as `競争相手平均馬場状態レース数`,
    stddev_pop(b.`馬場状態レース数`) as `競争相手馬場状態レース数標準偏差`,

    -- `馬場状態1位完走`
    max(b.`馬場状態1位完走`) as `競争相手最高馬場状態1位完走`,
    min(b.`馬場状態1位完走`) as `競争相手最低馬場状態1位完走`,
    avg(b.`馬場状態1位完走`) as `競争相手平均馬場状態1位完走`,
    stddev_pop(b.`馬場状態1位完走`) as `競争相手馬場状態1位完走標準偏差`,

    -- `馬場状態トップ3完走`
    max(b.`馬場状態トップ3完走`) as `競争相手最高馬場状態トップ3完走`,
    min(b.`馬場状態トップ3完走`) as `競争相手最低馬場状態トップ3完走`,
    avg(b.`馬場状態トップ3完走`) as `競争相手平均馬場状態トップ3完走`,
    stddev_pop(b.`馬場状態トップ3完走`) as `競争相手馬場状態トップ3完走標準偏差`,

    -- `距離レース数`
    max(b.`距離レース数`) as `競争相手最高距離レース数`,
    min(b.`距離レース数`) as `競争相手最低距離レース数`,
    avg(b.`距離レース数`) as `競争相手平均距離レース数`,
    stddev_pop(b.`距離レース数`) as `競争相手距離レース数標準偏差`,

    -- `距離1位完走`
    max(b.`距離1位完走`) as `競争相手最高距離1位完走`,
    min(b.`距離1位完走`) as `競争相手最低距離1位完走`,
    avg(b.`距離1位完走`) as `競争相手平均距離1位完走`,
    stddev_pop(b.`距離1位完走`) as `競争相手距離1位完走標準偏差`,

    -- `距離トップ3完走`
    max(b.`距離トップ3完走`) as `競争相手最高距離トップ3完走`,
    min(b.`距離トップ3完走`) as `競争相手最低距離トップ3完走`,
    avg(b.`距離トップ3完走`) as `競争相手平均距離トップ3完走`,
    stddev_pop(b.`距離トップ3完走`) as `競争相手距離トップ3完走標準偏差`,

    -- `四半期レース数`
    max(b.`四半期レース数`) as `競争相手最高四半期レース数`,
    min(b.`四半期レース数`) as `競争相手最低四半期レース数`,
    avg(b.`四半期レース数`) as `競争相手平均四半期レース数`,
    stddev_pop(b.`四半期レース数`) as `競争相手四半期レース数標準偏差`,

    -- `四半期1位完走`
    max(b.`四半期1位完走`) as `競争相手最高四半期1位完走`,
    min(b.`四半期1位完走`) as `競争相手最低四半期1位完走`,
    avg(b.`四半期1位完走`) as `競争相手平均四半期1位完走`,
    stddev_pop(b.`四半期1位完走`) as `競争相手四半期1位完走標準偏差`,

    -- `四半期トップ3完走`
    max(b.`四半期トップ3完走`) as `競争相手最高四半期トップ3完走`,
    min(b.`四半期トップ3完走`) as `競争相手最低四半期トップ3完走`,
    avg(b.`四半期トップ3完走`) as `競争相手平均四半期トップ3完走`,
    stddev_pop(b.`四半期トップ3完走`) as `競争相手四半期トップ3完走標準偏差`,

    -- `過去3走順位平方和`
    max(b.`過去3走順位平方和`) as `競争相手最高過去3走順位平方和`,
    min(b.`過去3走順位平方和`) as `競争相手最低過去3走順位平方和`,
    avg(b.`過去3走順位平方和`) as `競争相手平均過去3走順位平方和`,
    stddev_pop(b.`過去3走順位平方和`) as `競争相手過去3走順位平方和標準偏差`,

    -- `本賞金累計`
    max(b.`本賞金累計`) as `競争相手最高本賞金累計`,
    min(b.`本賞金累計`) as `競争相手最低本賞金累計`,
    avg(b.`本賞金累計`) as `競争相手平均本賞金累計`,
    stddev_pop(b.`本賞金累計`) as `競争相手本賞金累計標準偏差`,

    -- `1位完走平均賞金`
    max(b.`1位完走平均賞金`) as `競争相手最高1位完走平均賞金`,
    min(b.`1位完走平均賞金`) as `競争相手最低1位完走平均賞金`,
    avg(b.`1位完走平均賞金`) as `競争相手平均1位完走平均賞金`,
    stddev_pop(b.`1位完走平均賞金`) as `競争相手1位完走平均賞金標準偏差`,

    -- `レース数平均賞金`
    max(b.`レース数平均賞金`) as `競争相手最高レース数平均賞金`,
    min(b.`レース数平均賞金`) as `競争相手最低レース数平均賞金`,
    avg(b.`レース数平均賞金`) as `競争相手平均レース数平均賞金`,
    stddev_pop(b.`レース数平均賞金`) as `競争相手レース数平均賞金標準偏差`,

    -- `瞬発戦好走馬_芝`
    sum(case when b.`瞬発戦好走馬_芝` then 1 else 0 end) / cast(count(*) as float) as `競争相手瞬発戦好走馬_芝割合`,

    -- `消耗戦好走馬_芝`
    sum(case when b.`消耗戦好走馬_芝` then 1 else 0 end) / cast(count(*) as float) as `競争相手消耗戦好走馬_芝割合`,

    -- `瞬発戦好走馬_ダート`
    sum(case when b.`瞬発戦好走馬_ダート` then 1 else 0 end) / cast(count(*) as float) as `競争相手瞬発戦好走馬_ダート割合`,

    -- `消耗戦好走馬_ダート`
    sum(case when b.`消耗戦好走馬_ダート` then 1 else 0 end) / cast(count(*) as float) as `競争相手消耗戦好走馬_ダート割合`,

    -- `瞬発戦好走馬_総合`
    sum(case when b.`瞬発戦好走馬_総合` then 1 else 0 end) / cast(count(*) as float) as `競争相手瞬発戦好走馬_総合割合`,

    -- `消耗戦好走馬_総合`
    sum(case when b.`消耗戦好走馬_総合` then 1 else 0 end) / cast(count(*) as float) as `競争相手消耗戦好走馬_総合割合`,

    -- `連続1着`
    max(b.`連続1着`) as `競争相手最高連続1着`,
    min(b.`連続1着`) as `競争相手最低連続1着`,
    avg(b.`連続1着`) as `競争相手平均連続1着`,
    stddev_pop(b.`連続1着`) as `競争相手連続1着標準偏差`,

    -- `連続3着内`
    max(b.`連続3着内`) as `競争相手最高連続3着内`,
    min(b.`連続3着内`) as `競争相手最低連続3着内`,
    avg(b.`連続3着内`) as `競争相手平均連続3着内`,
    stddev_pop(b.`連続3着内`) as `競争相手連続3着内標準偏差`

  from
    race_horses a
  inner join
    race_horses b
  on
    a.`レースキー` = b.`レースキー`
    and a.`馬番` <> b.`馬番`
  group by
    a.`レースキー`,
    a.`馬番`
  ),

  final as (
  select
    -- Metadata (not used for training)
    race_horses.`レースキー`,
    race_horses.`馬番`,
    race_horses.`血統登録番号`,
    race_horses.`発走日時`,
    race_horses.`先読み注意_着順`,
    race_horses.`先読み注意_本賞金`,

    -- Base features
    race_horses.`一走前着順`,
    race_horses.`二走前着順`,
    race_horses.`三走前着順`,
    race_horses.`四走前着順`,
    race_horses.`五走前着順`,
    race_horses.`六走前着順`,
    race_horses.`前走トップ3`,
    race_horses.`枠番`,
    race_horses.`前走枠番`,
    race_horses.`入厩何日前`, -- horse_rest_time
    race_horses.`入厩15日未満`, -- horse_rest_lest14
    race_horses.`入厩35日以上`, -- horse_rest_over35
    race_horses.`馬体重`, -- declared_weight
    race_horses.`馬体重増減`, -- diff_declared_weight (todo: check if this matches lag)
    race_horses.`前走距離差`, -- diff_distance
    race_horses.`年齢`, -- horse_age (years)
    race_horses.`4歳以下`,
    race_horses.`4歳以下頭数`,
    race_horses.`4歳以下割合`,
    race_horses.`レース数`, -- horse_runs
    race_horses.`1位完走`, -- horse_wins
    race_horses.`トップ3完走`, -- horse_places
    race_horses.`1位完走率`,
    race_horses.`トップ3完走率`,
    race_horses.`過去5走勝率`,
    race_horses.`過去5走トップ3完走率`,
    race_horses.`場所レース数`, -- horse_venue_runs
    race_horses.`場所1位完走`, -- horse_venue_wins
    race_horses.`場所トップ3完走`, -- horse_venue_places
    race_horses.`場所1位完走率`, -- ratio_win_horse_venue
    race_horses.`場所トップ3完走率`, -- ratio_place_horse_venue
    race_horses.`トラック種別レース数`, -- horse_surface_runs
    race_horses.`トラック種別1位完走`, -- horse_surface_wins
    race_horses.`トラック種別トップ3完走`, -- horse_surface_places
    race_horses.`トラック種別1位完走率`, -- ratio_win_horse_surface
    race_horses.`トラック種別トップ3完走率`, -- ratio_place_horse_surface
    race_horses.`馬場状態レース数`, -- horse_going_runs
    race_horses.`馬場状態1位完走`, -- horse_going_wins
    race_horses.`馬場状態トップ3完走`, -- horse_going_places
    race_horses.`馬場状態1位完走率`, -- ratio_win_horse_going
    race_horses.`馬場状態トップ3完走率`, -- ratio_place_horse_going
    race_horses.`距離レース数`, -- horse_distance_runs
    race_horses.`距離1位完走`, -- horse_distance_wins
    race_horses.`距離トップ3完走`, -- horse_distance_places
    race_horses.`距離1位完走率`, -- ratio_win_horse_distance
    race_horses.`距離トップ3完走率`, -- ratio_place_horse_distance
    race_horses.`四半期レース数`, -- horse_quarter_runs
    race_horses.`四半期1位完走`, -- horse_quarter_wins
    race_horses.`四半期トップ3完走`, -- horse_quarter_places
    race_horses.`四半期1位完走率`, -- ratio_win_horse_quarter
    race_horses.`四半期トップ3完走率`, -- ratio_place_horse_quarter
    race_horses.`過去3走順位平方和`,
    race_horses.`本賞金累計`,
    race_horses.`1位完走平均賞金`,
    race_horses.`レース数平均賞金`,
    race_horses.`連続1着`,
    race_horses.`連続3着内`,
    race_horses.`性別`,
    race_horses.`瞬発戦好走馬_芝`,
    race_horses.`消耗戦好走馬_芝`,
    race_horses.`瞬発戦好走馬_ダート`,
    race_horses.`消耗戦好走馬_ダート`,
    race_horses.`瞬発戦好走馬_総合`,
    race_horses.`消耗戦好走馬_総合`,

    -- Competitors
    competitors.`競争相手性別牡割合`,
    competitors.`競争相手性別牝割合`,
    competitors.`競争相手性別セ割合`,
    competitors.`競争相手最高一走前着順`,
    competitors.`競争相手最低一走前着順`,
    competitors.`競争相手平均一走前着順`,
    competitors.`競争相手一走前着順標準偏差`,
    competitors.`競争相手最高二走前着順`,
    competitors.`競争相手最低二走前着順`,
    competitors.`競争相手平均二走前着順`,
    competitors.`競争相手二走前着順標準偏差`,
    competitors.`競争相手最高三走前着順`,
    competitors.`競争相手最低三走前着順`,
    competitors.`競争相手平均三走前着順`,
    competitors.`競争相手三走前着順標準偏差`,
    competitors.`競争相手最高四走前着順`,
    competitors.`競争相手最低四走前着順`,
    competitors.`競争相手平均四走前着順`,
    competitors.`競争相手四走前着順標準偏差`,
    competitors.`競争相手最高五走前着順`,
    competitors.`競争相手最低五走前着順`,
    competitors.`競争相手平均五走前着順`,
    competitors.`競争相手五走前着順標準偏差`,
    competitors.`競争相手最高六走前着順`,
    competitors.`競争相手最低六走前着順`,
    competitors.`競争相手平均六走前着順`,
    competitors.`競争相手六走前着順標準偏差`,
    competitors.`競争相手前走トップ3割合`,
    competitors.`競争相手最高入厩何日前`,
    competitors.`競争相手最低入厩何日前`,
    competitors.`競争相手平均入厩何日前`,
    competitors.`競争相手入厩何日前標準偏差`,
    competitors.`競争相手最高馬体重`,
    competitors.`競争相手最低馬体重`,
    competitors.`競争相手平均馬体重`,
    competitors.`競争相手馬体重標準偏差`,
    competitors.`競争相手最高馬体重増減`,
    competitors.`競争相手最低馬体重増減`,
    competitors.`競争相手平均馬体重増減`,
    competitors.`競争相手馬体重増減標準偏差`,
    competitors.`競争相手最高年齢`,
    competitors.`競争相手最低年齢`,
    competitors.`競争相手平均年齢`,
    competitors.`競争相手年齢標準偏差`,
    competitors.`競争相手最高レース数`,
    competitors.`競争相手最低レース数`,
    competitors.`競争相手平均レース数`,
    competitors.`競争相手レース数標準偏差`,
    competitors.`競争相手最高1位完走`,
    competitors.`競争相手最低1位完走`,
    competitors.`競争相手平均1位完走`,
    competitors.`競争相手1位完走標準偏差`,
    competitors.`競争相手最高トップ3完走`,
    competitors.`競争相手最低トップ3完走`,
    competitors.`競争相手平均トップ3完走`,
    competitors.`競争相手トップ3完走標準偏差`,
    competitors.`競争相手最高1位完走率`,
    competitors.`競争相手最低1位完走率`,
    competitors.`競争相手平均1位完走率`,
    competitors.`競争相手1位完走率標準偏差`,
    competitors.`競争相手最高トップ3完走率`,
    competitors.`競争相手最低トップ3完走率`,
    competitors.`競争相手平均トップ3完走率`,
    competitors.`競争相手トップ3完走率標準偏差`,
    competitors.`競争相手最高過去5走勝率`,
    competitors.`競争相手最低過去5走勝率`,
    competitors.`競争相手平均過去5走勝率`,
    competitors.`競争相手過去5走勝率標準偏差`,
    competitors.`競争相手最高過去5走トップ3完走率`,
    competitors.`競争相手最低過去5走トップ3完走率`,
    competitors.`競争相手平均過去5走トップ3完走率`,
    competitors.`競争相手過去5走トップ3完走率標準偏差`,
    competitors.`競争相手最高場所レース数`,
    competitors.`競争相手最低場所レース数`,
    competitors.`競争相手平均場所レース数`,
    competitors.`競争相手場所レース数標準偏差`,
    competitors.`競争相手最高場所1位完走`,
    competitors.`競争相手最低場所1位完走`,
    competitors.`競争相手平均場所1位完走`,
    competitors.`競争相手場所1位完走標準偏差`,
    competitors.`競争相手最高場所トップ3完走`,
    competitors.`競争相手最低場所トップ3完走`,
    competitors.`競争相手平均場所トップ3完走`,
    competitors.`競争相手場所トップ3完走標準偏差`,
    competitors.`競争相手最高場所1位完走率`,
    competitors.`競争相手最低場所1位完走率`,
    competitors.`競争相手平均場所1位完走率`,
    competitors.`競争相手場所1位完走率標準偏差`,
    competitors.`競争相手最高場所トップ3完走率`,
    competitors.`競争相手最低場所トップ3完走率`,
    competitors.`競争相手平均場所トップ3完走率`,
    competitors.`競争相手場所トップ3完走率標準偏差`,
    competitors.`競争相手最高トラック種別レース数`,
    competitors.`競争相手最低トラック種別レース数`,
    competitors.`競争相手平均トラック種別レース数`,
    competitors.`競争相手トラック種別レース数標準偏差`,
    competitors.`競争相手最高トラック種別1位完走`,
    competitors.`競争相手最低トラック種別1位完走`,
    competitors.`競争相手平均トラック種別1位完走`,
    competitors.`競争相手トラック種別1位完走標準偏差`,
    competitors.`競争相手最高トラック種別トップ3完走`,
    competitors.`競争相手最低トラック種別トップ3完走`,
    competitors.`競争相手平均トラック種別トップ3完走`,
    competitors.`競争相手トラック種別トップ3完走標準偏差`,
    competitors.`競争相手最高馬場状態レース数`,
    competitors.`競争相手最低馬場状態レース数`,
    competitors.`競争相手平均馬場状態レース数`,
    competitors.`競争相手馬場状態レース数標準偏差`,
    competitors.`競争相手最高馬場状態1位完走`,
    competitors.`競争相手最低馬場状態1位完走`,
    competitors.`競争相手平均馬場状態1位完走`,
    competitors.`競争相手馬場状態1位完走標準偏差`,
    competitors.`競争相手最高馬場状態トップ3完走`,
    competitors.`競争相手最低馬場状態トップ3完走`,
    competitors.`競争相手平均馬場状態トップ3完走`,
    competitors.`競争相手馬場状態トップ3完走標準偏差`,
    competitors.`競争相手最高距離レース数`,
    competitors.`競争相手最低距離レース数`,
    competitors.`競争相手平均距離レース数`,
    competitors.`競争相手距離レース数標準偏差`,
    competitors.`競争相手最高距離1位完走`,
    competitors.`競争相手最低距離1位完走`,
    competitors.`競争相手平均距離1位完走`,
    competitors.`競争相手距離1位完走標準偏差`,
    competitors.`競争相手最高距離トップ3完走`,
    competitors.`競争相手最低距離トップ3完走`,
    competitors.`競争相手平均距離トップ3完走`,
    competitors.`競争相手距離トップ3完走標準偏差`,
    competitors.`競争相手最高四半期レース数`,
    competitors.`競争相手最低四半期レース数`,
    competitors.`競争相手平均四半期レース数`,
    competitors.`競争相手四半期レース数標準偏差`,
    competitors.`競争相手最高四半期1位完走`,
    competitors.`競争相手最低四半期1位完走`,
    competitors.`競争相手平均四半期1位完走`,
    competitors.`競争相手四半期1位完走標準偏差`,
    competitors.`競争相手最高四半期トップ3完走`,
    competitors.`競争相手最低四半期トップ3完走`,
    competitors.`競争相手平均四半期トップ3完走`,
    competitors.`競争相手四半期トップ3完走標準偏差`,
    competitors.`競争相手最高過去3走順位平方和`,
    competitors.`競争相手最低過去3走順位平方和`,
    competitors.`競争相手平均過去3走順位平方和`,
    competitors.`競争相手過去3走順位平方和標準偏差`,
    competitors.`競争相手最高本賞金累計`,
    competitors.`競争相手最低本賞金累計`,
    competitors.`競争相手平均本賞金累計`,
    competitors.`競争相手本賞金累計標準偏差`,
    competitors.`競争相手最高1位完走平均賞金`,
    competitors.`競争相手最低1位完走平均賞金`,
    competitors.`競争相手平均1位完走平均賞金`,
    competitors.`競争相手1位完走平均賞金標準偏差`,
    competitors.`競争相手最高レース数平均賞金`,
    competitors.`競争相手最低レース数平均賞金`,
    competitors.`競争相手平均レース数平均賞金`,
    competitors.`競争相手レース数平均賞金標準偏差`,
    competitors.`競争相手瞬発戦好走馬_芝割合`,
    competitors.`競争相手消耗戦好走馬_芝割合`,
    competitors.`競争相手瞬発戦好走馬_ダート割合`,
    competitors.`競争相手消耗戦好走馬_ダート割合`,
    competitors.`競争相手瞬発戦好走馬_総合割合`,
    competitors.`競争相手消耗戦好走馬_総合割合`,
    competitors.`競争相手最高連続1着`,
    competitors.`競争相手最低連続1着`,
    competitors.`競争相手平均連続1着`,
    competitors.`競争相手連続1着標準偏差`,
    competitors.`競争相手最高連続3着内`,
    competitors.`競争相手最低連続3着内`,
    competitors.`競争相手平均連続3着内`,
    competitors.`競争相手連続3着内標準偏差`,

    -- Relative to competitors
    race_horses.`一走前着順` - competitors.`競争相手平均一走前着順` as `競争相手平均一走前着順差`,
    race_horses.`二走前着順` - competitors.`競争相手平均二走前着順` as `競争相手平均二走前着順差`,
    race_horses.`三走前着順` - competitors.`競争相手平均三走前着順` as `競争相手平均三走前着順差`,
    race_horses.`四走前着順` - competitors.`競争相手平均四走前着順` as `競争相手平均四走前着順差`,
    race_horses.`五走前着順` - competitors.`競争相手平均五走前着順` as `競争相手平均五走前着順差`,
    race_horses.`六走前着順` - competitors.`競争相手平均六走前着順` as `競争相手平均六走前着順差`,
    race_horses.`入厩何日前` - competitors.`競争相手平均入厩何日前` as `競争相手平均入厩何日前差`,
    race_horses.`馬体重` - competitors.`競争相手平均馬体重` as `競争相手平均馬体重差`,
    race_horses.`馬体重増減` - competitors.`競争相手平均馬体重増減` as `競争相手平均馬体重増減差`,
    race_horses.`前走距離差` - competitors.`競争相手平均前走距離差` as `競争相手平均前走距離差差`,
    race_horses.`年齢` - competitors.`競争相手平均年齢` as `競争相手平均年齢差`,
    race_horses.`レース数` - competitors.`競争相手平均レース数` as `競争相手平均レース数差`,
    race_horses.`1位完走` - competitors.`競争相手平均1位完走` as `競争相手平均1位完走差`,
    race_horses.`トップ3完走` - competitors.`競争相手平均トップ3完走` as `競争相手平均トップ3完走差`,
    race_horses.`1位完走率` - competitors.`競争相手平均1位完走率` as `競争相手平均1位完走率差`,
    race_horses.`トップ3完走率` - competitors.`競争相手平均トップ3完走率` as `競争相手平均トップ3完走率差`,
    race_horses.`過去5走勝率` - competitors.`競争相手平均過去5走勝率` as `競争相手平均過去5走勝率差`,
    race_horses.`過去5走トップ3完走率` - competitors.`競争相手平均過去5走トップ3完走率` as `競争相手平均過去5走トップ3完走率差`,
    race_horses.`場所レース数` - competitors.`競争相手平均場所レース数` as `競争相手平均場所レース数差`,
    race_horses.`場所1位完走` - competitors.`競争相手平均場所1位完走` as `競争相手平均場所1位完走差`,
    race_horses.`場所トップ3完走` - competitors.`競争相手平均場所トップ3完走` as `競争相手平均場所トップ3完走差`,
    race_horses.`場所1位完走率` - competitors.`競争相手平均場所1位完走率` as `競争相手平均場所1位完走率差`,
    race_horses.`場所トップ3完走率` - competitors.`競争相手平均場所トップ3完走率` as `競争相手平均場所トップ3完走率差`,
    race_horses.`トラック種別レース数` - competitors.`競争相手平均トラック種別レース数` as `競争相手平均トラック種別レース数差`,
    race_horses.`トラック種別1位完走` - competitors.`競争相手平均トラック種別1位完走` as `競争相手平均トラック種別1位完走差`,
    race_horses.`トラック種別トップ3完走` - competitors.`競争相手平均トラック種別トップ3完走` as `競争相手平均トラック種別トップ3完走差`,
    race_horses.`馬場状態レース数` - competitors.`競争相手平均馬場状態レース数` as `競争相手平均馬場状態レース数差`,
    race_horses.`馬場状態1位完走` - competitors.`競争相手平均馬場状態1位完走` as `競争相手平均馬場状態1位完走差`,
    race_horses.`馬場状態トップ3完走` - competitors.`競争相手平均馬場状態トップ3完走` as `競争相手平均馬場状態トップ3完走差`,
    race_horses.`距離レース数` - competitors.`競争相手平均距離レース数` as `競争相手平均距離レース数差`,
    race_horses.`距離1位完走` - competitors.`競争相手平均距離1位完走` as `競争相手平均距離1位完走差`,
    race_horses.`距離トップ3完走` - competitors.`競争相手平均距離トップ3完走` as `競争相手平均距離トップ3完走差`,
    race_horses.`四半期レース数` - competitors.`競争相手平均四半期レース数` as `競争相手平均四半期レース数差`,
    race_horses.`四半期1位完走` - competitors.`競争相手平均四半期1位完走` as `競争相手平均四半期1位完走差`,
    race_horses.`四半期トップ3完走` - competitors.`競争相手平均四半期トップ3完走` as `競争相手平均四半期トップ3完走差`,
    race_horses.`過去3走順位平方和` - competitors.`競争相手平均過去3走順位平方和` as `競争相手平均過去3走順位平方和差`,
    race_horses.`本賞金累計` - competitors.`競争相手平均本賞金累計` as `競争相手平均本賞金累計差`,
    race_horses.`1位完走平均賞金` - competitors.`競争相手平均1位完走平均賞金` as `競争相手平均1位完走平均賞金差`,
    race_horses.`レース数平均賞金` - competitors.`競争相手平均レース数平均賞金` as `競争相手平均レース数平均賞金差`,
    race_horses.`連続1着` - competitors.`競争相手平均連続1着` as `競争相手平均連続1着差`,
    race_horses.`連続3着内` - competitors.`競争相手平均連続3着内` as `競争相手平均連続3着内差`

  from
    race_horses
  -- 馬はどうするか。。inner joinだと初走の馬は結果に出てこなくなる
  -- 初走の馬にかけても意味がないので、inner joinでいい
  inner join
    competitors
  on
    race_horses.`レースキー` = competitors.`レースキー`
    and race_horses.`馬番` = competitors.`馬番`
  )

select
  *
from
  final
