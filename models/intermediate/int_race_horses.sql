{{
  config(
    materialized='table',
    schema='intermediate',
    indexes=[{'columns': ['レースキー', '馬番'], 'unique': True}]
  )
}}

with recursive
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
    (血統登録番号, データ年月日) in (select 血統登録番号, MAX(データ年月日) from ukc group by 血統登録番号)
  ),

  good_finish_turf as (
  select
    "競走成績キー_血統登録番号" as "血統登録番号",
    sum(
      case
        when "ＪＲＤＢデータ_馬場差" <= -10 and "馬成績_着順" <= 3 then 1
        else 0
      end
    ) > 0 "瞬発戦好走馬",
    sum(
      case
  	    when "ＪＲＤＢデータ_馬場差" >= 10 and "馬成績_着順" <= 3 then 1
  	    else 0
      end
    ) > 0 "消耗戦好走馬"
  from
    sed
  where
    "馬成績_異常区分" = '0'
    and レース条件_トラック情報_芝ダ障害コード = '芝'
  group by
    "血統登録番号"
  ),

  good_finish_dirt as (
  select
    "競走成績キー_血統登録番号" as "血統登録番号",
    sum(
      case
        when "ＪＲＤＢデータ_馬場差" <= -10 and "馬成績_着順" <= 3 then 1
        else 0
      end
    ) > 0 "瞬発戦好走馬",
    sum(
      case
  	    when "ＪＲＤＢデータ_馬場差" >= 10 and "馬成績_着順" <= 3 then 1
  	    else 0
      end
    ) > 0 "消耗戦好走馬"
  from
    sed
  where
    "馬成績_異常区分" = '0'
    and レース条件_トラック情報_芝ダ障害コード = 'ダート'
  group by
    "血統登録番号"
  ),

  good_finish_any as (
  select
    "競走成績キー_血統登録番号" as "血統登録番号",
    sum(
      case
        when "ＪＲＤＢデータ_馬場差" <= -10 and "馬成績_着順" <= 3 then 1
        else 0
      end
    ) > 0 "瞬発戦好走馬",
    sum(
      case
  	    when "ＪＲＤＢデータ_馬場差" >= 10 and "馬成績_着順" <= 3 then 1
  	    else 0
      end
    ) > 0 "消耗戦好走馬"
  from
    sed
  where
    "馬成績_異常区分" = '0'
  group by
    "血統登録番号"
  ),

  horses as (
  select
    ukc."血統登録番号",
    case
      when ukc."性別コード" = '1' then '牡'
      when ukc."性別コード" = '2' then '牝'
      else 'セン'
    end "性別",
    ukc."生年月日" as "生年月日",
    good_finish_turf."瞬発戦好走馬" as "瞬発戦好走馬_芝",
    good_finish_turf."消耗戦好走馬" as "消耗戦好走馬_芝",
    good_finish_dirt."瞬発戦好走馬" as "瞬発戦好走馬_ダート",
    good_finish_dirt."消耗戦好走馬" as "消耗戦好走馬_ダート",
    good_finish_any."瞬発戦好走馬" as "瞬発戦好走馬_総合",
    good_finish_any."消耗戦好走馬" as "消耗戦好走馬_総合"
  from
    ukc_latest as ukc
  left join
    good_finish_turf 
  on
    ukc."血統登録番号" = good_finish_turf."血統登録番号"
  left join
    good_finish_dirt
  on
    ukc."血統登録番号" = good_finish_dirt."血統登録番号"
  left join
    good_finish_any
  on
    ukc."血統登録番号" = good_finish_any."血統登録番号"
  ),

  race_horses_base as (
  select
    kyi."レースキー",
    kyi."馬番",
    kyi."枠番",
    bac."年月日",
    kyi."レースキー_場コード" as "場コード",
    case
      when extract(month from bac."年月日") <= 3 then 1
      when extract(month from bac."年月日") <= 6 then 2
      when extract(month from bac."年月日") <= 9 then 3
      when extract(month from bac."年月日") <= 12 then 4
    end as "四半期",
    kyi."血統登録番号",
    kyi."入厩年月日",
    horses."生年月日",
    -- Assumption: TYB is available (~15 minutes before race)
    tyb."馬体重",
    tyb."馬体重増減",
    bac."レース条件_距離" as "距離",
    sed."本賞金",
    coalesce(
      tyb."馬場状態コード",
      case
        -- 障害コースの場合は芝馬場状態を使用する
        when bac."レース条件_トラック情報_芝ダ障害コード" = 'ダート' then kab."ダ馬場状態コード"
        else kab."芝馬場状態コード"
      end
    ) as "馬場状態コード",
    bac."頭数",
    bac."レース条件_トラック情報_芝ダ障害コード" as "トラック種別",
    sed."馬成績_着順" as "着順",
    lag(sed."馬成績_着順", 1) over (partition by kyi."血統登録番号" order by bac."年月日") as "一走前着順",
    lag(sed."馬成績_着順", 2) over (partition by kyi."血統登録番号" order by bac."年月日") as "二走前着順",
    lag(sed."馬成績_着順", 3) over (partition by kyi."血統登録番号" order by bac."年月日") as "三走前着順",
    lag(sed."馬成績_着順", 4) over (partition by kyi."血統登録番号" order by bac."年月日") as "四走前着順",
    lag(sed."馬成績_着順", 5) over (partition by kyi."血統登録番号" order by bac."年月日") as "五走前着順",
    lag(sed."馬成績_着順", 6) over (partition by kyi."血統登録番号" order by bac."年月日") as "六走前着順",
    -- how many races this horse has run until now
    row_number() over (partition by kyi."血統登録番号" order by bac."年月日") - 1 as "レース数",
    -- how many races this horse has won until now (incremented by one on the following race)
    coalesce(
      cast(
        sum(
          case
            when sed."馬成績_着順" = 1 then 1
            else 0
          end
        ) over (partition by kyi."血統登録番号" order by bac."年月日" rows between unbounded preceding and 1 preceding) as integer
      ), 0) as "1位完走", -- horse_wins
    -- nth race (used to calculate streaks)
    row_number() over (partition by kyi."血統登録番号" order by bac."年月日") as rn
  from
    kyi

  -- 前日系は inner join
  inner join
    bac
  on
    kyi."レースキー" = bac."レースキー"

  -- 実績系はレースキーがないかもしれないから left join
  left join
    sed
  on
    kyi."レースキー" = sed."レースキー"
    and kyi."馬番" = sed."馬番"

  -- TYBが公開される前に予測する可能性があるから left join
  left join
    tyb
  on
    kyi."レースキー" = tyb."レースキー"
    and kyi."馬番" = tyb."馬番"

  inner join
    kab
  on
    kyi."開催キー" = kab."開催キー"
    and bac."年月日" = kab."年月日"

  inner join
    horses
  on
    kyi."血統登録番号" = horses."血統登録番号"
  ),

  race_horses_streaks as (
  -- Base case: Initialize the first row of the CTE for each horse
  select
    "レースキー",
    "馬番",
    "血統登録番号",
    rn,
    case when "着順" = 1 then 1 else 0 end as "連続1着",
    case when "着順" <= 3 then 1 else 0 end as "連続3着以内"
  from
    race_horses_base
  where
    rn = 1

  union all

  -- Recursive step: Calculate streaks by comparing with the previous row
  select
    r."レースキー",
    r."馬番",
    r."血統登録番号",
    r.rn,
    case
      when r."着順" = 1 then c."連続1着" + 1 -- Increase streak if win
      else 0 -- Reset streak if not a win
    end as "連続1着",
    case
      when r."着順" <= 3 then c."連続3着以内" + 1
      else 0
    end as "連続3着以内"
  from
    race_horses_base r
  inner join
    race_horses_streaks c
  on
    r."血統登録番号" = c."血統登録番号"
    and r.rn = c.rn + 1
  ),

  -- 参考:
  -- https://github.com/codeworks-data/mvp-horse-racing-prediction/blob/master/extract_features.py#L73
  -- https://medium.com/codeworksparis/horse-racing-prediction-a-machine-learning-approach-part-2-e9f5eb9a92e9
  race_horses as (
  select
    base."レースキー",
    base."馬番",
    base."血統登録番号",
    base."年月日",

    "一走前着順",
    "二走前着順",
    "三走前着順",
    "四走前着順",
    "五走前着順",
    "六走前着順",

    -- whether the horse placed in the previous race
    -- last_place
    case
      when lag("着順") over (partition by base."血統登録番号" order by "年月日") <= 3 then true
      else false
    end as "前走トップ3",

    -- previous race draw
    lag("枠番") over (partition by base."血統登録番号" order by "年月日") as "前走枠番", -- last_draw

    -- horse_rest_time
    "年月日" - "入厩年月日" as "入厩何日前",

    -- horse_rest_lest14
    "年月日" - "入厩年月日" < 15 as "入厩15日未満",

    -- horse_rest_over35
    "年月日" - "入厩年月日" >= 35 as "入厩35日以上",

    -- declared_weight
    "馬体重",

    -- diff_declared_weight
    -- todo: calculate yourself and confirm match
    "馬体重増減" as "馬体重増減",

    -- distance
    "距離",

    -- diff_distance from previous race
    coalesce("距離" - lag("距離") over (partition by base."血統登録番号" order by "年月日"), 0) as "前走距離差",

    -- horse_age in years
    extract(year from age("年月日", "生年月日")) + extract(month from age("年月日", "生年月日")) / 12 + extract(day from age("年月日", "生年月日")) / (12 * 30.44) AS "年齢",

    -- horse_age_4 or less
    age("年月日", "生年月日") < '5 years' as "4歳以下",

    sum(
      case
        when age("年月日", "生年月日") < '5 years' then 1
        else 0
      end
    ) over (partition by base."レースキー") as "4歳以下頭数",

    coalesce(
      sum(
        case
          when age("年月日", "生年月日") < '5 years' then 1
          else 0
        end
      ) over (partition by base."レースキー") / cast("頭数" as numeric), 0) as "4歳以下割合",

    "レース数", -- horse_runs
    "1位完走", -- horse_wins

    -- how many races this horse has placed in until now (incremented by one on the following race)
    coalesce(cast(sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号" order by "年月日" rows between unbounded preceding and 1 preceding) as integer), 0) as "トップ3完走", -- horse_places

    -- ratio_win_horse
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by base."血統登録番号" order by "年月日" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号" order by "年月日") - 1 as numeric)'
      )
    }}, 0) as "1位完走率",

    -- ratio_place_horse
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号" order by "年月日" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号" order by "年月日") - 1 as numeric)'
      )
    }}, 0) as "トップ3完走率",

    -- Horse Win Percent: Horse’s win percent over the past 5 races.
    -- horse_win_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by base."血統登録番号" order by "年月日" rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号" order by "年月日" rows between 5 preceding and 1 preceding) - 1 as numeric)'
      )
    }}, 0) as "過去5走勝率",

    -- horse_place_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号" order by "年月日" rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号" order by "年月日" rows between 5 preceding and 1 preceding) - 1 as numeric)'
      )
    }}, 0) as "過去5走トップ3完走率",

    -- horse_venue_runs
    coalesce(cast(count(*) over (partition by base."血統登録番号", "場コード" order by "年月日") - 1 as integer), 0) as "場所レース数",

    -- horse_venue_wins
    coalesce(cast(sum(case when "着順" = 1 then 1 else 0 end) over (partition by base."血統登録番号", "場コード" order by "年月日" rows between unbounded preceding and 1 preceding) as integer), 0) as "場所1位完走",

    -- horse_venue_places
    coalesce(cast(sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号", "場コード" order by "年月日" rows between unbounded preceding and 1 preceding) as integer), 0) as "場所トップ3完走",

    -- ratio_win_horse_venue
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by base."血統登録番号", "場コード" order by "年月日" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号", "場コード" order by "年月日") - 1 as numeric)'
      )
    }}, 0) as "場所1位完走率",

    -- ratio_place_horse_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号", "場コード" order by "年月日" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号", "場コード" order by "年月日") - 1 as numeric)'
      )
    }}, 0) as "場所トップ3完走率",

    -- horse_surface_runs
    coalesce(cast(count(*) over (partition by base."血統登録番号", "トラック種別" order by "年月日") - 1 as integer), 0) as "トラック種別レース数",

    -- horse_surface_wins
    coalesce(cast(sum(case when "着順" = 1 then 1 else 0 end) over (partition by base."血統登録番号", "トラック種別" order by "年月日" rows between unbounded preceding and 1 preceding) as integer), 0) as "トラック種別1位完走",

    -- horse_surface_places
    coalesce(cast(sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号", "トラック種別" order by "年月日" rows between unbounded preceding and 1 preceding) as integer), 0) as "トラック種別トップ3完走",

    -- ratio_win_horse_surface
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by base."血統登録番号", "トラック種別" order by "年月日" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号", "トラック種別" order by "年月日") - 1 as numeric)'
      )
    }}, 0) as "トラック種別1位完走率",

    -- ratio_place_horse_surface
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号", "トラック種別" order by "年月日" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号", "トラック種別" order by "年月日") - 1 as numeric)'
      )
    }}, 0) as "トラック種別トップ3完走率",

    -- horse_going_runs
    coalesce(cast(count(*) over (partition by base."血統登録番号", "馬場状態コード" order by "年月日") - 1 as integer), 0) as "馬場状態レース数",

    -- horse_going_wins
    coalesce(cast(sum(case when "着順" = 1 then 1 else 0 end) over (partition by base."血統登録番号", "馬場状態コード" order by "年月日" rows between unbounded preceding and 1 preceding) as integer), 0) as "馬場状態1位完走",

    -- horse_going_places
    coalesce(cast(sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号", "馬場状態コード" order by "年月日" rows between unbounded preceding and 1 preceding) as integer), 0) as "馬場状態トップ3完走",

    -- ratio_win_horse_going
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by base."血統登録番号", "馬場状態コード" order by "年月日" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号", "馬場状態コード" order by "年月日") - 1 as numeric)'
      )
    }}, 0) as "馬場状態1位完走率",

    -- ratio_place_horse_going
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号", "馬場状態コード" order by "年月日" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号", "馬場状態コード" order by "年月日") - 1 as numeric)'
      )
    }}, 0) as "馬場状態トップ3完走率",

    -- horse_distance_runs
    coalesce(cast(count(*) over (partition by base."血統登録番号", "距離" order by "年月日") - 1 as integer), 0) as "距離レース数",

    -- horse_distance_wins
    coalesce(cast(sum(case when "着順" = 1 then 1 else 0 end) over (partition by base."血統登録番号", "距離" order by "年月日" rows between unbounded preceding and 1 preceding) as integer), 0) as "距離1位完走",

    -- horse_distance_places
    coalesce(cast(sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号", "距離" order by "年月日" rows between unbounded preceding and 1 preceding) as integer), 0) as "距離トップ3完走",

    -- ratio_win_horse_distance
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by base."血統登録番号", "距離" order by "年月日" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号", "距離" order by "年月日") - 1 as numeric)'
      )
    }}, 0) as "距離1位完走率",

    -- ratio_place_horse_distance
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号", "距離" order by "年月日" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号", "距離" order by "年月日") - 1 as numeric)'
      )
    }}, 0) as "距離トップ3完走率",

    -- horse_quarter_runs
    coalesce(cast(count(*) over (partition by base."血統登録番号", "四半期" order by "年月日") - 1 as integer), 0) as "四半期レース数",

    -- horse_quarter_wins
    coalesce(cast(sum(case when "着順" = 1 then 1 else 0 end) over (partition by base."血統登録番号", "四半期" order by "年月日" rows between unbounded preceding and 1 preceding) as integer), 0) as "四半期1位完走",

    -- horse_quarter_places
    coalesce(cast(sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号", "四半期" order by "年月日" rows between unbounded preceding and 1 preceding) as integer), 0) as "四半期トップ3完走",

    -- ratio_win_horse_quarter
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by base."血統登録番号", "四半期" order by "年月日" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号", "四半期" order by "年月日") - 1 as numeric)'
      )
    }}, 0) as "四半期1位完走率",

    -- ratio_place_horse_quarter
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by base."血統登録番号", "四半期" order by "年月日" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base."血統登録番号", "四半期" order by "年月日") - 1 as numeric)'
      )
    }}, 0) as "四半期トップ3完走率",

    -- Compute the standard rank of the horse on his last 3 races giving us an overview of his state of form
    cast(coalesce(power("一走前着順" - 1, 2) + power("二走前着順" - 1, 2) + power("三走前着順" - 1, 2), 0) as integer) as "過去3走順位平方和", -- horse_std_rank

    -- prize_horse_cumulative
    coalesce(sum("本賞金") over (partition by base."血統登録番号" order by "年月日" rows between unbounded preceding and 1 preceding), 0) as "本賞金累計",

    -- avg_prize_wins_horse
    coalesce({{
      dbt_utils.safe_divide(
        'sum("本賞金") over (partition by base."血統登録番号" order by "年月日" rows between unbounded preceding and 1 preceding)',
        '"1位完走"'
      )
    }}, 0) as "1位完走平均賞金",

    -- avg_prize_runs_horse
    coalesce({{
      dbt_utils.safe_divide(
        'sum("本賞金") over (partition by base."血統登録番号" order by "年月日" rows between unbounded preceding and 1 preceding)',
        '"レース数"'
      )
    }}, 0) as "レース数平均賞金",

    lag(race_horses_streaks."連続1着", 1, 0) over (partition by base."血統登録番号" order by "年月日") as "連続1着",
    lag(race_horses_streaks."連続3着以内", 1, 0) over (partition by base."血統登録番号" order by "年月日") as "連続3着以内"

  from
    race_horses_base base
  inner join
    race_horses_streaks
  on
    race_horses_streaks."レースキー" = base."レースキー"
    and race_horses_streaks."馬番" = base."馬番"
  ),

  final as (
  select
    race_horses."レースキー",
    race_horses."馬番",
    race_horses."年月日",
    race_horses."一走前着順",
    race_horses."二走前着順",
    race_horses."三走前着順",
    race_horses."四走前着順",
    race_horses."五走前着順",
    race_horses."六走前着順",
    race_horses."前走トップ3",
    race_horses."前走枠番",
    race_horses."入厩何日前", -- horse_rest_time
    race_horses."入厩15日未満", -- horse_rest_lest14
    race_horses."入厩35日以上", -- horse_rest_over35
    race_horses."馬体重", -- declared_weight
    race_horses."馬体重増減", -- diff_declared_weight (todo: check if this matches lag)
    race_horses."距離", -- distance
    race_horses."前走距離差", -- diff_distance
    race_horses."年齢", -- horse_age (years)
    race_horses."4歳以下",
    race_horses."4歳以下頭数",
    race_horses."4歳以下割合",
    race_horses."レース数", -- horse_runs
    race_horses."1位完走", -- horse_wins
    race_horses."トップ3完走", -- horse_places
    race_horses."1位完走率",
    race_horses."トップ3完走率",
    race_horses."過去5走勝率",
    race_horses."過去5走トップ3完走率",
    race_horses."場所レース数", -- horse_venue_runs
    race_horses."場所1位完走", -- horse_venue_wins
    race_horses."場所トップ3完走", -- horse_venue_places
    race_horses."場所1位完走率", -- ratio_win_horse_venue
    race_horses."場所トップ3完走率", -- ratio_place_horse_venue
    race_horses."トラック種別レース数", -- horse_surface_runs
    race_horses."トラック種別1位完走", -- horse_surface_wins
    race_horses."トラック種別トップ3完走", -- horse_surface_places
    race_horses."トラック種別1位完走率", -- ratio_win_horse_surface
    race_horses."トラック種別トップ3完走率", -- ratio_place_horse_surface
    race_horses."馬場状態レース数", -- horse_going_runs
    race_horses."馬場状態1位完走", -- horse_going_wins
    race_horses."馬場状態トップ3完走", -- horse_going_places
    race_horses."馬場状態1位完走率", -- ratio_win_horse_going
    race_horses."馬場状態トップ3完走率", -- ratio_place_horse_going
    race_horses."距離レース数", -- horse_distance_runs
    race_horses."距離1位完走", -- horse_distance_wins
    race_horses."距離トップ3完走", -- horse_distance_places
    race_horses."距離1位完走率", -- ratio_win_horse_distance
    race_horses."距離トップ3完走率", -- ratio_place_horse_distance
    race_horses."四半期レース数", -- horse_quarter_runs
    race_horses."四半期1位完走", -- horse_quarter_wins
    race_horses."四半期トップ3完走", -- horse_quarter_places
    race_horses."四半期1位完走率", -- ratio_win_horse_quarter
    race_horses."四半期トップ3完走率", -- ratio_place_horse_quarter
    race_horses."過去3走順位平方和",
    race_horses."本賞金累計",
    race_horses."1位完走平均賞金",
    race_horses."レース数平均賞金",
    race_horses."連続1着",
    race_horses."連続3着以内",

    horses."血統登録番号",
    horses."瞬発戦好走馬_芝",
    horses."消耗戦好走馬_芝",
    horses."瞬発戦好走馬_ダート",
    horses."消耗戦好走馬_ダート",
    horses."瞬発戦好走馬_総合",
    horses."消耗戦好走馬_総合",
    horses."性別"

  from
    race_horses
  -- 馬はどうするか。。inner joinだと初走の馬は結果に出てこなくなる
  -- 初走の馬にかけても意味がないので、inner joinでいい
  inner join
    horses
  on
    race_horses."血統登録番号" = horses."血統登録番号"
  )

select
  *
from
  final
