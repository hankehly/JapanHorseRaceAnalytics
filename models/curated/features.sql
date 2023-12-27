{{ config(materialized='table') }}
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

  skb as (
  select
    *
  from
    {{ ref('stg_jrdb__skb') }}
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

  horses as (
  select
    *
  from
    {{ ref('int_horses') }}
  ),

  win_odds as (
  select
    *
  from
    {{ ref('int_win_odds') }}
  ),

  place_odds as (
  select
    *
  from
    {{ ref('int_place_odds') }}
  ),

  win_payouts as (
  select
    *
  from
    {{ ref('int_win_payouts') }}
  ),

  place_payouts as (
  select
      *
  from
      {{ ref('int_place_payouts') }}
  ),

  base as (
  select
    kyi."レースキー",
    kyi."馬番",
    kyi."枠番",
    kab."場名",
    bac."年月日",
    kyi."レースキー_場コード" as "場コード",
    sed."騎手コード",
    sed."調教師コード",
    sed."レースキー_Ｒ",

    case when extract(month from bac."年月日") <= 3 then 1
         when extract(month from bac."年月日") <= 6 then 2
         when extract(month from bac."年月日") <= 9 then 3
         when extract(month from bac."年月日") <= 12 then 4
    end as "四半期",

    sed."馬成績_着順" as "着順",
    kyi."血統登録番号",
    kyi."入厩年月日",
    sed."馬体重",
    sed."馬体重増減",
    sed."レース条件_距離" as "距離",
    skb."馬具コード",
    sed."レース条件_馬場状態" as "馬場状態",
    sed."本賞金",
    sed."収得賞金",

    bac."頭数",
    bac."レース条件_トラック情報_芝ダ障害コード" as "トラック種別",
    horses."生年月日",
    horses."瞬発戦好走馬_芝",
    horses."消耗戦好走馬_芝",
    horses."瞬発戦好走馬_ダート",
    horses."消耗戦好走馬_ダート",
    horses."瞬発戦好走馬_総合",
    horses."消耗戦好走馬_総合",
    horses."性別",

    kab."芝馬場差" as "前日_芝馬場差",
    kab."ダ馬場差" as "前日_ダ馬場差",
    kyi."ＩＤＭ" as "前日_ＩＤＭ",
    (SELECT "name" FROM {{ ref('脚質コード') }} WHERE "code" = kyi."脚質") as "前日_脚質",
    win_odds."単勝オッズ" as "前日_単勝オッズ",
    place_odds."複勝オッズ" as "前日_複勝オッズ",

    tyb."ＩＤＭ" as "直前_ＩＤＭ",
    tyb."騎手指数" as "直前_騎手指数",
    tyb."情報指数" as "直前_情報指数",
    tyb."オッズ指数" as "直前_オッズ指数",
    tyb."パドック指数" as "直前_パドック指数",
    tyb."脚元情報" as "直前_脚元情報",
    (SELECT "weather_condition" FROM {{ ref('天候コード') }} WHERE "code" = tyb."天候コード") as "直前_天候",
    tyb."単勝オッズ" as "直前_単勝オッズ",
    tyb."複勝オッズ" as "直前_複勝オッズ",

    lag(sed."馬成績_着順", 1) over (partition by kyi."血統登録番号" order by bac."年月日") as "前走着順",
    lag(sed."馬成績_着順", 2) over (partition by kyi."血統登録番号" order by bac."年月日") as "前々走着順",
    lag(sed."馬成績_着順", 3) over (partition by kyi."血統登録番号" order by bac."年月日") as "前々々走着順",

    coalesce(win_payouts."払戻金", 0) > 0 as "単勝的中",
    coalesce(win_payouts."払戻金", 0) as "単勝払戻金",
    coalesce(place_payouts."払戻金", 0) > 0 as "複勝的中",
    coalesce(place_payouts."払戻金", 0) as "複勝払戻金"
  from
    kyi

  inner join
    bac
  on
    kyi."レースキー" = bac."レースキー"

  -- Note: SED lags behind KYI by a few days, so the most recent races will be missing
  -- This means you shouldn't use SED fields as-is, but rather use them to calculate
  -- features based off of past performance
  inner join
    sed
  on
    kyi."レースキー" = sed."レースキー"
    and kyi."馬番" = sed."馬番"

  inner join
    skb
  on
    kyi."レースキー" = skb."レースキー"
    and kyi."馬番" = skb."馬番"

  inner join
    tyb
  on
    kyi."レースキー" = tyb."レースキー"
    and kyi."馬番" = tyb."馬番"

  inner join
    kab
  on
    kyi."開催キー" = kab."開催キー"

  inner join
    horses
  on
    kyi."血統登録番号" = horses."血統登録番号"

  left join
    win_odds
  on
    kyi."レースキー" = win_odds."レースキー"
    and kyi."馬番" = win_odds."馬番"

  left join
    place_odds
  on
    kyi."レースキー" = place_odds."レースキー"
    and kyi."馬番" = place_odds."馬番"

  left join
    win_payouts
  on
    kyi."レースキー" = win_payouts."レースキー"
    and kyi."馬番" = win_payouts."馬番"

  left join
    place_payouts
  on
    kyi."レースキー" = place_payouts."レースキー"
    and kyi."馬番" = place_payouts."馬番"
  ),

  final as (
  select
    -- 基本情報
    "レースキー",
    "馬番",
    "枠番",
    "場名",
    "年月日",
    "頭数",
    "四半期",

    -- 結果/払戻金
    "単勝的中",
    "単勝払戻金",
    "複勝的中",
    "複勝払戻金",
    "本賞金",
    "収得賞金",

    -- 馬情報
    "血統登録番号",
    "瞬発戦好走馬_芝",
    "消耗戦好走馬_芝",
    "瞬発戦好走馬_ダート",
    "消耗戦好走馬_ダート",
    "瞬発戦好走馬_総合",
    "消耗戦好走馬_総合",
    "性別",

    -- 前日
    "トラック種別",
    "前日_芝馬場差",
    "前日_ダ馬場差",
    "前日_ＩＤＭ",
    "前日_脚質",
    "前日_単勝オッズ",
    "前日_複勝オッズ",

    -- 直前
    "直前_ＩＤＭ",
    "直前_騎手指数",
    "直前_情報指数",
    "直前_オッズ指数",
    "直前_パドック指数",
    "直前_脚元情報",
    "直前_天候",
    "直前_単勝オッズ",
    "直前_複勝オッズ",

    -- 計算特徴料
    -- https://github.com/codeworks-data/mvp-horse-racing-prediction/blob/master/extract_features.py#L73
    -- https://medium.com/codeworksparis/horse-racing-prediction-a-machine-learning-approach-part-2-e9f5eb9a92e9

    -- whether the horse placed in the previous race
    case when lag("着順") over (partition by "血統登録番号" order by "年月日") <= 3 then true else false end as "前走トップ3", -- last_place

    -- previous race draw
    lag("枠番") over (partition by "血統登録番号" order by "年月日") as "前走枠番", -- last_draw

    "年月日" - "入厩年月日" as "入厩何日前", -- horse_rest_time
    "年月日" - "入厩年月日" < 15 as "入厩15日未満", -- horse_rest_lest14
    "年月日" - "入厩年月日" >= 35 as "入厩35日以上", -- horse_rest_over35
    "馬体重", -- declared_weight
    "馬体重増減" as "馬体重増減", -- diff_declared_weight
    "距離" as "距離", -- distance
    "距離" - lag("距離") over (partition by "血統登録番号" order by "年月日") as "前走距離差", -- diff_distance
    "馬具コード", -- horse_gear
    age("年月日", "生年月日") as "年齢",
    age("年月日", "生年月日") < '5 years' as "4歳以下",
    sum(case when age("年月日", "生年月日") < '5 years' then 1 else 0 end) over (partition by "レースキー") as "4歳以下頭数",
    sum(case when age("年月日", "生年月日") < '5 years' then 1 else 0 end) over (partition by "レースキー") / "頭数" as "4歳以下割合",

    -- how many races this horse has run until now
    count(*) over (partition by "血統登録番号" order by "年月日") - 1 as "レース数", -- horse_runs

    -- how many races this horse has won until now (incremented by one on the following race)
    sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号" order by "年月日") - cast("単勝的中" as integer) as "1位完走", -- horse_wins

    -- how many races this horse has placed in until now (incremented by one on the following race)
    sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号" order by "年月日") - cast("複勝的中" as integer) as "トップ3完走", -- horse_places

    -- ratio_win_horse
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号" order by "年月日") - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号" order by "年月日") - 1 as numeric)'
      )
    }} as "1位完走率",

    -- ratio_place_horse
    {{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号" order by "年月日") - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号" order by "年月日") - 1 as numeric)'
      )
    }} as "トップ3完走率",

    -- horse_venue_runs
    count(*) over (partition by "血統登録番号", "場コード" order by "年月日") - 1 as "場所レース数",

    -- horse_venue_wins
    sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "場コード" order by "年月日") - cast("単勝的中" as integer) as "場所1位完走",

    -- horse_venue_places
    sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "場コード" order by "年月日") - cast("複勝的中" as integer) as "場所トップ3完走",

    -- ratio_win_horse_venue
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "場コード" order by "年月日") - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号", "場コード" order by "年月日") - 1 as numeric)'
      )
    }} as "場所1位完走率",

    -- ratio_place_horse_venue
    {{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "場コード" order by "年月日") - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号", "場コード" order by "年月日") - 1 as numeric)'
      )
    }} as "場所トップ3完走率",

    -- horse_surface_runs
    count(*) over (partition by "血統登録番号", "トラック種別" order by "年月日") - 1 as "トラック種別レース数",

    -- horse_surface_wins
    sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "トラック種別" order by "年月日") - cast("単勝的中" as integer) as "トラック種別1位完走",

    -- horse_surface_places
    sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "トラック種別" order by "年月日") - cast("複勝的中" as integer) as "トラック種別トップ3完走",

    -- ratio_win_horse_surface
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "トラック種別" order by "年月日") - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号", "トラック種別" order by "年月日") - 1 as numeric)'
      )
    }} as "トラック種別1位完走率",

    -- ratio_place_horse_surface
    {{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "トラック種別" order by "年月日") - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号", "トラック種別" order by "年月日") - 1 as numeric)'
      )
    }} as "トラック種別トップ3完走率",

    -- レース条件_馬場状態
    -- horse_going_runs
    count(*) over (partition by "血統登録番号", "馬場状態" order by "年月日") - 1 as "馬場状態レース数",

    -- horse_going_wins
    sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "馬場状態" order by "年月日") - cast("単勝的中" as integer) as "馬場状態1位完走",

    -- horse_going_places
    sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "馬場状態" order by "年月日") - cast("複勝的中" as integer) as "馬場状態トップ3完走",

    -- ratio_win_horse_going
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "馬場状態" order by "年月日") - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号", "馬場状態" order by "年月日") - 1 as numeric)'
      )
    }} as "馬場状態1位完走率",

    -- ratio_place_horse_going
    {{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "馬場状態" order by "年月日") - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号", "馬場状態" order by "年月日") - 1 as numeric)'
      )
    }} as "馬場状態トップ3完走率",

    -- horse_distance_runs
    count(*) over (partition by "血統登録番号", "距離" order by "年月日") - 1 as "距離レース数",

    -- horse_distance_wins
    sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "距離" order by "年月日") - cast("単勝的中" as integer) as "距離1位完走",

    -- horse_distance_places
    sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "距離" order by "年月日") - cast("複勝的中" as integer) as "距離トップ3完走",

    -- ratio_win_horse_distance
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "距離" order by "年月日") - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号", "距離" order by "年月日") - 1 as numeric)'
      )
    }} as "距離1位完走率",

    -- ratio_place_horse_distance
    {{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "距離" order by "年月日") - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号", "距離" order by "年月日") - 1 as numeric)'
      )
    }} as "距離トップ3完走率",

    -- horse_quarter_runs
    count(*) over (partition by "血統登録番号", "四半期" order by "年月日") - 1 as "四半期レース数",

    -- horse_quarter_wins
    sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "四半期" order by "年月日") - cast("単勝的中" as integer) as "四半期1位完走",

    -- horse_quarter_places
    sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "四半期" order by "年月日") - cast("複勝的中" as integer) as "四半期トップ3完走",

    -- ratio_win_horse_quarter
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "四半期" order by "年月日") - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号", "四半期" order by "年月日") - 1 as numeric)'
      )
    }} as "四半期1位完走率",

    -- ratio_place_horse_quarter
    {{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "四半期" order by "年月日") - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号", "四半期" order by "年月日") - 1 as numeric)'
      )
    }} as "四半期トップ3完走率",

    -- jockey_runs
    count(*) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ") - 1 as "騎手レース数",

    -- jockey_wins
    sum(case when "着順" = 1 then 1 else 0 end) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ") - cast("単勝的中" as integer) as "騎手1位完走",

    -- jockey_places
    sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ") - cast("複勝的中" as integer) as "騎手トップ3完走",

    -- ratio_win_jockey
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ") - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }} as "騎手1位完走率",

    -- ratio_place_jockey
    {{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ") - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }} as "騎手トップ3完走率",

    -- jockey_venue_runs
    count(*) over (partition by "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ") - 1 as "騎手場所レース数",

    -- jockey_venue_wins
    sum(case when "着順" = 1 then 1 else 0 end) over (partition by "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ") - cast("単勝的中" as integer) as "騎手場所1位完走",

    -- jockey_venue_places
    sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ") - cast("複勝的中" as integer) as "騎手場所トップ3完走",

    -- ratio_win_jockey_venue
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ") - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }} as "騎手場所1位完走率",

    -- ratio_place_jockey_venue
    {{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ") - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }} as "騎手場所トップ3完走率",

    -- jockey_distance_runs
    count(*) over (partition by "騎手コード", "距離" order by "年月日", "レースキー_Ｒ") - 1 as "騎手距離レース数",

    -- jockey_distance_wins
    sum(case when "着順" = 1 then 1 else 0 end) over (partition by "騎手コード", "距離" order by "年月日", "レースキー_Ｒ") - cast("単勝的中" as integer) as "騎手距離1位完走",

    -- jockey_distance_places
    sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "騎手コード", "距離" order by "年月日", "レースキー_Ｒ") - cast("複勝的中" as integer) as "騎手距離トップ3完走",

    -- ratio_win_jockey_distance
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "騎手コード", "距離" order by "年月日", "レースキー_Ｒ") - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "騎手コード", "距離" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }} as "騎手距離1位完走率",

    -- ratio_place_jockey_distance
    {{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "騎手コード", "距離" order by "年月日", "レースキー_Ｒ") - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "騎手コード", "距離" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }} as "騎手距離トップ3完走率",

    -- trainer_runs
    count(*) over (partition by "調教師コード" order by "年月日", "レースキー_Ｒ") - 1 as "調教師レース数",

    -- trainer_wins
    sum(case when "着順" = 1 then 1 else 0 end) over (partition by "調教師コード" order by "年月日", "レースキー_Ｒ") - cast("単勝的中" as integer) as "調教師1位完走",

    -- trainer_places
    sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "調教師コード" order by "年月日", "レースキー_Ｒ") - cast("複勝的中" as integer) as "調教師トップ3完走",

    -- ratio_win_trainer
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "調教師コード" order by "年月日", "レースキー_Ｒ") - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "調教師コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }} as "調教師1位完走率",

    -- ratio_place_trainer
    {{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "調教師コード" order by "年月日", "レースキー_Ｒ") - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "調教師コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }} as "調教師トップ3完走率",

    -- trainer_venue_runs
    count(*) over (partition by "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ") - 1 as "調教師場所レース数",

    -- trainer_venue_wins
    sum(case when "着順" = 1 then 1 else 0 end) over (partition by "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ") - cast("単勝的中" as integer) as "調教師場所1位完走",

    -- trainer_venue_places
    sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ") - cast("複勝的中" as integer) as "調教師場所トップ3完走",

    -- ratio_win_trainer_venue
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ") - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }} as "調教師場所1位完走率",

    -- ratio_place_trainer_venue
    {{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ") - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }} as "調教師場所トップ3完走率",

    -- Compute the standard rank of the horse on his last 3 races giving us an overview of his state of form
    coalesce(power(前走着順 - 1, 2) + power(前々走着順 - 1, 2) + power(前々々走着順 - 1, 2), 0) as "過去3走順位平方和", -- horse_std_rank


    -- Todo:
    -- https://teddykoker.com/2019/12/beating-the-odds-machine-learning-for-horse-racing/
    
    -- Horse Win Percent: Horse’s win percent over the past 5 races.
    -- horse_win_percent_past_5_races
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号" order by "年月日" rows between 5 preceding and 1 preceding) - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号" order by "年月日" rows between 5 preceding and 1 preceding) - 1 as numeric)'
      )
    }} as "過去5走勝率",

    -- horse_place_percent_past_5_races
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号" order by "年月日" rows between 5 preceding and 1 preceding) - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "血統登録番号" order by "年月日" rows between 5 preceding and 1 preceding) - 1 as numeric)'
      )
    }} as "過去5走トップ3完走率",

    -- Jockey Win Percent: Jockey’s win percent over the past 5 races.
    -- jockey_win_percent_past_5_races
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ" rows between 5 preceding and 1 preceding) - cast("単勝的中" as integer)',
        'cast(count(*) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ" rows between 5 preceding and 1 preceding) - 1 as numeric)'
      )
    }} as "騎手過去5走勝率",

    -- jockey_place_percent_past_5_races
    {{
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ" rows between 5 preceding and 1 preceding) - cast("複勝的中" as integer)',
        'cast(count(*) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ" rows between 5 preceding and 1 preceding) - 1 as numeric)'
      )
    }} as "騎手過去5走トップ3完走率",

  from
    base
  )

select * from final
