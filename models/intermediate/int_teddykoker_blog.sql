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
    kyi."レースキー",
    kyi."馬番",
    bac."年月日",
    coalesce(tyb."騎手コード", kyi."騎手コード") as "騎手コード",
    kyi."レースキー_Ｒ",
    kyi."血統登録番号",
    sed."馬成績_着順" as "着順"
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
  ),

  -- Todo:
  -- https://teddykoker.com/2019/12/beating-the-odds-machine-learning-for-horse-racing/
  teddykoker_blog_features as (
  select
    "レースキー",
    "馬番",

    -- Horse Win Percent: Horse’s win percent over the past 5 races.
    -- horse_win_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号" order by "年月日" rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by "血統登録番号" order by "年月日" rows between 5 preceding and 1 preceding) - 1 as numeric)'
      )
    }}, 0) as "過去5走勝率",

    -- horse_place_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号" order by "年月日" rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by "血統登録番号" order by "年月日" rows between 5 preceding and 1 preceding) - 1 as numeric)'
      )
    }}, 0) as "過去5走トップ3完走率",

    -- Jockey Win Percent: Jockey’s win percent over the past 5 races.
    -- jockey_win_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ" rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ" rows between 5 preceding and 1 preceding) - 1 as numeric)'
      )
    }}, 0) as "騎手過去5走勝率",

    -- jockey_place_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ" rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by "騎手コード" order by "年月日", "レースキー_Ｒ" rows between 5 preceding and 1 preceding) - 1 as numeric)'
      )
    }}, 0) as "騎手過去5走トップ3完走率"
  from
    base
  ),

  final as (
  select
    base."レースキー",
    base."馬番",
    tkb_features."過去5走勝率", -- horse_win_percent_past_5_races
    tkb_features."過去5走トップ3完走率", -- horse_place_percent_past_5_races
    tkb_features."騎手過去5走勝率", -- jockey_win_percent_past_5_races
    tkb_features."騎手過去5走トップ3完走率" -- jockey_place_percent_past_5_races
  from
    base
  inner join
    teddykoker_blog_features tkb_features
  on
    base."レースキー" = tkb_features."レースキー"
    and base."馬番" = tkb_features."馬番"
  )

select
  *
from
  final
