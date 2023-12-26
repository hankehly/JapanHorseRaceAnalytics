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

  final as (
  select
    kyi."レースキー" as "レースキー",
    kyi."馬番" as "馬番",
    bac."年月日" as "年月日",

    -- horses
    horses."血統登録番号" as "血統登録番号",
    horses."瞬発戦好走馬" as "瞬発戦好走馬",
    horses."消耗戦好走馬" as "消耗戦好走馬",

    -- 成績
    sed."ＪＲＤＢデータ_馬場差" as "成績_馬場差",
    sed."馬成績_着順" as "成績_着順",

    -- 前日
    bac."レース条件_トラック情報_芝ダ障害コード" as "トラック種別",
    kab."芝馬場差" as "前日_芝馬場差",
    kab."ダ馬場差" as "前日_ダ馬場差",
    kyi."ＩＤＭ" as "前日_ＩＤＭ",

    -- 直前
    tyb."ＩＤＭ" as "直前_ＩＤＭ",
    tyb."騎手指数" as "直前_騎手指数",
    tyb."情報指数" as "直前_情報指数",
    tyb."オッズ指数" as "直前_オッズ指数",
    tyb."パドック指数" as "直前_パドック指数",

    coalesce(win_payouts."払戻金", 0) as "単勝払戻金",
    coalesce(place_payouts."払戻金", 0) as "複勝払戻金"
  from
    kyi

  inner join
    bac
  on
    kyi."レースキー" = bac."レースキー"

  -- todo: for future races, sed will not be available
  inner join
    sed
  on
    kyi."レースキー" = sed."レースキー"
    and kyi."馬番" = sed."馬番"

  inner join
    tyb
  on
    kyi."レースキー" = tyb."レースキー"
    and kyi."馬番" = tyb."馬番"

  inner join
    kab
  on
    kyi."開催キー" = kab."開催キー"

  left join
    horses
  on
    kyi."血統登録番号" = horses."血統登録番号"

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
  )
  
select * from final