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

    bac."レース条件_トラック情報_芝ダ障害コード" as "トラック種別",

    kab."芝馬場差" as "前日_芝馬場差",
    kab."ダ馬場差" as "前日_ダ馬場差",

    kyi."ＩＤＭ" as "前日_ＩＤＭ",

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
