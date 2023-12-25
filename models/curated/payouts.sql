{{ config(materialized='table') }}
with
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
    kyi.レースキー as レースキー,
    kyi.馬番 as 馬番,

    kyi."ＩＤＭ" as "前日_ＩＤＭ",

    tyb."ＩＤＭ" as "直前_ＩＤＭ",
    tyb."騎手指数" as "直前_騎手指数",
    tyb."情報指数" as "直前_情報指数",
    tyb."オッズ指数" as "直前_オッズ指数",
    tyb."パドック指数" as "直前_パドック指数",

    coalesce(win_payouts.払戻金, 0) as 単勝払戻金,
    coalesce(place_payouts.払戻金, 0) as 複勝払戻金
  from
    kyi
  inner join
    tyb
  using
    (レースキー, 馬番)
  left join
    win_payouts
  using
    (レースキー, 馬番)
  left join
    place_payouts
  using
    (レースキー, 馬番)
  )
  
select * from final
