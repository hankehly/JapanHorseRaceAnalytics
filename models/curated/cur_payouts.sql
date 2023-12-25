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
    kyi.レースキー,
    kyi.馬番,
    coalesce(win_payouts.払戻金, 0) as 単勝払戻金,
    coalesce(place_payouts.払戻金, 0) as 複勝払戻金
  from
    kyi
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
