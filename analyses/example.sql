with
  bets as (
  select
    *
  from
	  jrdb_curated.payouts
  where
    --============================--
    (
      トラック種別 = '芝'
      AND (瞬発戦好走馬_芝 AND 消耗戦好走馬_芝)
    )

    OR
    (
      トラック種別 = 'ダート'
      AND (瞬発戦好走馬_ダート AND 消耗戦好走馬_ダート)
    )
    --============================--
  ),

  hits_placed as (
  select
    *
  from
    bets
  where
    複勝払戻金 > 0
  ),
  
  hits_win as (
  select
    *
  from
    bets
  where
    単勝払戻金 > 0
  ),

  results_placed as (
  select
    '複勝' as bet_type,
    to_char((select count(*) from bets), 'FM999,999,999') total_bets,
    to_char(count(*), 'FM999,999,999') total_hits,
    round(cast(count(*) as numeric) / (select count(*) from bets) * 100, 2) hit_rate,
    round(cast((select count(*) from bets) as numeric) / (select count(*) from jrdb_curated.payouts) * 100, 2) betting_rate,
    to_char((select count(*) * 100 from bets), 'FM999,999,999') total_amount_bet,
    to_char(sum(複勝払戻金), 'FM999,999,999') total_amount_won,
    round(cast(sum(複勝払戻金) as numeric) / (select count(*) * 100 from bets) * 100, 2) return_rate
  from
    hits_placed hits
  ),

  results_win as (
  select
    '単勝' as bet_type,
    to_char((select count(*) from bets), 'FM999,999,999') total_bets,
    to_char(count(*), 'FM999,999,999') total_hits,
    round(cast(count(*) as numeric) / (select count(*) from bets) * 100, 2) hit_rate,
    round(cast((select count(*) from bets) as numeric) / (select count(*) from jrdb_curated.payouts) * 100, 2) betting_rate,
    to_char((select count(*) * 100 from bets), 'FM999,999,999') total_amount_bet,
    to_char(sum(単勝払戻金), 'FM999,999,999') total_amount_won,
    round(cast(sum(単勝払戻金) as numeric) / (select count(*) * 100 from bets) * 100, 2) return_rate
  from
    hits_win hits
  ),

  final as (
  select
    *
  from
    results_placed
  union all
  select
    *
  from
    results_win
  )

select * from final