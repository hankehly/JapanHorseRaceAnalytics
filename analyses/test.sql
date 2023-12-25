with
  bets as (
  select
    *
  from
	jrdb_curated.payouts
  where
	直前_ＩＤＭ > 15
  ),

  hits as (
  select
    *
  from
    bets
  where
    単勝払戻金 > 0
  )

  select
    (select count(*) from bets) total_bets,
	count(*) total_hits,
    round(cast(count(*) as numeric) / (select count(*) from bets), 5) hit_rate,
    round(cast((select count(*) from bets) as numeric) / (select count(*) from jrdb_curated.payouts), 5) bet_rate,
	(select count(*) * 100 from bets) amount_bet,
	sum(単勝払戻金) amount_won,
	round(cast(sum(単勝払戻金) as numeric) / (select count(*) * 100 from bets), 5) return_rate
  from
    hits
