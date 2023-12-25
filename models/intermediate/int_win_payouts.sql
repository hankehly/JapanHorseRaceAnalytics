with
  final as (
  select
    レースキー,
    cast(left(val, 2) as integer) as 馬番,
    cast(right(val, 7) as integer) as 払戻金
  from
    {{ ref('stg_jrdb__hjc') }},
    unnest(単勝払戻) as val
  where
    cast(left(val, 2) as integer) != 0
  )
select * from final