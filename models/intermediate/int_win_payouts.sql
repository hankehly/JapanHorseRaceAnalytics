with
  hjc as (
  select
    *
  from
    {{ ref('stg_jrdb__hjc') }}
  ),

  final as (
  select
    `レースキー`,
    left(val, 2) as `馬番`,
    cast(right(val, 7) as integer) as `払戻金`
  from
    hjc
  lateral view explode(`単勝払戻`) t as val
  where
    cast(left(val, 2) as integer) != 0
  )

select
  *
from
  final