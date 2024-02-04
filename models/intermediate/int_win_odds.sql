with
  oz as (
  select
    *
  from
    {{ ref('stg_jrdb__oz') }}
  ),

  final as (
  select
    oz.`レースキー`,
    oz.`開催キー`,
    oz.`レースキー_場コード`,
    oz.`レースキー_年`,
    oz.`レースキー_回`,
    oz.`レースキー_日`,
    oz.`レースキー_Ｒ`,
    lpad(cast(idx + 1 as string), 2, '0') as `馬番`,
    cast(nullif(el, '') as float) `単勝オッズ`
  from
    oz
  lateral view posexplode(oz.`単勝オッズ`) t AS idx, el
  where
    nullif(el, '') is not null
)

select
  *
from
  final
