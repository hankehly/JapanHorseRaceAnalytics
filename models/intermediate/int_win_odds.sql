with
  oz as (
  select
    *
  from
    {{ ref('stg_jrdb__oz') }}
  ),

  final as (
  select
    concat(oz.`レースキー`, lpad(cast(idx + 1 as string), 2, '0')) as `unique_key`,
    oz.`レースキー`,
    lpad(cast(idx + 1 as string), 2, '0') as `馬番`,
    cast(nullif(el, '') as float) `単勝オッズ`
  from
    oz
    lateral view posexplode(oz.`単勝オッズ`) t AS idx, el
  where
    nullif(el, '') is not null
)

select * from final
