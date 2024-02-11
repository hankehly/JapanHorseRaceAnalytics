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
    lpad(cast(idx + 1 as string), 2, '0') as `馬番`,
    cast(nullif(el, '') as float) `複勝オッズ`
  from
    oz
    lateral view posexplode(oz.`複勝オッズ`) t AS idx, el
  where
    nullif(el, '') is not null
  )

select * from final
