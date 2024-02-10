with
  oz as (
  select
    *
  from
    {{ ref('stg_jrdb__oz') }}
  ),

  final as (
  select
    `レースキー`,
    `開催キー`,
    `レースキー_場コード`,
    `レースキー_年`,
    `レースキー_回`,
    `レースキー_日`,
    `レースキー_Ｒ`,
    lpad(cast(idx + 1 as string), 2, '0') as `馬番`,
    cast(nullif(el, '') as float) `複勝オッズ`
  from
    oz
    lateral view posexplode(oz.`複勝オッズ`) t AS idx, el
  where
    nullif(el, '') is not null
  )

select * from final
