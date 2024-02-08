with
  duplicates as (
  select
    `レースキー_場コード`,
    `レースキー_年`,
    `レースキー_回`,
    `レースキー_日`,
    `レースキー_Ｒ`,
    count(*)
  from
    {{ source('jrdb', 'raw_jrdb__bac') }}
  group by
    `レースキー_場コード`,
    `レースキー_年`,
    `レースキー_回`,
    `レースキー_日`,
    `レースキー_Ｒ`
  having
    count(*) > 1
  )
select
  *
from
  {{ source('jrdb', 'raw_jrdb__bac') }}
where
  (`レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`) in (select `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ` from duplicates)
order by
  `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`
