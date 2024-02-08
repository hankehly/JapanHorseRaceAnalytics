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
    jhra_raw.raw_jrdb__hjc
  group by
    `レースキー_場コード`,
    `レースキー_年`,
    `レースキー_回`,
    `レースキー_日`,
    `レースキー_Ｒ`
  having
    count(*) > 1
  ),
  duplicates_with_sk as (
  select
    row_number() over (partition by `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ` order by hjc_sk desc) rn,
    *
  from
    jhra_raw.raw_jrdb__hjc
  where
    (`レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`) in (select `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ` from duplicates)
  )

select
  *
from
  duplicates_with_sk
