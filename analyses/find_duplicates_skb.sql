with
  duplicates as (
  select
    `レースキー_場コード`,
    `レースキー_年`,
    `レースキー_回`,
    `レースキー_日`,
    `レースキー_Ｒ`,
    `馬番`,
    count(*)
  from
    jhra_raw.raw_jrdb__skb
  group by
    `レースキー_場コード`,
    `レースキー_年`,
    `レースキー_回`,
    `レースキー_日`,
    `レースキー_Ｒ`,
    `馬番`
  having
    count(*) > 1
  ),
  duplicates_with_sk as (
  select
    row_number() over (partition by `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番` order by skb_sk) rn,
    *
  from
    jhra_raw.raw_jrdb__skb
  where
    (`レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番`) in (select `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番` from duplicates)
  )

-- The following query will return all duplicate rows.
select
  *
from
  duplicates_with_sk
