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
    jhra_raw.raw_jrdb__tyb
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
    -- For tyb, the row with the highest sk contains the more complete data.
    row_number() over (partition by `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番` order by tyb_sk desc) rn,
    *
  from
    jhra_raw.raw_jrdb__tyb
  where
    (`レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番`) in (select `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番` from duplicates)
  )

-- The following query will return all rows that should be deleted.
select
  tyb_sk
from
  duplicates_with_sk
where
  tyb_sk in (select tyb_sk from duplicates_with_sk where rn > 1)
