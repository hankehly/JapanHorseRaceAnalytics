with
  duplicates as (
  select
    "レースキー_場コード",
    "レースキー_年",
    "レースキー_回",
    "レースキー_日",
    "レースキー_Ｒ",
    count(*)
  from
    jrdb_raw.oz
  group by
    "レースキー_場コード",
    "レースキー_年",
    "レースキー_回",
    "レースキー_日",
    "レースキー_Ｒ"
  having
    count(*) > 1
  ),
  duplicates_with_sk as (
  select
    row_number() over (partition by "レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ" order by oz_sk desc) rn,
    *
  from
    jrdb_raw.oz
  where
    ("レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ") in (select "レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ" from duplicates)
  )

-- The following query will return all duplicate rows.
select
  *
from
  duplicates_with_sk
