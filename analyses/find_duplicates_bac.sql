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
    jrdb_raw.bac
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
    row_number() over (partition by "レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ" order by bac_sk desc) rn,
    *
  from
    jrdb_raw.bac
  where
    ("レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ") in (select "レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ" from duplicates)
  )

-- The following query will return all rows that should be deleted.
select
  bac_sk
from
  jrdb_raw.bac
where
  bac_sk in (select bac_sk from duplicates_with_sk where rn > 1)
