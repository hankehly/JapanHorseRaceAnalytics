with
  duplicates as (
  select
    "レースキー_場コード",
    "レースキー_年",
    "レースキー_回",
    "レースキー_日",
    "レースキー_Ｒ",
    "馬番",
    count(*)
  from
    jrdb_raw.cha
  group by
    "レースキー_場コード",
    "レースキー_年",
    "レースキー_回",
    "レースキー_日",
    "レースキー_Ｒ",
    "馬番"
  having
    count(*) > 1
  ),
  duplicates_with_sk as (
  select
    row_number() over (partition by "レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ", "馬番" order by cha_sk) rn,
    *
  from
    jrdb_raw.cha
  where
    ("レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ", "馬番") in (select "レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ", "馬番" from duplicates)
  )

-- No duplicates found (2023/12/30)
-- The following query will return all rows that should be deleted.
select
  *
from
  duplicates_with_sk
