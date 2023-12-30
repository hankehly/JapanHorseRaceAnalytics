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
    jrdb_raw.sed
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
    -- The following line orders the rows by sed_sk in ascending order because we want to keep the oldest row in the case of sed.
    -- For other tables, this is not the case because we want to keep the newest row.
    -- Check netkeiba to see if the oldest or newest row should be kept.
    row_number() over (partition by "レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ", "馬番" order by sed_sk) rn,
    *
  from
    jrdb_raw.sed
  where
    ("レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ", "馬番") in (select "レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ", "馬番" from duplicates)
  )

-- The following query will return all rows that should be deleted.
select
  sed_sk
from
  duplicates_with_sk
where
  sed_sk in (select sed_sk from duplicates_with_sk where rn > 1)
