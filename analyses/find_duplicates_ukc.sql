with
  duplicates as (
  select
    `血統登録番号`,
	`データ年月日`,
    count(*)
  from
    jhra_raw.raw_jrdb__ukc
  group by
    `血統登録番号`,
    `データ年月日`
  having
    count(*) > 1
  ),
  duplicates_with_sk as (
  select
    row_number() over (partition by `血統登録番号`, `データ年月日` order by ukc_sk) rn,
    *
  from
    jhra_raw.raw_jrdb__ukc
  where
    (`血統登録番号`, `データ年月日`) in (select `血統登録番号`, `データ年月日` from duplicates)
  )

-- The following query will return all duplicate rows.
select
  *
from
  duplicates_with_sk
