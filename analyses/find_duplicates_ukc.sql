-- Check for duplicate keys.
with
  duplicates as (
  select
    `血統登録番号`,
	  `データ年月日`,
    count(*)
  from
    {{ source('jrdb', 'raw_jrdb__ukc') }}
  group by
    `血統登録番号`,
    `データ年月日`
  having
    count(*) > 1
  )
select
  *
from
  {{ source('jrdb', 'raw_jrdb__ukc') }}
where
  (`血統登録番号`, `データ年月日`) in (select `血統登録番号`, `データ年月日` from duplicates)
order by
  `血統登録番号`, `データ年月日`

-- Check for duplicate keys with different data.
-- with
--   distinct_rows as (
--   select distinct
--     `血統登録番号`,
--     `馬名`,
--     `性別コード`,
--     `毛色コード`,
--     `馬記号コード`,
--     `血統情報_父馬名`,
--     `血統情報_母馬名`,
--     `血統情報_母父馬名`,
--     `生年月日`,
--     `父馬生年`,
--     `母馬生年`,
--     `母父馬生年`,
--     `馬主名`,
--     `馬主会コード`,
--     `生産者名`,
--     `産地名`,
--     `登録抹消フラグ`,
--     `データ年月日`,
--     `父系統コード`,
--     `母父系統コード`
--   from
--     {{ source('jrdb', 'raw_jrdb__ukc') }}
--   ),
--   duplicates as (
--   select
--     `血統登録番号`,
--     `データ年月日`
--   from
--     distinct_rows
--   group by
--     `血統登録番号`,
--     `データ年月日`
--   having
--     count(*) > 1
--   )
-- select
--   *
-- from
--   {{ source('jrdb', 'raw_jrdb__ukc') }}
-- where
--   (`血統登録番号`, `データ年月日`) in (select `血統登録番号`, `データ年月日` from duplicates)
-- order by
--   `血統登録番号`, `データ年月日`
