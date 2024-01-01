{{
  config(
    materialized='table',
    indexes=[{'columns': ['血統登録番号', 'データ年月日'], 'unique': True}]
  )
}}

with
  source as (
  select
    *
  from
    {{ source('jrdb', 'ukc') }}
  where
    -- there are 8 records with values of '99999999'
    -- these records are identical to the records with the same 血統登録番号
    -- so we can safely ignore them
    データ年月日 != '99999999'
  ),

  duplicates as (
  select
    "血統登録番号",
    "データ年月日",
    count(*)
  from
    source
  group by
    "血統登録番号",
    "データ年月日"
  having
    count(*) > 1
  ),

  duplicates_with_sk as (
  select
    row_number() over (partition by "血統登録番号", "データ年月日" order by ukc_sk) rn,
    *
  from
    source
  where
    ("血統登録番号", "データ年月日") in (select "血統登録番号", "データ年月日" from duplicates)
  ),

  source_dedupe as (
  select
    *
  from
    source
  where
    ukc_sk not in (select ukc_sk from duplicates_with_sk where rn > 1)
  ),

  final as (
  select
    ukc_sk,
    concat(
      nullif("血統登録番号", ''),
      nullif("データ年月日", '')
    ) as ukc_bk,
    nullif("血統登録番号", '') as "血統登録番号",
    nullif("馬名", '') as "馬名",
    -- 1:牡,2:牝,3,セン
    nullif("性別コード", '') as 性別コード,
    nullif("毛色コード", '') as "毛色コード",
    -- 00 is a special 馬記号コード value for unknown (there are only 3 nulls in the data)
    coalesce(nullif("馬記号コード", ''), '00') as "馬記号コード",
    nullif("血統情報_父馬名", '') as "血統情報_父馬名",
    nullif("血統情報_母馬名", '') as "血統情報_母馬名",
    nullif("血統情報_母父馬名", '') as "血統情報_母父馬名",
    to_date(nullif("生年月日", ''), 'YYYYMMDD') as "生年月日",
    cast(nullif("父馬生年", '') as integer) as "父馬生年",
    cast(nullif("母馬生年", '') as integer) as "母馬生年",
    cast(nullif("母父馬生年", '') as integer) as "母父馬生年",
    nullif("馬主名", '') as "馬主名",
    nullif("馬主会コード", '') as "馬主会コード",
    nullif("生産者名", '') as "生産者名",
    nullif("産地名", '') as "産地名",
    cast(nullif("登録抹消フラグ", '') as boolean) as "登録抹消フラグ",
    to_date(nullif("データ年月日", ''), 'YYYYMMDD') as "データ年月日",
    nullif("父系統コード", '') as "父系統コード",
    nullif("母父系統コード", '') as "母父系統コード"
  from
    source_dedupe
)

select
  *
from
  final
  