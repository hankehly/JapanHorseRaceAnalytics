{{ config(materialized = 'table') }}

with
  source as (
  select
    *
  from
    {{ source('jrdb', 'cyb') }}
  ),

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
    source
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
    -- For cyb, the row with the highest cyb_sk has the newest data.
    row_number() over (partition by "レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ", "馬番" order by cyb_sk desc) rn,
    *
  from
    source
  where
    ("レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ", "馬番") in (select "レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ", "馬番" from duplicates)
  ),

  source_dedupe as (
  select
    *
  from
    source
  where
    cyb_sk not in (select cyb_sk from duplicates_with_sk where rn > 1)
  ),

  final as (
  select
    cyb_sk,
    concat(
      nullif("レースキー_場コード", ''),
      nullif("レースキー_年", ''),
      nullif("レースキー_回", ''),
      nullif("レースキー_日", ''),
      nullif("レースキー_Ｒ", ''),
      nullif("馬番", '')
    ) as cyb_bk,
    concat(
      nullif("レースキー_場コード", ''),
      nullif("レースキー_年", ''),
      nullif("レースキー_回", ''),
      nullif("レースキー_日", ''),
      nullif("レースキー_Ｒ", '')
    ) as "レースキー",
    concat(
      nullif("レースキー_場コード", ''),
      nullif("レースキー_年", ''),
      nullif("レースキー_回", ''),
      nullif("レースキー_日", '')
    ) as "開催キー",
    nullif("レースキー_場コード", '') as "レースキー_場コード",
    nullif("レースキー_年", '') as "レースキー_年",
    nullif("レースキー_回", '') as "レースキー_回",
    nullif("レースキー_日", '') as "レースキー_日",
    nullif("レースキー_Ｒ", '') as "レースキー_Ｒ",
    nullif("馬番", '') as "馬番",
    nullif("調教タイプ", '') as "調教タイプ",
    nullif("調教コース種別", '') as "調教コース種別",
    cast(cast(nullif("調教コース種類_坂", '') as integer) as boolean) as "調教コース種類_坂",
    cast(cast(nullif("調教コース種類_Ｗ", '') as integer) as boolean) as "調教コース種類_Ｗ",
    cast(cast(nullif("調教コース種類_ダ", '') as integer) as boolean) as "調教コース種類_ダ",
    cast(cast(nullif("調教コース種類_芝", '') as integer) as boolean) as "調教コース種類_芝",
    cast(cast(nullif("調教コース種類_プ", '') as integer) as boolean) as "調教コース種類_プ",
    cast(cast(nullif("調教コース種類_障", '') as integer) as boolean) as "調教コース種類_障",
    cast(cast(nullif("調教コース種類_ポ", '') as integer) as boolean) as "調教コース種類_ポ",
    nullif("調教距離", '') as "調教距離",
    nullif("調教重点", '') as "調教重点",
    cast(nullif("追切指数", '') as integer) as "追切指数",
    cast(nullif("仕上指数", '') as integer) as "仕上指数",
    nullif("調教量評価", '') as "調教量評価",
    nullif("仕上指数変化", '') as "仕上指数変化",
    nullif("調教コメント", '') as "調教コメント",
    -- Date formats are mixed: YYYYMMDD and YYYY/MM/DD
    case
      when "コメント年月日" like '%/%' then to_date("コメント年月日", 'YYYY/MM/DD')
      else to_date(nullif("コメント年月日", ''), 'YYYYMMDD')
    end as "コメント年月日",
    nullif("調教評価", '') as "調教評価",
    cast(nullif("一週前追切指数", '') as integer) as "一週前追切指数",
    nullif("一週前追切コース", '') as "一週前追切コース"
  from
    source_dedupe
)

select
  *
from
  final
  