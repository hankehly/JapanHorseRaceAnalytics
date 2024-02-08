with
  source as (
  select
    *
  from
    {{ source('jrdb', 'raw_jrdb__cyb') }}
  ),

  prioritized as (
  select
    *,
    -- for cyb, duplicate keys exist but only some rows have 一週前追切指数 and 一週前追切コース populated
    -- so we prioritize rows with these fields populated
    row_number() over(partition by `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番` order by case when trim(`一週前追切指数`) = '' then 1 else 0 end) AS row_priority
  from
    {{ source('jrdb', 'raw_jrdb__cyb') }}
  ),

  source_dedupe as (
  select
    *
  from
    prioritized
  where
    row_priority = 1
  order by
    `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番`
  ),

  final as (
  select
    cyb_sk,
    concat(
      nullif(`レースキー_場コード`, ''),
      nullif(`レースキー_年`, ''),
      nullif(`レースキー_回`, ''),
      nullif(`レースキー_日`, ''),
      nullif(`レースキー_Ｒ`, ''),
      nullif(`馬番`, '')
    ) as cyb_bk,
    concat(
      nullif(`レースキー_場コード`, ''),
      nullif(`レースキー_年`, ''),
      nullif(`レースキー_回`, ''),
      nullif(`レースキー_日`, ''),
      nullif(`レースキー_Ｒ`, '')
    ) as `レースキー`,
    concat(
      nullif(`レースキー_場コード`, ''),
      nullif(`レースキー_年`, ''),
      nullif(`レースキー_回`, ''),
      nullif(`レースキー_日`, '')
    ) as `開催キー`,
    nullif(`レースキー_場コード`, '') as `レースキー_場コード`,
    nullif(`レースキー_年`, '') as `レースキー_年`,
    nullif(`レースキー_回`, '') as `レースキー_回`,
    nullif(`レースキー_日`, '') as `レースキー_日`,
    nullif(`レースキー_Ｒ`, '') as `レースキー_Ｒ`,
    nullif(`馬番`, '') as `馬番`,
    nullif(`調教タイプ`, '') as `調教タイプ`,
    nullif(`調教コース種別`, '') as `調教コース種別`,
    cast(cast(nullif(`調教コース種類_坂`, '') as integer) as boolean) as `調教コース種類_坂`,
    cast(cast(nullif(`調教コース種類_Ｗ`, '') as integer) as boolean) as `調教コース種類_Ｗ`,
    cast(cast(nullif(`調教コース種類_ダ`, '') as integer) as boolean) as `調教コース種類_ダ`,
    cast(cast(nullif(`調教コース種類_芝`, '') as integer) as boolean) as `調教コース種類_芝`,
    cast(cast(nullif(`調教コース種類_プ`, '') as integer) as boolean) as `調教コース種類_プ`,
    cast(cast(nullif(`調教コース種類_障`, '') as integer) as boolean) as `調教コース種類_障`,
    cast(cast(nullif(`調教コース種類_ポ`, '') as integer) as boolean) as `調教コース種類_ポ`,
    nullif(`調教距離`, '') as `調教距離`,
    nullif(`調教重点`, '') as `調教重点`,
    cast(nullif(`追切指数`, '') as integer) as `追切指数`,
    cast(nullif(`仕上指数`, '') as integer) as `仕上指数`,
    nullif(`調教量評価`, '') as `調教量評価`,
    nullif(`仕上指数変化`, '') as `仕上指数変化`,
    nullif(`調教コメント`, '') as `調教コメント`,
    -- Date formats are mixed: yyyyMMdd and yyyy/MM/dd and yyyy/M/d
    -- Handle later if necessary
    -- case
    --   when `コメント年月日` like '%/%' then to_date(`コメント年月日`, 'yyyy/MM/dd')
    --   else to_date(nullif(`コメント年月日`, ''), 'yyyyMMdd')
    -- end as `コメント年月日`,
    `コメント年月日`,
    nullif(`調教評価`, '') as `調教評価`,
    cast(nullif(`一週前追切指数`, '') as integer) as `一週前追切指数`,
    nullif(`一週前追切コース`, '') as `一週前追切コース`
  from
    source_dedupe
)

select
  *
from
  final
  