with
  source as (
  select
    *
  from
    {{ source('jrdb', 'raw_jrdb__ow') }}
  ),

  final as (
  select
    ow_sk,
    concat(
      nullif(`レースキー_場コード`, ''),
      nullif(`レースキー_年`, ''),
      nullif(`レースキー_回`, ''),
      nullif(`レースキー_日`, ''),
      nullif(`レースキー_Ｒ`, '')
    ) as ow_bk,
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
    cast(nullif(`登録頭数`, '') as integer) as `登録頭数`,
    TRANSFORM(`ワイドオッズ`, x -> CAST(x AS FLOAT)) AS `ワイドオッズ`
  from
    source
)

select
  *
from
  final
  