with
  source as (
  select
    *
  from
    {{ source('jrdb', 'raw_jrdb__bac') }}
  ),
  prioritized as (
  select
    *,
    -- bac has rows with duplicate race keys but different 年月日.
    -- E.g., see 開催キー in '061345', '091115', '101125'
    -- we want the rows with the latest 年月日
    -- Note: the 'YYYYMMDD' format maintains chronological ordering when sorted alphabetically as strings,
    -- so we don't need to cast to date or integer when sorting
    row_number() over(partition by `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ` order by `年月日` desc) AS row_priority
  from
    {{ source('jrdb', 'raw_jrdb__bac') }}
  ),

  source_dedupe as (
  select
    *
  from
    prioritized
  where
    row_priority = 1
  ),

  final as (
  select
    bac_sk,
    concat(
      nullif(`レースキー_場コード`, ''),
      nullif(`レースキー_年`, ''),
      nullif(`レースキー_回`, ''),
      nullif(`レースキー_日`, ''),
      nullif(`レースキー_Ｒ`, '')
    ) as bac_bk,
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
    to_date(nullif(`年月日`, ''), 'yyyyMMdd') as `年月日`,
    concat(substr(`発走時間`, 1, 2), ':', substr(`発走時間`, -2)) as `発走時間`,
    to_timestamp(
      concat(
        to_date(nullif(`年月日`, ''), 'yyyyMMdd'),
        ' ',
        concat(substr(`発走時間`, 1, 2), ':', substr(`発走時間`, -2))
      ),
      'yyyy-MM-dd HH:mm'
    ) as `発走日時`,
    cast(nullif(`レース条件_距離`, '') as integer) as `レース条件_距離`,
    case
      when `レース条件_トラック情報_芝ダ障害コード` = '1' then '芝'
      when `レース条件_トラック情報_芝ダ障害コード` = '2' then 'ダート'
      when `レース条件_トラック情報_芝ダ障害コード` = '3' then '障害'
      else null
    end as `レース条件_トラック情報_芝ダ障害コード`,
    nullif(`レース条件_トラック情報_右左`, '') as `レース条件_トラック情報_右左`,
    nullif(`レース条件_トラック情報_内外`, '') as `レース条件_トラック情報_内外`,
    nullif(`レース条件_種別`, '') as `レース条件_種別`,
    nullif(`レース条件_条件`, '') as `レース条件_条件`,
    nullif(`レース条件_記号`, '') as `レース条件_記号`,
    nullif(`レース条件_重量`, '') as `レース条件_重量`,
    nullif(`レース条件_グレード`, '') as `レース条件_グレード`,
    nullif(`レース名`, '') as `レース名`,
    nullif(`回数`, '') as `回数`,
    cast(nullif(`頭数`, '') as integer) as `頭数`,
    nullif(`コース`, '') as `コース`,
    nullif(`開催区分`, '') as `開催区分`,
    nullif(`レース名短縮`, '') as `レース名短縮`,
    nullif(`レース名９文字`, '') as `レース名９文字`,
    nullif(`データ区分`, '') as `データ区分`,
    cast(nullif(`１着賞金`, '') as integer) as `１着賞金`,
    cast(nullif(`２着賞金`, '') as integer) as `２着賞金`,
    cast(nullif(`３着賞金`, '') as integer) as `３着賞金`,
    cast(nullif(`４着賞金`, '') as integer) as `４着賞金`,
    cast(nullif(`５着賞金`, '') as integer) as `５着賞金`,
    cast(nullif(`１着算入賞金`, '') as integer) as `１着算入賞金`,
    cast(nullif(`２着算入賞金`, '') as integer) as `２着算入賞金`,
    cast(coalesce(nullif(`馬券発売フラグ_単勝`, ''), '0') as boolean) as `馬券発売フラグ_単勝`,
    cast(coalesce(nullif(`馬券発売フラグ_複勝`, ''), '0') as boolean) as `馬券発売フラグ_複勝`,
    cast(coalesce(nullif(`馬券発売フラグ_枠連`, ''), '0') as boolean) as `馬券発売フラグ_枠連`,
    cast(coalesce(nullif(`馬券発売フラグ_馬連`, ''), '0') as boolean) as `馬券発売フラグ_馬連`,
    cast(coalesce(nullif(`馬券発売フラグ_馬単`, ''), '0') as boolean) as `馬券発売フラグ_馬単`,
    cast(coalesce(nullif(`馬券発売フラグ_ワイド`, ''), '0') as boolean) as `馬券発売フラグ_ワイド`,
    cast(coalesce(nullif(`馬券発売フラグ_３連複`, ''), '0') as boolean) as `馬券発売フラグ_３連複`,
    cast(coalesce(nullif(`馬券発売フラグ_３連単`, ''), '0') as boolean) as `馬券発売フラグ_３連単`,
    nullif(`WIN5フラグ`, '') as `WIN5フラグ`
  from
    source_dedupe
  )

select
  *
from
  final
