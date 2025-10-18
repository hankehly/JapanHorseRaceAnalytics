with
  source as (
  select
    *
  from
    {{ source('jrdb', 'raw_jrdb__bac') }}
  ),

  source_deduped as (
  select
    *
  from (
    select
      *,
      -- bac has rows with duplicate race keys but different 年月日.
      -- E.g., see 開催キー in '061345', '091115', '101125'
      -- we want the rows with the latest 年月日
      -- Note: the 'YYYYMMDD' format maintains chronological ordering when sorted alphabetically as strings,
      -- so we don't need to cast to date or integer when sorting
      row_number() over(partition by `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ` order by `年月日` desc) AS rn
    from
      source
    )
  where
    rn = 1
  ),

  final as (
  select
    file_name,
    sha256,
    concat(
      nullif(trim(`レースキー_場コード`), ''),
      nullif(trim(`レースキー_年`), ''),
      nullif(trim(`レースキー_回`), ''),
      nullif(trim(`レースキー_日`), ''),
      nullif(trim(`レースキー_Ｒ`), '')
    ) as bac_bk,
    concat(
      nullif(trim(`レースキー_場コード`), ''),
      nullif(trim(`レースキー_年`), ''),
      nullif(trim(`レースキー_回`), ''),
      nullif(trim(`レースキー_日`), ''),
      nullif(trim(`レースキー_Ｒ`), '')
    ) as `レースキー`,
    concat(
      nullif(trim(`レースキー_場コード`), ''),
      nullif(trim(`レースキー_年`), ''),
      nullif(trim(`レースキー_回`), ''),
      nullif(trim(`レースキー_日`), '')
    ) as `開催キー`,
    nullif(trim(`レースキー_場コード`), '') as `レースキー_場コード`,
    nullif(trim(`レースキー_年`), '') as `レースキー_年`,
    nullif(trim(`レースキー_回`), '') as `レースキー_回`,
    nullif(trim(`レースキー_日`), '') as `レースキー_日`,
    nullif(trim(`レースキー_Ｒ`), '') as `レースキー_Ｒ`,
    to_date(nullif(trim(`年月日`), ''), 'yyyyMMdd') as `年月日`,
    concat(substr(`発走時間`, 1, 2), ':', substr(`発走時間`, -2)) as `発走時間`,
    concat(
      to_date(nullif(trim(`年月日`), ''), 'yyyyMMdd'),
      ' ',
      concat(substr(`発走時間`, 1, 2), ':', substr(`発走時間`, -2)),
      ':00+09:00'
    ) as `発走日時`,
    cast(nullif(trim(`レース条件_距離`), '') as integer) as `レース条件_距離`,
    nullif(trim(`レース条件_トラック情報_芝ダ障害コード`), '') as `レース条件_トラック情報_芝ダ障害コード`,
    nullif(trim(`レース条件_トラック情報_右左`), '') as `レース条件_トラック情報_右左`,
    nullif(trim(`レース条件_トラック情報_内外`), '') as `レース条件_トラック情報_内外`,
    nullif(trim(`レース条件_種別`), '') as `レース条件_種別`,
    nullif(trim(`レース条件_条件`), '') as `レース条件_条件`,
    nullif(trim(`レース条件_記号`), '') as `レース条件_記号`,
    nullif(trim(`レース条件_重量`), '') as `レース条件_重量`,
    nullif(trim(`レース条件_グレード`), '') as `レース条件_グレード`,
    nullif(trim(`レース名`), '') as `レース名`,
    nullif(trim(`回数`), '') as `回数`,
    cast(nullif(trim(`頭数`), '') as integer) as `頭数`,
    nullif(trim(`コース`), '') as `コース`,
    nullif(trim(`開催区分`), '') as `開催区分`,
    nullif(trim(`レース名短縮`), '') as `レース名短縮`,
    nullif(trim(`レース名９文字`), '') as `レース名９文字`,
    nullif(trim(`データ区分`), '') as `データ区分`,
    cast(nullif(trim(`１着賞金`), '') as integer) as `１着賞金`,
    cast(nullif(trim(`２着賞金`), '') as integer) as `２着賞金`,
    cast(nullif(trim(`３着賞金`), '') as integer) as `３着賞金`,
    cast(nullif(trim(`４着賞金`), '') as integer) as `４着賞金`,
    cast(nullif(trim(`５着賞金`), '') as integer) as `５着賞金`,
    cast(nullif(trim(`１着算入賞金`), '') as integer) as `１着算入賞金`,
    cast(nullif(trim(`２着算入賞金`), '') as integer) as `２着算入賞金`,
    cast(coalesce(nullif(trim(`馬券発売フラグ_単勝`), ''), '0') as boolean) as `馬券発売フラグ_単勝`,
    cast(coalesce(nullif(trim(`馬券発売フラグ_複勝`), ''), '0') as boolean) as `馬券発売フラグ_複勝`,
    cast(coalesce(nullif(trim(`馬券発売フラグ_枠連`), ''), '0') as boolean) as `馬券発売フラグ_枠連`,
    cast(coalesce(nullif(trim(`馬券発売フラグ_馬連`), ''), '0') as boolean) as `馬券発売フラグ_馬連`,
    cast(coalesce(nullif(trim(`馬券発売フラグ_馬単`), ''), '0') as boolean) as `馬券発売フラグ_馬単`,
    cast(coalesce(nullif(trim(`馬券発売フラグ_ワイド`), ''), '0') as boolean) as `馬券発売フラグ_ワイド`,
    cast(coalesce(nullif(trim(`馬券発売フラグ_３連複`), ''), '0') as boolean) as `馬券発売フラグ_３連複`,
    cast(coalesce(nullif(trim(`馬券発売フラグ_３連単`), ''), '0') as boolean) as `馬券発売フラグ_３連単`,
    nullif(trim(`WIN5フラグ`), '') as `WIN5フラグ`
  from
    source_deduped
  )

select
  *
from
  final
