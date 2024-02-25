with
  source as (
  select
    *
  from
    {{ source('jrdb', 'raw_jrdb__sed') }}
  ),

  prioritized as (
  select
    *,
    -- duplicates exist where 競走成績キー_年月日=20220827
    -- half of the rows have empty ＪＲＤＢデータ_* columns, the other half have non-empty values
    -- we want to keep the rows with non-empty values
    row_number() over(partition by `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番` order by case when trim(`ＪＲＤＢデータ_ＩＤＭ`) = '' then 1 else 0 end) AS row_priority
  from
    {{ source('jrdb', 'raw_jrdb__sed') }}
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
    sed_sk,
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
    -- todo ------------------------------------------------------
    nullif(`競走成績キー_血統登録番号`, '') as `競走成績キー_血統登録番号`,
    nullif(`競走成績キー_血統登録番号`, '') as `血統登録番号`,
    to_date(nullif(`競走成績キー_年月日`, ''), 'yyyyMMdd') as `競走成績キー_年月日`,
    nullif(`馬名`, '') as `馬名`,
    cast(nullif(`レース条件_距離`, '') as integer) as `レース条件_距離`,
    -- 1:芝, 2:ダート, 3:障害
    case
      when nullif(`レース条件_トラック情報_芝ダ障害コード`, '') = '1' then '芝'
      when nullif(`レース条件_トラック情報_芝ダ障害コード`, '') = '2' then 'ダート'
      when nullif(`レース条件_トラック情報_芝ダ障害コード`, '') = '3' then '障害'
      else null
    end as `レース条件_トラック情報_芝ダ障害コード`,
    nullif(`レース条件_トラック情報_右左`, '') as `レース条件_トラック情報_右左`,
    nullif(`レース条件_トラック情報_内外`, '') as `レース条件_トラック情報_内外`,
    nullif(`レース条件_馬場状態`, '') as `レース条件_馬場状態`,
    nullif(`レース条件_種別`, '') as `レース条件_種別`,
    nullif(`レース条件_条件`, '') as `レース条件_条件`,
    nullif(`レース条件_記号`, '') as `レース条件_記号`,
    nullif(`レース条件_重量`, '') as `レース条件_重量`,
    nullif(`レース条件_グレード`, '') as `レース条件_グレード`,
    nullif(`レース条件_レース名`, '') as `レース条件_レース名`,
    cast(nullif(`レース条件_頭数`, '') as integer) as `レース条件_頭数`,
    nullif(`レース条件_レース名略称`, '') as `レース条件_レース名略称`,
    -- This value is zero when the horse is disqualified.
    -- 0 < 1 which can cause problems when counting number of places.
    -- However we shouldn't just ignore disqualified races because that would
    -- cause problems when calculating how long the horse has rested.
    nullif(cast(nullif(`馬成績_着順`, '') as integer), 0) as `馬成績_着順`,
    nullif(`馬成績_異常区分`, '') as `馬成績_異常区分`,
    cast(substring(`馬成績_タイム`, 1, 1) as double) * 60 + cast(substring(`馬成績_タイム`, 2, 2) as double) + cast(substring(`馬成績_タイム`, 4, 1) as double) / 10 as `馬成績_タイム`,
    cast(nullif(`馬成績_斤量`, '') as integer) as `馬成績_斤量`,
    nullif(`馬成績_騎手名`, '') as `馬成績_騎手名`,
    nullif(`馬成績_調教師名`, '') as `馬成績_調教師名`,
    cast(nullif(`馬成績_確定単勝オッズ`, '') as double) as `馬成績_確定単勝オッズ`,
    nullif(`馬成績_確定単勝人気順位`, '') as `馬成績_確定単勝人気順位`,
    cast(nullif(`ＪＲＤＢデータ_ＩＤＭ`, '') as double) as `ＪＲＤＢデータ_ＩＤＭ`,
    nullif(`ＪＲＤＢデータ_素点`, '') as `ＪＲＤＢデータ_素点`,
    cast(nullif(`ＪＲＤＢデータ_馬場差`, '') as integer) as `ＪＲＤＢデータ_馬場差`,
    nullif(`ＪＲＤＢデータ_ペース`, '') as `ＪＲＤＢデータ_ペース`,
    nullif(`ＪＲＤＢデータ_出遅`, '') as `ＪＲＤＢデータ_出遅`,
    nullif(`ＪＲＤＢデータ_位置取`, '') as `ＪＲＤＢデータ_位置取`,
    coalesce(cast(nullif(`ＪＲＤＢデータ_不利`, '') as integer), 0) as `ＪＲＤＢデータ_不利`,
    coalesce(cast(nullif(`ＪＲＤＢデータ_前不利`, '') as integer), 0) as `ＪＲＤＢデータ_前不利`,
    coalesce(cast(nullif(`ＪＲＤＢデータ_中不利`, '') as integer), 0) as `ＪＲＤＢデータ_中不利`,
    coalesce(cast(nullif(`ＪＲＤＢデータ_後不利`, '') as integer), 0) as `ＪＲＤＢデータ_後不利`,
    nullif(`ＪＲＤＢデータ_レース`, '') as `ＪＲＤＢデータ_レース`,
    nullif(`ＪＲＤＢデータ_コース取り`, '') as `ＪＲＤＢデータ_コース取り`,
    nullif(`ＪＲＤＢデータ_上昇度コード`, '') as `ＪＲＤＢデータ_上昇度コード`,
    nullif(`ＪＲＤＢデータ_クラスコード`, '') as `ＪＲＤＢデータ_クラスコード`,
    nullif(`ＪＲＤＢデータ_馬体コード`, '') as `ＪＲＤＢデータ_馬体コード`,
    nullif(`ＪＲＤＢデータ_気配コード`, '') as `ＪＲＤＢデータ_気配コード`,
    nullif(`ＪＲＤＢデータ_レースペース`, '') as `ＪＲＤＢデータ_レースペース`,
    nullif(`ＪＲＤＢデータ_馬ペース`, '') as `ＪＲＤＢデータ_馬ペース`,
    cast(nullif(`ＪＲＤＢデータ_テン指数`, '') as double) as `ＪＲＤＢデータ_テン指数`,
    cast(nullif(`ＪＲＤＢデータ_上がり指数`, '') as double) as `ＪＲＤＢデータ_上がり指数`,
    cast(nullif(`ＪＲＤＢデータ_ペース指数`, '') as double) as `ＪＲＤＢデータ_ペース指数`,
    cast(nullif(`ＪＲＤＢデータ_レースＰ指数`, '') as double) as `ＪＲＤＢデータ_レースＰ指数`,
    nullif(`ＪＲＤＢデータ_1(2)着馬名`, '') as `ＪＲＤＢデータ_1(2)着馬名`,
    cast(nullif(`ＪＲＤＢデータ_1(2)着タイム差`, '') as integer) * 0.1 `ＪＲＤＢデータ_1(2)着タイム差`,
    cast(nullif(`ＪＲＤＢデータ_前３Ｆタイム`, '') as integer) * 0.1 `ＪＲＤＢデータ_前３Ｆタイム`,
    cast(nullif(`ＪＲＤＢデータ_後３Ｆタイム`, '') as integer) * 0.1 `ＪＲＤＢデータ_後３Ｆタイム`,
    nullif(`ＪＲＤＢデータ_備考`, '') as `ＪＲＤＢデータ_備考`,
    cast(nullif(`確定複勝オッズ下`, '') as double) as `確定複勝オッズ下`,
    nullif(`10時単勝オッズ`, '') as `10時単勝オッズ`,
    nullif(`10時複勝オッズ`, '') as `10時複勝オッズ`,
    nullif(`コーナー順位１`, '') as `コーナー順位１`,
    nullif(`コーナー順位２`, '') as `コーナー順位２`,
    nullif(`コーナー順位３`, '') as `コーナー順位３`,
    nullif(`コーナー順位４`, '') as `コーナー順位４`,
    cast(nullif(`前３Ｆ先頭差`, '') as integer) * 0.1 as `前３Ｆ先頭差`,
    cast(nullif(`後３Ｆ先頭差`, '') as integer) * 0.1 as `後３Ｆ先頭差`,
    nullif(`騎手コード`, '') as `騎手コード`,
    nullif(`調教師コード`, '') as `調教師コード`,
    -- Treat 0, 999 and null the same
    nullif(nullif(cast(nullif(`馬体重`, '') as integer), 0), 999) as `馬体重`,
    cast(nullif(replace(replace(`馬体重増減`, '+', ''), ' ', ''), '') as integer) as `馬体重増減`,
    nullif(`天候コード`, '') as `天候コード`,
    nullif(`コース`, '') as `コース`,
    nullif(`レース脚質`, '') as `レース脚質`,
    cast(nullif(`払戻データ_単勝`, '') as integer) as `払戻データ_単勝`,
    cast(nullif(`払戻データ_複勝`, '') as integer) as `払戻データ_複勝`,
    cast(nullif(`本賞金`, '') as double) as `本賞金`,
    cast(nullif(`収得賞金`, '') as integer) as `収得賞金`,
    nullif(`レースペース流れ`, '') as `レースペース流れ`,
    nullif(`馬ペース流れ`, '') as `馬ペース流れ`,
    nullif(`４角コース取り`, '') as `４角コース取り`,
    nullif(`発走時間`, '') as `発走時間`
  from
    source_dedupe
  )

select
  *
from
  final
  