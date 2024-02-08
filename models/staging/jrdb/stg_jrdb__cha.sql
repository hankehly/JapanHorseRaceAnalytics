with
  source as (
  select
    *
  from
    {{ source('jrdb', 'raw_jrdb__cha') }}
  ),
  final as (
  select
    cha_sk,
    concat(
      nullif(`レースキー_場コード`, ''),
      nullif(`レースキー_年`, ''),
      nullif(`レースキー_回`, ''),
      nullif(`レースキー_日`, ''),
      nullif(`レースキー_Ｒ`, ''),
      nullif(`馬番`, '')
    ) as cha_bk,
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
    nullif(`曜日`, '') as `曜日`,
    to_date(nullif(`調教年月日`, ''), 'yyyyMMdd') as `調教年月日`,
    nullif(`回数`, '') as `回数`,
    nullif(`調教コースコード`, '') as `調教コースコード`,
    nullif(`追切種類`, '') as `追切種類`,
    nullif(`追い状態`, '') as `追い状態`,
    nullif(`乗り役`, '') as `乗り役`,
    cast(nullif(`調教Ｆ`, '') as integer) as `調教Ｆ`,
    cast(nullif(`時計分析データ_テンＦ`, '') as integer) as `時計分析データ_テンＦ`,
    cast(nullif(`時計分析データ_中間Ｆ`, '') as integer) as `時計分析データ_中間Ｆ`,
    cast(nullif(`時計分析データ_終いＦ`, '') as integer) as `時計分析データ_終いＦ`,
    cast(nullif(`時計分析データ_テンＦ指数`, '') as integer) as `時計分析データ_テンＦ指数`,
    cast(nullif(`時計分析データ_中間Ｆ指数`, '') as integer) as `時計分析データ_中間Ｆ指数`,
    cast(nullif(`時計分析データ_終いＦ指数`, '') as integer) as `時計分析データ_終いＦ指数`,
    cast(nullif(`時計分析データ_追切指数`, '') as integer) as `時計分析データ_追切指数`,
    nullif(`併せ馬相手データ_併せ結果`, '') as `併せ馬相手データ_併せ結果`,
    nullif(`併せ馬相手データ_追切種類`, '') as `併せ馬相手データ_追切種類`,
    cast(nullif(`併せ馬相手データ_年齢`, '') as integer) as `併せ馬相手データ_年齢`,
    nullif(`併せ馬相手データ_クラス`, '') as `併せ馬相手データ_クラス`
  from
    source
)
select * from final
  