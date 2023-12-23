with source as (
      select * from {{ source('jrdb', 'cha') }}
),
final as (
    select
        concat(
            nullif("レースキー_場コード", ''),
            nullif("レースキー_年", ''),
            nullif("レースキー_回", ''),
            nullif("レースキー_日", ''),
            nullif("レースキー_Ｒ", '')
        ) as "レースキー",
        nullif("レースキー_場コード", '') AS "レースキー_場コード",
        nullif("レースキー_年", '') AS "レースキー_年",
        nullif("レースキー_回", '') AS "レースキー_回",
        nullif("レースキー_日", '') AS "レースキー_日",
        nullif("レースキー_Ｒ", '') AS "レースキー_Ｒ",
        nullif("馬番", '') AS "馬番",
        nullif("曜日", '') AS "曜日",
        to_date(nullif("調教年月日", ''), 'YYYYMMDD') as "調教年月日",
        nullif("回数", '') AS "回数",
        nullif("調教コースコード", '') AS "調教コースコード",
        nullif("追切種類", '') AS "追切種類",
        nullif("追い状態", '') AS "追い状態",
        nullif("乗り役", '') AS "乗り役",
        nullif("調教Ｆ", '') AS "調教Ｆ",
        nullif("時計分析データ_テンＦ", '') AS "時計分析データ_テンＦ",
        nullif("時計分析データ_中間Ｆ", '') AS "時計分析データ_中間Ｆ",
        nullif("時計分析データ_終いＦ", '') AS "時計分析データ_終いＦ",
        nullif("時計分析データ_テンＦ指数", '') AS "時計分析データ_テンＦ指数",
        nullif("時計分析データ_中間Ｆ指数", '') AS "時計分析データ_中間Ｆ指数",
        nullif("時計分析データ_終いＦ指数", '') AS "時計分析データ_終いＦ指数",
        nullif("時計分析データ_追切指数", '') AS "時計分析データ_追切指数",
        nullif("併せ馬相手データ_併せ結果", '') AS "併せ馬相手データ_併せ結果",
        nullif("併せ馬相手データ_追切種類", '') AS "併せ馬相手データ_追切種類",
        cast(nullif("併せ馬相手データ_年齢", '') as int) AS "併せ馬相手データ_年齢",
        nullif("併せ馬相手データ_クラス", '') AS "併せ馬相手データ_クラス"
    from source
)
select * from final
  