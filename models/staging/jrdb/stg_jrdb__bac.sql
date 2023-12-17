with source as (
      select * from {{ source('jrdb', 'bac') }}
),
final as (
    select
        nullif("レースキー_場コード", '') as "レースキー_場コード",
        nullif("レースキー_年", '') as "レースキー_年",
        nullif("レースキー_回", '') as "レースキー_回",
        nullif("レースキー_日", '') as "レースキー_日",
        nullif("レースキー_Ｒ", '') as "レースキー_Ｒ",
        to_date(nullif("年月日", ''), 'YYYYMMDD') as "年月日",
        cast(
            nullif(left("発走時間", 2) || ':' || right("発走時間", 2), '') as time
        ) as "発走時間",
        nullif("レース条件_距離", '') as "レース条件_距離",
        nullif("レース条件_トラック情報_芝ダ障害コード", '') as "レース条件_トラック情報_芝ダ障害コード",
        nullif("レース条件_トラック情報_右左", '') as "レース条件_トラック情報_右左",
        nullif("レース条件_トラック情報_内外", '') as "レース条件_トラック情報_内外",
        nullif("レース条件_種別", '') as "レース条件_種別",
        nullif("レース条件_条件", '') as "レース条件_条件",
        nullif("レース条件_記号", '') as "レース条件_記号",
        nullif("レース条件_重量", '') as "レース条件_重量",
        nullif("レース条件_グレード", '') as "レース条件_グレード",
        nullif("レース名", '') as "レース名",
        nullif("回数", '') as "回数",
        nullif("頭数", '') as "頭数",
        nullif("コース", '') as "コース",
        nullif("開催区分", '') as "開催区分",
        nullif("レース名短縮", '') as "レース名短縮",
        nullif("レース名９文字", '') as "レース名９文字",
        nullif("データ区分", '') as "データ区分",
        nullif("１着賞金", '') as "１着賞金",
        nullif("２着賞金", '') as "２着賞金",
        nullif("３着賞金", '') as "３着賞金",
        nullif("４着賞金", '') as "４着賞金",
        nullif("５着賞金", '') as "５着賞金",
        nullif("１着算入賞金", '') as "１着算入賞金",
        nullif("２着算入賞金", '') as "２着算入賞金",
        coalesce(nullif("馬券発売フラグ_単勝", ''), '0')::boolean as "馬券発売フラグ_単勝",
        coalesce(nullif("馬券発売フラグ_複勝", ''), '0')::boolean as "馬券発売フラグ_複勝",
        coalesce(nullif("馬券発売フラグ_枠連", ''), '0')::boolean as "馬券発売フラグ_枠連",
        coalesce(nullif("馬券発売フラグ_馬連", '') , '0')::boolean as "馬券発売フラグ_馬連",
        coalesce(nullif("馬券発売フラグ_馬単", '') , '0')::boolean as "馬券発売フラグ_馬単",
        coalesce(nullif("馬券発売フラグ_ワイド", ''), '0')::boolean as "馬券発売フラグ_ワイド",
        coalesce(nullif("馬券発売フラグ_３連複", ''), '0')::boolean as "馬券発売フラグ_３連複",
        coalesce(nullif("馬券発売フラグ_３連単", ''), '0')::boolean as "馬券発売フラグ_３連単",
        nullif("WIN5フラグ", '') as "WIN5フラグ"
    from source
)
select * from final
