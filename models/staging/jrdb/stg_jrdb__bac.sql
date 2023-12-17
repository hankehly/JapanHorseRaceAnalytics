with source as (
      select * from {{ source('jrdb', 'bac') }}
),
final as (
    select
        "レースキー_場コード" || "レースキー_年" || "レースキー_回" || "レースキー_日" || "レースキー_Ｒ" as "レースキー",
        {{ adapter.quote("レースキー_場コード") }},
        {{ adapter.quote("レースキー_年") }},
        {{ adapter.quote("レースキー_回") }},
        {{ adapter.quote("レースキー_日") }},
        {{ adapter.quote("レースキー_Ｒ") }},
        to_date({{ adapter.quote("年月日") }}, 'YYYYMMDD')
        {{ adapter.quote("発走時間") }},
        {{ adapter.quote("レース条件_距離") }}::integer,
        {{ adapter.quote("レース条件_トラック情報_芝ダ障害コード") }},
        {{ adapter.quote("レース条件_トラック情報_右左") }},
        {{ adapter.quote("レース条件_トラック情報_内外") }},
        {{ adapter.quote("レース条件_種別") }},
        {{ adapter.quote("レース条件_条件") }},
        {{ adapter.quote("レース条件_記号") }},
        {{ adapter.quote("レース条件_重量") }},
        {{ adapter.quote("レース条件_グレード") }},
        {{ adapter.quote("レース名") }},
        {{ adapter.quote("回数") }},
        {{ adapter.quote("頭数") }}::integer,
        {{ adapter.quote("コース") }},
        {{ adapter.quote("開催区分") }},
        {{ adapter.quote("レース名短縮") }},
        {{ adapter.quote("レース名９文字") }},
        {{ adapter.quote("データ区分") }},
        {{ adapter.quote("１着賞金") }}::integer,
        {{ adapter.quote("２着賞金") }}::integer,
        {{ adapter.quote("３着賞金") }}::integer,
        {{ adapter.quote("４着賞金") }}::integer,
        {{ adapter.quote("５着賞金") }}::integer,
        {{ adapter.quote("１着算入賞金") }}::integer,
        {{ adapter.quote("２着算入賞金") }}::integer,
        {{ adapter.quote("馬券発売フラグ_単勝") }}::boolean,
        {{ adapter.quote("馬券発売フラグ_複勝") }}::boolean,
        {{ adapter.quote("馬券発売フラグ_枠連") }}::boolean,
        {{ adapter.quote("馬券発売フラグ_馬連") }}::boolean,
        {{ adapter.quote("馬券発売フラグ_馬単") }}::boolean,
        {{ adapter.quote("馬券発売フラグ_ワイド") }}::boolean,
        {{ adapter.quote("馬券発売フラグ_３連複") }}::boolean,
        {{ adapter.quote("馬券発売フラグ_３連単") }}::boolean,
        {{ adapter.quote("WIN5フラグ") }}
    from source
)
select * from final
  