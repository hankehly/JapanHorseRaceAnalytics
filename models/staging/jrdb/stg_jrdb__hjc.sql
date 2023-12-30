with source as (
      select * from {{ source('jrdb', 'hjc') }}
),
final as (
    select
        hjc_sk,
        concat(
            nullif("レースキー_場コード", ''),
            nullif("レースキー_年", ''),
            nullif("レースキー_回", ''),
            nullif("レースキー_日", ''),
            nullif("レースキー_Ｒ", '')
        ) as hjc_bk,
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
        coalesce("単勝払戻", '{}') as "単勝払戻",
        coalesce("複勝払戻", '{}') as "複勝払戻",
        coalesce("枠連払戻", '{}') as "枠連払戻",
        coalesce("馬連払戻", '{}') as "馬連払戻",
        coalesce("ワイド払戻", '{}') as "ワイド払戻",
        coalesce("馬単払戻", '{}') as "馬単払戻",
        coalesce("３連複払戻", '{}') as "３連複払戻",
        coalesce("３連単払戻", '{}') as "３連単払戻"
    from source
)
select * from final
