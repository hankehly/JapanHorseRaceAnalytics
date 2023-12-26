with source as (
      select * from {{ source('jrdb', 'oz') }}
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
        cast(nullif("登録頭数", '') as integer) as "登録頭数",
        nullif("単勝オッズ", '{}') as "単勝オッズ",
        nullif("複勝オッズ", '{}') as "複勝オッズ",
        nullif("連勝オッズ", '{}') as "連勝オッズ"
    from source
)
select * from final
  