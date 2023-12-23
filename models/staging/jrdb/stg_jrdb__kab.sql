with source as (
      select * from {{ source('jrdb', 'kab') }}
),
final as (
    select
        concat(
            nullif("開催キー_場コード", ''),
            nullif("開催キー_年", ''),
            nullif("開催キー_回", ''),
            nullif("開催キー_日", '')
        ) as "開催キー",
        nullif("開催キー_場コード", '') as "開催キー_場コード",
        nullif("開催キー_年", '') as "開催キー_年",
        nullif("開催キー_回", '') as "開催キー_回",
        nullif("開催キー_日", '') as "開催キー_日",
        to_date(nullif("年月日", ''), 'YYYYMMDD') as "年月日",
        nullif("開催区分", '') as "開催区分",
        nullif("曜日", '') as "曜日",
        nullif("場名", '') as "場名",
        nullif("天候コード", '') as "天候コード",
        nullif("芝馬場状態コード", '') as "芝馬場状態コード",
        nullif("芝馬場状態内", '') as "芝馬場状態内",
        nullif("芝馬場状態中", '') as "芝馬場状態中",
        nullif("芝馬場状態外", '') as "芝馬場状態外",
        cast(nullif("芝馬場差", '') as integer) as "芝馬場差",
        -- There are 36 values with trailing dots "1." in the source data from years 1999-2000.
        -- We need to remove the dot to cast it to integer.
        cast(nullif(replace("直線馬場差最内", '.', ''), '') as integer) as "直線馬場差最内",
        cast(nullif(replace("直線馬場差内", '.', ''), '') as integer) as "直線馬場差内",
        cast(nullif(replace("直線馬場差中", '.', ''), '') as integer) as "直線馬場差中",
        cast(nullif(replace("直線馬場差外", '.', ''), '') as integer) as "直線馬場差外",
        cast(nullif(replace("直線馬場差大外", '.', ''), '') as integer) as "直線馬場差大外",
        nullif("ダ馬場状態コード", '') as "ダ馬場状態コード",
        nullif("ダ馬場状態内", '') as "ダ馬場状態内",
        nullif("ダ馬場状態中", '') as "ダ馬場状態中",
        nullif("ダ馬場状態外", '') as "ダ馬場状態外",
        cast(nullif("ダ馬場差", '') as integer) as "ダ馬場差",
        nullif("データ区分", '') as "データ区分",
        cast(nullif("連続何日目", '') as integer) as "連続何日目",
        nullif("芝種類", '') as "芝種類",
        cast(nullif("草丈", '') as numeric) as "草丈",
        cast(nullif("転圧", '') as boolean) as "転圧",
        cast(nullif("凍結防止剤", '') as boolean) as "凍結防止剤",
        cast(nullif("中間降水量", '') as numeric) as "中間降水量"
    from source
)
select * from final
  