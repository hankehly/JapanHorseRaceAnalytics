with
  oz as (
  select
    *
  from
    {{ ref('stg_jrdb__oz') }}
  ),
  final as (
  select
    レースキー,
    開催キー,
    レースキー_場コード,
    レースキー_年,
    レースキー_回,
    レースキー_日,
    レースキー_Ｒ,
    cast(idx as integer) as 馬番,
    cast(nullif(el, '') as numeric) 単勝オッズ
  from
    oz,
    unnest(単勝オッズ) WITH ORDINALITY AS t(el, idx)
  where
    nullif(el, '') is not null
)
select * from final
