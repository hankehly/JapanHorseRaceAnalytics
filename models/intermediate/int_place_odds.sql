{{
  config(
    materialized='table',
    schema='intermediate',
    indexes=[{'columns': ['レースキー', '馬番'], 'unique': True}]
  )
}}

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
    lpad(cast(idx as varchar), 2, '0') as 馬番,
    cast(nullif(el, '') as numeric) 複勝オッズ
  from
    oz,
    unnest(複勝オッズ) WITH ORDINALITY AS t(el, idx)
  where
    nullif(el, '') is not null
)

select
  *
from
  final
