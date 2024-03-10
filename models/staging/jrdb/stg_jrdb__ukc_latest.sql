with
  final as (
  select
    *
  from
    {{ ref('stg_jrdb__ukc') }}
  where
    (`血統登録番号`, `データ年月日`) in (select `血統登録番号`, MAX(`データ年月日`) from {{ ref('stg_jrdb__ukc') }} group by `血統登録番号`)
  )

select * from final
