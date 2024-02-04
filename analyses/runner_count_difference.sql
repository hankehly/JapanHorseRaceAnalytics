with
  bac as (
  select
    "レースキー",
    "頭数"
  from
    {{ ref('stg_jrdb__bac') }}
  ),

  kyi as (
  select
    "レースキー",
    count(*) as "頭数"
  from
    {{ ref('stg_jrdb__kyi') }}
  group by
    "レースキー"
  ),

  final as (
  select
    kyi."レースキー",
    kyi."頭数" as "頭数_kyi",
    bac."頭数" as "頭数_bac"
  from
    kyi
  inner join
    bac
  using
    ("レースキー")
  )

select * from final where 頭数_kyi != "頭数_bac"
