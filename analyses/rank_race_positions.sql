select
  `レースキー`,
  `馬成績_タイム` as `タイム`,
  `馬成績_着順` as `着順`,
  dense_rank() over (partition by `レースキー` order by `馬成績_着順` asc) as dense_rank_,
  rank() over (partition by `レースキー` order by `馬成績_着順` asc) as rank_,
  row_number() over (partition by `レースキー` order by `馬成績_着順` asc) as row_number_
from
  {{ ref('stg_jrdb__sed') }}
where
  `レースキー` in (
    '10051512', -- no horses in 3rd place
    '01041107' -- has disqualified horse
  )
order by
  `レースキー`,
  `馬成績_着順`
