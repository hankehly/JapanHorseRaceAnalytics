with source_data as (
    SELECT * FROM public.mytable
)

select
  CONCAT("レースキー_場コード", "レースキー_年", "レースキー_回", "レースキー_日", "レースキー_Ｒ") AS "レースキー"
from
  source_data
