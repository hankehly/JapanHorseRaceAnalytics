with
  duplicates as (
  select
    "開催キー_場コード",
    "開催キー_年",
    "開催キー_回",
    "開催キー_日",
    -- 同じ開催キーのレースは基本的に同じ日に行われるが、天気によって延期されることが稀にある（例: 2011年1小倉7）
    -- そのため「開催キー + 年月日」が本当のキーとなる。
    -- 2023/12/17時点の最新データに、同じ開催キーのレースで年月日が異なるレコードは10件あった。
    "年月日",
    count(*)
  from
    jrdb_raw.kab
  group by
    "開催キー_場コード",
    "開催キー_年",
    "開催キー_回",
    "開催キー_日",
    "年月日"
  having
    count(*) > 1
  ),
  duplicates_with_sk as (
  select
    row_number() over (partition by "開催キー_場コード", "開催キー_年", "開催キー_回", "開催キー_日", "年月日" order by kab_sk) rn,
    *
  from
    jrdb_raw.kab
  where
    ("開催キー_場コード", "開催キー_年", "開催キー_回", "開催キー_日", "年月日") in (select "開催キー_場コード", "開催キー_年", "開催キー_回", "開催キー_日", "年月日" from duplicates)
  )

-- The following query will return all duplicate rows.
select
  *
from
  duplicates_with_sk