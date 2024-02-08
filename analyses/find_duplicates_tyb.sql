with
  distinct_rows as (
  -- removed surrogate key which is always unique
  select distinct
    `レースキー_場コード`,
    `レースキー_年`,
    `レースキー_回`,
    `レースキー_日`,
    `レースキー_Ｒ`,
    `馬番`,
    `ＩＤＭ`,
    `騎手指数`,
    `情報指数`,
    `オッズ指数`,
    `パドック指数`,
    `予備１`,
    `総合指数`,
    `馬具変更情報`,
    `脚元情報`,
    `取消フラグ`,
    `騎手コード`,
    `騎手名`,
    `負担重量`,
    `見習い区分`,
    `馬場状態コード`,
    `天候コード`,
    `単勝オッズ`,
    `複勝オッズ`,
    `オッズ取得時間`,
    `馬体重`,
    `馬体重増減`,
    `オッズ印`,
    `パドック印`,
    `直前総合印`,
    `馬体コード`,
    `気配コード`,
    `発走時間`
  from
    {{ source('jrdb', 'raw_jrdb__tyb') }}
  ),
  duplicates as (
  select
    `レースキー_場コード`,
    `レースキー_年`,
    `レースキー_回`,
    `レースキー_日`,
    `レースキー_Ｒ`,
    `馬番`
  from
    distinct_rows
  group by
    `レースキー_場コード`,
    `レースキー_年`,
    `レースキー_回`,
    `レースキー_日`,
    `レースキー_Ｒ`,
    `馬番`
  having
    count(*) > 1
  )
select
  *
from
  {{ source('jrdb', 'raw_jrdb__tyb') }}
where
  (`レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番`) in (select `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番` from duplicates)
order by
  `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番`
