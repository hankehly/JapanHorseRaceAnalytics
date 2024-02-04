with
  source as (
  select
    *
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
    `馬番`,
    count(*)
  from
    source
  group by
    `レースキー_場コード`,
    `レースキー_年`,
    `レースキー_回`,
    `レースキー_日`,
    `レースキー_Ｒ`,
    `馬番`
  having
    count(*) > 1
  ),

  duplicates_with_sk as (
  select
    -- For tyb, the row with the highest sk contains the more complete data.
    row_number() over (partition by `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番` order by tyb_sk desc) rn,
    *
  from
    source
  where
    (`レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番`) in (select `レースキー_場コード`, `レースキー_年`, `レースキー_回`, `レースキー_日`, `レースキー_Ｒ`, `馬番` from duplicates)
  ),

  source_dedupe as (
  select
    *
  from
    source
  where
    tyb_sk not in (select tyb_sk from duplicates_with_sk where rn > 1)
  ),

  final as (
  select
    tyb_sk,
    concat(
      nullif(`レースキー_場コード`, ''),
      nullif(`レースキー_年`, ''),
      nullif(`レースキー_回`, ''),
      nullif(`レースキー_日`, ''),
      nullif(`レースキー_Ｒ`, ''),
      nullif(`馬番`, '')
    ) as tyb_bk,
    concat(
      nullif(`レースキー_場コード`, ''),
      nullif(`レースキー_年`, ''),
      nullif(`レースキー_回`, ''),
      nullif(`レースキー_日`, ''),
      nullif(`レースキー_Ｒ`, '')
    ) as `レースキー`,
    concat(
      nullif(`レースキー_場コード`, ''),
      nullif(`レースキー_年`, ''),
      nullif(`レースキー_回`, ''),
      nullif(`レースキー_日`, '')
    ) as `開催キー`,
    nullif(`レースキー_場コード`, '') as `レースキー_場コード`,
    nullif(`レースキー_年`, '') as `レースキー_年`,
    nullif(`レースキー_回`, '') as `レースキー_回`,
    nullif(`レースキー_日`, '') as `レースキー_日`,
    nullif(`レースキー_Ｒ`, '') as `レースキー_Ｒ`,
    nullif(`馬番`, '') as `馬番`,
    cast(nullif(`ＩＤＭ`, '') as float) as `ＩＤＭ`,
    cast(nullif(`騎手指数`, '') as float) as `騎手指数`,
    cast(nullif(`情報指数`, '') as float) as `情報指数`,
    cast(nullif(`オッズ指数`, '') as float) as `オッズ指数`,
    cast(nullif(`パドック指数`, '') as float) as `パドック指数`,
    nullif(`予備１`, '') as `予備１`,
    cast(nullif(`総合指数`, '') as float) as `総合指数`,
    nullif(`馬具変更情報`, '') as `馬具変更情報`,
    nullif(`脚元情報`, '') as `脚元情報`,
    cast(nullif(`取消フラグ`, '') as boolean) as `取消フラグ`,
    nullif(`騎手コード`, '') as `騎手コード`,
    nullif(`騎手名`, '') as `騎手名`,
    cast(nullif(`負担重量`, '') as integer) as `負担重量`,
    nullif(`見習い区分`, '') as `見習い区分`,
    nullif(`馬場状態コード`, '') as `馬場状態コード`,
    nullif(`天候コード`, '') as `天候コード`,

    cast(nullif(`単勝オッズ`, '') as float) as `単勝オッズ`,
    cast(nullif(`複勝オッズ`, '') as float) as `複勝オッズ`,

    -- values like `107` exist..
    -- cast(nullif(left(`オッズ取得時間`, 2) || ':' || right(`オッズ取得時間`, 2), '') as time) as `オッズ取得時間`,
    nullif(`オッズ取得時間`, '') as `オッズ取得時間`,

    cast(nullif(`馬体重`, '') as integer) as `馬体重`,
    cast(nullif(replace(replace(`馬体重増減`, '+', ''), ' ', ''), '') as integer) as `馬体重増減`,
    nullif(`オッズ印`, '') as `オッズ印`,
    nullif(`パドック印`, '') as `パドック印`,
    nullif(`直前総合印`, '') as `直前総合印`,

    -- 0 is not a valid value for `馬体コード` but there are 42 rows with it.
    -- This is a workaround to avoid the error.
    case when `馬体コード` in (select code from {{ ref('jrdb__horse_form_codes') }}) then `馬体コード` else null end `馬体コード`,

    -- 0 is not a valid value for `気配コード` but there are 42 rows with it.
    -- This is a workaround to avoid the error.
    case when `気配コード` in (select code from {{ ref('jrdb__demeanor_codes') }}) then `気配コード` else null end `気配コード`,

    -- values like `09:90` exist..
    -- case
    --   when `発走時間` = ''
    --     then null
    --   else
    --     cast(nullif(left(`発走時間`, 2) || ':' || right(`発走時間`, 2), '') as time)
    -- end `発走時間`
    nullif(`発走時間`, '') as `発走時間`
  from
    source_dedupe
  )

select
  *
from
  final
  