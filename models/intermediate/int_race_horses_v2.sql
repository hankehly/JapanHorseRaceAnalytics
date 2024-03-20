with
  race_3rd_placers as (
  select
    `レースキー`,
    `タイム`
  from (
    select
      `レースキー`,
      `馬成績_タイム` as `タイム`,
      -- See rank_race_positions.sql for an example of why we must determine order like this
      row_number() over (partition by `レースキー` order by `馬成績_着順` asc) as race_rank
    from
      {{ ref('stg_jrdb__sed') }}
    where
      -- row_number will count nulls, so we must filter them out
      `馬成績_着順` is not null
    )
  where
    race_rank = 3
  ),

  base as (
  select
    kyi.`レースキー`,
    kyi.`馬番`,
    kyi.`血統登録番号`,
    bac.`発走日時`,
    case when sed.`馬成績_着順` = 1 then 1 else 0 end as `単勝的中`,
    sed.`馬成績_確定単勝オッズ` as `単勝オッズ`,
    case when sed.`馬成績_着順` <= 3 then 1 else 0 end as `複勝的中`,
    sed.`確定複勝オッズ下` as `複勝オッズ`,
    sed.`馬成績_着順` as `着順`,
    sed.`馬成績_タイム` as `タイム`,
    sed.`ＪＲＤＢデータ_不利` as `不利`,
    sed.`馬成績_異常区分` as `異常区分`,
    sed.`ＪＲＤＢデータ_後３Ｆタイム` as `後３Ｆタイム`,
    sed.`馬成績_タイム` - race_3rd_placers.`タイム` as `3着タイム差`,
    race_3rd_placers.`タイム` as `3着タイム`,
    case
      when going_codes.name in ('良', '速良', '遅良') then '良'
      when going_codes.name in ('稍重', '速稍重', '遅稍重') then '稍重'
      when going_codes.name in ('重', '速重', '遅重') then '重'
      when going_codes.name in ('不良', '速不良', '遅不良') then '不良'
      else null
    end `馬場状態`,
    kyi.`レースキー_場コード` as `場コード`,
    date_diff(bac.`発走日時`, kyi.`入厩年月日`) as `入厩何日前`,
    bac.`頭数`,
    (months_between(bac.`発走日時`, ukc.`生年月日`) / 12) as `年齢`,
    case
      when ukc.`性別コード` = '1' then '牡'
      when ukc.`性別コード` = '2' then '牝'
      else 'セン'
    end `性別`,
    kyi.`ローテーション`,
    coalesce(tyb.`負担重量`, kyi.`負担重量`) as `負担重量`,
    tyb.`馬体重` as `馬体重`,
    tyb.`馬体重` - coalesce(
      avg(tyb.`馬体重`)
      over (partition by kyi.`血統登録番号` order by bac.`発走日時` rows between unbounded preceding and 1 preceding),
      tyb.`馬体重`
    ) as `平均馬体重差`,
    row_number() over (partition by kyi.`血統登録番号` order by bac.`発走日時`) - 1 as `レース数`,
    coalesce(
      cast(
        sum(case when sed.`馬成績_着順` <= 3 then 1 else 0 end)
        over (partition by kyi.`血統登録番号` order by bac.`発走日時` rows between unbounded preceding and 1 preceding)
        as integer
      ),
    0) as `複勝回数`,

    {% for i in range(1, 7) %}
    lag(sed.`ＪＲＤＢデータ_ＩＤＭ`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `{{ i }}走前ＩＤＭ`,
    lag(bac.`レース条件_距離`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `{{ i }}走前距離`,
    lag(sed.`ＪＲＤＢデータ_不利`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `{{ i }}走前不利`,
    date_diff(bac.`発走日時`, lag(bac.`発走日時`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`)) as `{{ i }}走前経過日数`,
    lag(bac.`頭数`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `{{ i }}走前頭数`,
    lag(sed.`馬成績_着順`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `{{ i }}走前着順`,
    lag(kyi.`休養理由分類コード`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `{{ i }}走前休養理由分類コード`,
    lag(sed.`馬成績_タイム` - race_3rd_placers.`タイム`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `{{ i }}走前3着タイム差`,
    {% endfor %}

    {% for i in range(1, 6) %}
    sed.`馬成績_タイム` - lead(sed.`馬成績_タイム`, {{ i }}) over (partition by kyi.`レースキー` order by sed.`馬成績_着順`) as `後続馬{{ i }}タイム差`
    {%- if not loop.last %},{% endif -%}
    {% endfor %}

  from
    {{ ref('stg_jrdb__kyi') }} kyi
  -- 前日系は inner join
  inner join
    {{ ref('stg_jrdb__bac') }} bac
  on
    kyi.`レースキー` = bac.`レースキー`
  -- 実績系はレースキーがないかもしれないから left join
  left join
    {{ ref('stg_jrdb__sed') }} sed
  on
    kyi.`レースキー` = sed.`レースキー`
    and kyi.`馬番` = sed.`馬番`
  left join
    race_3rd_placers
  on
    race_3rd_placers.`レースキー` = kyi.`レースキー`
  inner join
    {{ ref('stg_jrdb__ukc_latest') }} ukc
  on
    kyi.`血統登録番号` = ukc.`血統登録番号`
  -- TYBが公開される前に予測する可能性があるからleft join
  left join
    {{ ref('stg_jrdb__tyb') }} tyb
  on
    kyi.`レースキー` = tyb.`レースキー`
    and kyi.`馬番` = tyb.`馬番`

  left join
    {{ ref('jrdb__going_codes')}} going_codes
  on
    sed.`レース条件_馬場状態` = going_codes.code
  ),

  final as (
  select
    `レースキー` as `meta_レースキー`,
    `馬番` as `meta_馬番`,
    `血統登録番号` as `meta_血統登録番号`,
    `発走日時` as `meta_発走日時`,
    `単勝的中` as `meta_単勝的中`,
    `単勝オッズ` as `meta_単勝オッズ`,
    `複勝的中` as `meta_複勝的中`,
    `複勝オッズ` as `meta_複勝オッズ`,
    `着順` as `meta_着順`,
    `タイム` as `meta_タイム`,
    `不利` as `meta_不利`,
    `異常区分` as `meta_異常区分`,
    `後３Ｆタイム` as `meta_後３Ｆタイム`,
    `3着タイム差` as `meta_3着タイム差`,
    `3着タイム` as `meta_3着タイム`,
    `馬場状態` as `cat_馬場状態`,
    `場コード` as `cat_場コード`,
    `入厩何日前` as `num_入厩何日前`,
    `頭数` as `num_頭数`,
    `年齢` as `num_年齢`,
    `性別` as `cat_性別`,
    `ローテーション` as `num_ローテーション`,
    `負担重量` as `num_負担重量`,
    `馬体重` as `num_馬体重`,
    `平均馬体重差` as `num_平均馬体重差`,
    `レース数` as `num_レース数`,
    `複勝回数` as `num_複勝回数`,
    base.`複勝回数` / base.`レース数` as `num_複勝率`,
    {% for i in range(1, 7) %}
    `{{ i }}走前ＩＤＭ` as `num_{{ i }}走前ＩＤＭ`,
    `{{ i }}走前距離` as `num_{{ i }}走前距離`,
    `{{ i }}走前不利` as `num_{{ i }}走前不利`,
    `{{ i }}走前経過日数` as `num_{{ i }}走前経過日数`,
    `{{ i }}走前頭数` as `num_{{ i }}走前頭数`,
    `{{ i }}走前着順` as `num_{{ i }}走前着順`,
    {% for j in range(1, 6) %}
    lag(`後続馬{{ j }}タイム差`, {{ i }}) over (partition by `血統登録番号` order by `発走日時`) as `num_{{ i }}走前後続馬{{ j }}タイム差`,
    {% endfor %}
    `{{ i }}走前休養理由分類コード` as `cat_{{ i }}走前休養理由分類コード`,
    `{{ i }}走前3着タイム差` as `num_{{ i }}走前3着タイム差`
    {%- if not loop.last %},{% endif -%}
    {% endfor %}

  from
    base base
  )

select
  *
from
  final
