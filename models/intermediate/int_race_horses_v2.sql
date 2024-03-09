with
  race_horses_base as (
  select
    kyi.`レースキー`,
    kyi.`馬番`,
    kyi.`血統登録番号`,
    bac.`発走日時`,
    case when sed.`馬成績_着順` = 1 then 1 else 0 end as `単勝的中`,
    case when sed.`馬成績_着順` <= 3 then 1 else 0 end as `複勝的中`,
    sed.`ＪＲＤＢデータ_不利` as `不利`,
    sed.`馬成績_異常区分` as `異常区分`,
    date_diff(bac.`発走日時`, kyi.`入厩年月日`) as `入厩何日前`,
    bac.`頭数`,
    sed.`馬成績_着順` as `着順`,
    sed.`馬成績_タイム` as `タイム`,
    row_number() over (partition by kyi.`血統登録番号` order by bac.`発走日時`) - 1 as `レース数`,
    coalesce(
      cast(
        sum(case when sed.`馬成績_着順` <= 3 then 1 else 0 end)
        over (partition by kyi.`血統登録番号` order by bac.`発走日時` rows between unbounded preceding and 1 preceding)
        as integer
      ),
    0) as `複勝回数`,
    {% for i in range(1, 7) %}
    lag(sed.`ＪＲＤＢデータ_不利`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `{{ i }}走前不利`,
    date_diff(bac.`発走日時`, lag(bac.`発走日時`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`)) as `{{ i }}走前経過日数`,
    lag(bac.`頭数`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `{{ i }}走前頭数`,
    lag(sed.`馬成績_着順`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `{{ i }}走前着順`,
    sed.`馬成績_タイム` - lag(sed.`馬成績_タイム`, {{ i }}) over (partition by kyi.`レースキー` order by sed.`馬成績_着順`) as `先行馬{{ i }}タイム差`,
    sed.`馬成績_タイム` - lead(sed.`馬成績_タイム`, {{ i }}) over (partition by kyi.`レースキー` order by sed.`馬成績_着順`) as `後続馬{{ i }}タイム差`,
    lag(kyi.`休養理由分類コード`, {{ i }}) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `{{ i }}走前休養理由分類コード`
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
  ),

  final as (
  select
    `レースキー` as `meta_レースキー`,
    `馬番` as `meta_馬番`,
    `血統登録番号` as `meta_血統登録番号`,
    `発走日時` as `meta_発走日時`,
    `単勝的中` as `meta_単勝的中`,
    `複勝的中` as `meta_複勝的中`,
    `不利` as `meta_不利`,
    `異常区分` as `meta_異常区分`,
    `入厩何日前` as `num_入厩何日前`,
    `頭数` as `num_頭数`,
    `着順` as `num_着順`,
    `タイム` as `num_タイム`,
    `レース数` as `num_レース数`,
    `複勝回数` as `num_複勝回数`,
    {% for i in range(1, 7) %}
    `{{ i }}走前不利` as `num_{{ i }}走前不利`,
    `{{ i }}走前経過日数` as `num_{{ i }}走前経過日数`,
    `{{ i }}走前頭数` as `num_{{ i }}走前頭数`,
    `{{ i }}走前着順` as `num_{{ i }}走前着順`,
    `先行馬{{ i }}タイム差` as `num_先行馬{{ i }}タイム差`,
    `後続馬{{ i }}タイム差` as `num_後続馬{{ i }}タイム差`,
    `{{ i }}走前休養理由分類コード` as `cat_{{ i }}走前休養理由分類コード`,
    {% endfor %}
    base.`複勝回数` / base.`レース数` as `num_複勝率`
  from
    race_horses_base base
  )

select
  *
from
  final
