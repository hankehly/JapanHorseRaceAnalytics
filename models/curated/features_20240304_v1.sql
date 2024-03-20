with
  final as (
  select
    coalesce(win_payouts.`払戻金`, 0) as `meta_単勝払戻金`,
    coalesce(place_payouts.`払戻金`, 0) as `meta_複勝払戻金`,
    race_horses.`meta_レースキー`,
    race_horses.`meta_馬番`,
    race_horses.`meta_血統登録番号`,
    race_horses.`meta_発走日時`,
    race_horses.`meta_単勝的中`,
    race_horses.`meta_単勝オッズ`,
    race_horses.`meta_複勝的中`,
    race_horses.`meta_複勝オッズ`,
    race_horses.`meta_着順`,
    race_horses.`meta_タイム`,
    race_horses.`meta_不利`,
    race_horses.`meta_異常区分`,
    race_horses.`meta_後３Ｆタイム`,
    race_horses.`meta_3着タイム差`,
    race_horses.`meta_3着タイム`,
    race_horses.`cat_馬場状態`,
    race_horses.`cat_場コード`,
    race_horses.`num_入厩何日前`,
    race_horses.`num_頭数`,
    race_horses.`num_年齢`,
    race_horses.`cat_性別`,
    race_horses.`num_ローテーション`,
    race_horses.`num_負担重量`,
    race_horses.`num_馬体重`,
    race_horses.`num_平均馬体重差`,
    race_horses.`num_レース数`,
    race_horses.`num_複勝回数`,
    race_horses.`num_複勝率`,
    {% for i in range(1, 7) %}
    race_horses.`num_{{ i }}走前ＩＤＭ`,
    race_horses.`num_{{ i }}走前距離`,
    race_horses.`num_{{ i }}走前不利`,
    race_horses.`num_{{ i }}走前経過日数`,
    race_horses.`num_{{ i }}走前頭数`,
    race_horses.`num_{{ i }}走前着順`,
    {% for j in range(1, 6) %}
    `num_{{ i }}走前後続馬{{ j }}タイム差`,
    {% endfor %}
    race_horses.`cat_{{ i }}走前休養理由分類コード`,
    race_horses.`num_{{ i }}走前3着タイム差`,
    {% endfor %}

    races.`cat_トラック種別`,
    races.`num_距離`,
    races.`cat_距離区分`,

    -- 20240302_eda
      ((race_horses.`num_1走前着順` - 1) / (race_horses.`num_1走前頭数` - 1) * 0.2927557)
    + ((race_horses.`num_2走前着順` - 1) / (race_horses.`num_2走前頭数` - 1) * 0.1465141)
    + ((race_horses.`num_3走前着順` - 1) / (race_horses.`num_3走前頭数` - 1) * 0.1004372) as `num_過去3走重み付き着順成績`,

    -- 20240302_eda
    1 / coalesce(race_horses.`num_入厩何日前`, 1) as `num_入厩何日前逆数`,

    -- 20240302_eda
    race_horses.`num_レース数` >= 5 and race_horses.`num_複勝率` >= 0.8 as `cat_堅実な馬`,

    -- 20240302_eda
   -- Initialize 'Excuse' column based on conditions
    case
      when race_horses.`num_1走前着順` > 3 and race_horses.`num_1走前不利` >= 1 then true
      when race_horses.`num_2走前着順` > 3 and race_horses.`num_2走前不利` >= 1 then true
      when race_horses.`num_3走前着順` > 3 and race_horses.`num_3走前不利` >= 1 then true
      when race_horses.`cat_3走前休養理由分類コード` IN ('01', '02', '03', '04', '05', '06', '07') and race_horses.`num_3走前着順` > 3 then true
      else false
    end as `cat_過去3走中1走訳あり凡走`,
    -- Check if horse placed well in 2 of the last 3 races
    (
      case when race_horses.`num_1走前着順` <= 3 then 1 else 0 end +
      case when race_horses.`num_2走前着順` <= 3 then 1 else 0 end +
      case when race_horses.`num_3走前着順` <= 3 then 1 else 0 end
    ) = 2 as `cat_過去3走中2走好走`,
    -- Determine if a row belongs to the target group
    case
      when
        (
          case when race_horses.`num_1走前着順` <= 3 then 1 else 0 end +
          case when race_horses.`num_2走前着順` <= 3 then 1 else 0 end +
          case when race_horses.`num_3走前着順` <= 3 then 1 else 0 end
        ) = 2
        and (
          race_horses.`num_1走前着順` > 3 and race_horses.`num_1走前不利` >= 1 or
          race_horses.`num_2走前着順` > 3 and race_horses.`num_2走前不利` >= 1 or
          race_horses.`num_3走前着順` > 3 and race_horses.`num_3走前不利` >= 1 or
          race_horses.`cat_3走前休養理由分類コード` IN ('01', '02', '03', '04', '05', '06', '07') and race_horses.`num_3走前着順` > 3
        )
      then true
      else false
    end as `cat_過去3走繋がりあり`

  from
    {{ ref('int_race_horses_v2') }} race_horses

  left join
    {{ ref('int_win_payouts') }} win_payouts
  on
    race_horses.`meta_レースキー` = win_payouts.`レースキー`
    and race_horses.`meta_馬番` = win_payouts.`馬番`

  left join
    {{ ref('int_place_payouts') }} place_payouts
  on
    race_horses.`meta_レースキー` = place_payouts.`レースキー`
    and race_horses.`meta_馬番` = place_payouts.`馬番`

  inner join
    {{ ref('int_races') }} races
  on
    race_horses.`meta_レースキー` = races.`meta_int_races_レースキー`
  )

select * from final
