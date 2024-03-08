with
  final as (
  select
    -- Metadata (do not use for training)
    kyi.`レースキー` as `meta_レースキー`,
    kyi.`馬番` as `meta_馬番`,
    sed.`馬成績_着順` as `meta_着順`,
    races.`meta_int_races_発走日時` as `meta_発走日時`,
    sed.`本賞金` as `meta_本賞金`,
    case when sed.`馬成績_着順` = 1 then true else false end as `meta_単勝的中`,
    coalesce(win_payouts.`払戻金`, 0) as `meta_単勝払戻金`,
    case when sed.`馬成績_着順` <= 3 then true else false end as `meta_複勝的中`,
    coalesce(place_payouts.`払戻金`, 0) as `meta_複勝払戻金`,
    race_horses.`meta_int_race_horses_異常区分`,
    race_horses.`num_実績複勝オッズ` as `meta_実績複勝オッズ`,

    -- Race features
    races.`cat_トラック種別`,

    --
    -- Horse features
    --

    -- Keepers --
    race_horses.`num_休養日数`,
    -- 20240302_eda
      ((race_horses.`num_一走前着順` - 1) / (race_horses.`num_1走前頭数` - 1) * 0.2927557)
    + ((race_horses.`num_二走前不利` - 1) / (race_horses.`num_2走前頭数` - 1) * 0.1465141)
    + ((race_horses.`num_三走前着順` - 1) / (race_horses.`num_3走前頭数` - 1) * 0.1004372) as `num_過去3走重み付き着順成績`,
    race_horses.`num_トップ3完走率` as `num_複勝率`,
    -------------

    race_horses.`num_一走前着順` as `num_1走前着順`,
    race_horses.`num_二走前着順` as `num_2走前着順`,
    race_horses.`num_三走前着順` as `num_3走前着順`,
    race_horses.`num_四走前着順` as `num_4走前着順`,
    race_horses.`num_五走前着順` as `num_5走前着順`,
    race_horses.`num_1走前経過日数`,
    race_horses.`num_2走前経過日数`,
    race_horses.`num_3走前経過日数`,
    race_horses.`num_4走前経過日数`,
    race_horses.`num_5走前経過日数`,
    race_horses.`num_頭数`,
    race_horses.`num_1走前頭数`,
    race_horses.`num_2走前頭数`,
    race_horses.`num_3走前頭数`,

    -- 20240302_eda
    race_horses.`num_入厩何日前`,
    1 / coalesce(race_horses.`num_入厩何日前`, 1) as `num_入厩何日前逆数`,


    -- 20240302_eda
    race_horses.`num_レース数` >= 5 and race_horses.`num_トップ3完走率` >= 0.8 as `cat_堅実な馬`,

    -- 20240302_eda
   -- Initialize 'Excuse' column based on conditions
    case
      when race_horses.`num_一走前着順` > 3 and race_horses.`num_一走前不利` >= 1 then true
      when race_horses.`num_二走前不利` > 3 and race_horses.`num_二走前不利` >= 1 then true
      when race_horses.`num_三走前着順` > 3 and race_horses.`num_三走前不利` >= 1 then true
      when race_horses.`cat_3走前休養理由分類コード` IN ('01', '02', '03', '04', '05', '06', '07') and race_horses.`num_三走前着順` > 3 then true
      else false
    end as `cat_訳あり凡走`,
    -- Check if horse placed well in 2 of the last 3 races
    (
      case when race_horses.`num_一走前着順` <= 3 then 1 else 0 end +
      case when race_horses.`num_二走前不利` <= 3 then 1 else 0 end +
      case when race_horses.`num_三走前着順` <= 3 then 1 else 0 end
    ) = 2 as `cat_過去3走中2走好走`,
    -- Determine if a row belongs to the target group
    case
      when
        (
          case when race_horses.`num_一走前着順` <= 3 then 1 else 0 end +
          case when race_horses.`num_二走前不利` <= 3 then 1 else 0 end +
          case when race_horses.`num_三走前着順` <= 3 then 1 else 0 end
        ) = 2
        and (
          race_horses.`num_一走前着順` > 3 and race_horses.`num_一走前不利` >= 1 or
          race_horses.`num_二走前不利` > 3 and race_horses.`num_二走前不利` >= 1 or
          race_horses.`num_三走前着順` > 3 and race_horses.`num_三走前不利` >= 1 or
          race_horses.`cat_3走前休養理由分類コード` IN ('01', '02', '03', '04', '05', '06', '07') and race_horses.`num_三走前着順` > 3
        )
      then true
      else false
    end as `cat_過去3走繋がりあり`

  from
    {{ ref('stg_jrdb__kyi') }} kyi

  inner join
    {{ ref('stg_jrdb__bac') }} bac
  on
    kyi.`レースキー` = bac.`レースキー`

  left join
    {{ ref('stg_jrdb__sed') }} sed
  on
    kyi.`レースキー` = sed.`レースキー`
    and kyi.`馬番` = sed.`馬番`

  left join
    {{ ref('stg_jrdb__tyb') }} tyb
  on
    kyi.`レースキー` = tyb.`レースキー`
    and kyi.`馬番` = tyb.`馬番`

  left join
    {{ ref('int_win_payouts') }} win_payouts
  on
    kyi.`レースキー` = win_payouts.`レースキー`
    and kyi.`馬番` = win_payouts.`馬番`

  left join
    {{ ref('int_place_payouts') }} place_payouts
  on
    kyi.`レースキー` = place_payouts.`レースキー`
    and kyi.`馬番` = place_payouts.`馬番`

  inner join
    {{ ref('int_races') }} races
  on
    kyi.`レースキー` = races.`meta_int_races_レースキー`

  inner join
    {{ ref('int_race_horses') }} race_horses
  on
    kyi.`レースキー` = race_horses.`meta_int_race_horses_レースキー`
    and kyi.`馬番` = race_horses.`meta_int_race_horses_馬番`

  inner join
    {{ ref('int_race_jockeys') }} race_jockeys
  on
    kyi.`レースキー` = race_jockeys.`meta_int_race_jockeys_レースキー`
    and kyi.`馬番` = race_jockeys.`meta_int_race_jockeys_馬番`

  inner join
    {{ ref('int_race_trainers') }} race_trainers
  on
    kyi.`レースキー` = race_trainers.`meta_int_race_trainers_レースキー`
    and kyi.`馬番` = race_trainers.`meta_int_race_trainers_馬番`

  inner join
    {{ ref('int_combinations') }} combinations
  on
    kyi.`レースキー` = combinations.`meta_int_combinations_レースキー`
    and kyi.`馬番` = combinations.`meta_int_combinations_馬番`

  inner join
    {{ ref('int_race_weather') }} race_weather
  on
    kyi.`レースキー` = race_weather.`meta_int_race_weather_レースキー`
  )

select * from final
