with
  final as (
  select
    -- Metadata (do not use for training)
    kyi.`レースキー` as `meta_レースキー`,
    kyi.`馬番` as `meta_馬番`,
    sed.`馬成績_着順` as `meta_着順`,
    sed.`本賞金` as `meta_本賞金`,
    case when sed.`馬成績_着順` = 1 then TRUE else FALSE end as `meta_単勝的中`,
    coalesce(win_payouts.`払戻金`, 0) as `meta_単勝払戻金`,
    case when sed.`馬成績_着順` <= 3 then TRUE else FALSE end as `meta_複勝的中`,
    coalesce(place_payouts.`払戻金`, 0) as `meta_複勝払戻金`,

    -- Features
    race_horses.`num_事前_競争相手平均ＩＤＭ差`,
    race_horses.`num_実績_競争相手平均ＩＤＭ差`


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
    {{ ref('int_race_horses_20240217') }} race_horses
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
