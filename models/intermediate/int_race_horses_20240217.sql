with
  -- Get the latest data for each horse (scd type 2)
  ukc_latest AS (
  select
    *
  from
    {{ ref('stg_jrdb__ukc') }} ukc
  where
    (`血統登録番号`, `データ年月日`) in (select `血統登録番号`, MAX(`データ年月日`) from {{ ref('stg_jrdb__ukc') }} group by `血統登録番号`)
  ),

  race_horses_base as (
  select
    kyi.`レースキー`,
    kyi.`馬番`,
    kyi.`血統登録番号`,
    kyi.`枠番`,
    bac.`発走日時`,
    kyi.`レースキー_場コード` as `場コード`,
    sed.`馬成績_異常区分` as `異常区分`,

    -- General before
    coalesce(tyb.`ＩＤＭ`, kyi.`ＩＤＭ`) as `事前_ＩＤＭ`,
    run_style_codes_kyi.`name` as `事前_脚質`,
    coalesce(tyb.`単勝オッズ`, win_odds.`単勝オッズ`) as `事前_単勝オッズ`,
    coalesce(tyb.`複勝オッズ`, place_odds.`複勝オッズ`) as `事前_複勝オッズ`,
    horse_form_codes_tyb.`name` as `事前_馬体`,
    tyb.`気配コード` as `事前_気配コード`,
    kyi.`上昇度` as `事前_上昇度`,
    kyi.`クラスコード` as `事前_クラスコード`,
    kyi.`展開予想データ_テン指数` as `事前_テン指数`,
    kyi.`展開予想データ_ペース指数` as `事前_ペース指数`,
    kyi.`展開予想データ_上がり指数` as `事前_上がり指数`,

    -- General actual
    sed.`ＪＲＤＢデータ_ＩＤＭ` as `実績_ＩＤＭ`,
    run_style_codes_sed.`name` as `実績_脚質`,
    sed.`馬成績_確定単勝オッズ` as `実績_単勝オッズ`,
    sed.`確定複勝オッズ下` as `実績_複勝オッズ`,
    horse_form_codes_sed.`name` as `実績_馬体`,
    sed.`ＪＲＤＢデータ_気配コード` as `実績_気配コード`,
    sed.`ＪＲＤＢデータ_上昇度コード` as `実績_上昇度`,
    sed.`ＪＲＤＢデータ_クラスコード` as `実績_クラスコード`,
    sed.`ＪＲＤＢデータ_テン指数` as `実績_テン指数`,
    sed.`ＪＲＤＢデータ_ペース指数` as `実績_ペース指数`,
    sed.`ＪＲＤＢデータ_上がり指数` as `実績_上がり指数`,

    -- General common
    coalesce(tyb.`負担重量`, kyi.`負担重量`) as `負担重量`,
    tyb.`馬体重` as `馬体重`,
    sed.`ＪＲＤＢデータ_不利` as `不利`,
    case
      when extract(month from bac.`発走日時`) <= 3 then 1
      when extract(month from bac.`発走日時`) <= 6 then 2
      when extract(month from bac.`発走日時`) <= 9 then 3
      when extract(month from bac.`発走日時`) <= 12 then 4
    end as `四半期`,
    kyi.`入厩年月日`,
    case
      when ukc.`性別コード` = '1' then '牡'
      when ukc.`性別コード` = '2' then '牝'
      else 'セン'
    end `性別`,
    ukc.`生年月日` as `生年月日`,
    sed.`ＪＲＤＢデータ_馬場差` as `馬場差`,
    coalesce(sum(
      case 
        when sed.`ＪＲＤＢデータ_馬場差` <= -10 and sed.`馬成績_着順` <= 3 then 1 
        else 0 
      end
    ) over (
      partition by kyi.`血統登録番号`
      order by bac.`発走日時`
      rows between unbounded preceding and 1 preceding
    ), 0) > 0 `芝瞬発戦好走馬`,
    coalesce(sum(
      case
        when sed.`ＪＲＤＢデータ_馬場差` >= 10 and sed.`馬成績_着順` <= 3 then 1
        else 0
      end
    ) over (
      partition by kyi.`血統登録番号`
      order by bac.`発走日時`
      rows between unbounded preceding and 1 preceding
    ), 0) > 0 `芝消耗戦好走馬`,
    coalesce(sum(
      case 
        when sed.`ＪＲＤＢデータ_馬場差` <= -10 and sed.`馬成績_着順` <= 3 then 1 
        else 0
      end
    ) over (
      partition by kyi.`血統登録番号`
      order by bac.`発走日時`
      rows between unbounded preceding and 1 preceding
    ), 0) > 0 `ダート瞬発戦好走馬`,
    coalesce(sum(
      case
        when sed.`ＪＲＤＢデータ_馬場差` >= 10 and sed.`馬成績_着順` <= 3 then 1
        else 0
      end
    ) over (
      partition by kyi.`血統登録番号`
      order by bac.`発走日時`
      rows between unbounded preceding and 1 preceding
    ), 0) > 0 `ダート消耗戦好走馬`,
    bac.`レース条件_距離` as `距離`,
    sed.`本賞金`,
    coalesce(
      tyb.`馬場状態コード`,
      case
        -- 障害コースの場合は芝馬場状態を使用する
        when bac.`レース条件_トラック情報_芝ダ障害コード` = 'ダート' then kab.`ダ馬場状態コード`
        else kab.`芝馬場状態コード`
      end
    ) as `馬場状態コード`,
    bac.`頭数`,
    bac.`レース条件_トラック情報_芝ダ障害コード` as `トラック種別`,
    sed.`馬成績_着順` as `着順`,
    lag(sed.`馬成績_着順`, 1) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `一走前着順`,
    lag(sed.`馬成績_着順`, 2) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `二走前着順`,
    lag(sed.`馬成績_着順`, 3) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `三走前着順`,
    lag(sed.`馬成績_着順`, 4) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `四走前着順`,
    lag(sed.`馬成績_着順`, 5) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `五走前着順`,
    lag(sed.`馬成績_着順`, 6) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `六走前着順`,
    row_number() over (partition by kyi.`血統登録番号` order by bac.`発走日時`) - 1 as `レース数`,
    coalesce(
      cast(
        sum(
          case
            when sed.`馬成績_着順` = 1 then 1
            else 0
          end
        ) over (partition by kyi.`血統登録番号` order by bac.`発走日時` rows between unbounded preceding and 1 preceding) as integer
      ), 0) as `1位完走`,
    case when sed.`馬成績_着順` = 1 then 1 else 0 end as `単勝的中`,
    case when sed.`馬成績_着順` <= 3 then 1 else 0 end as `複勝的中`,
    coalesce(tyb.`騎手指数`, kyi.`騎手指数`) as `騎手指数`,
    coalesce(tyb.`情報指数`, kyi.`情報指数`) as `情報指数`,
    tyb.`オッズ指数`,
    tyb.`パドック指数`,
    coalesce(tyb.`総合指数`, kyi.`総合指数`) as `総合指数`,
    tyb.`馬具変更情報`,
    tyb.`脚元情報`,
    coalesce(tyb.`見習い区分`, kyi.`見習い区分`) as `見習い区分`,
    tyb.`オッズ印`,
    tyb.`パドック印`,
    tyb.`直前総合印`,
    kyi.`距離適性`,
    kyi.`ローテーション`,
    kyi.`基準オッズ`,
    kyi.`基準人気順位`,
    kyi.`基準複勝オッズ`,
    kyi.`基準複勝人気順位`,
    kyi.`特定情報◎`,
    kyi.`特定情報○`,
    kyi.`特定情報▲`,
    kyi.`特定情報△`,
    kyi.`特定情報×`,
    kyi.`総合情報◎`,
    kyi.`総合情報○`,
    kyi.`総合情報▲`,
    kyi.`総合情報△`,
    kyi.`総合情報×`,
    kyi.`人気指数`,
    kyi.`調教指数`,
    kyi.`厩舎指数`,
    kyi.`調教矢印コード`,
    kyi.`厩舎評価コード`,
    kyi.`騎手期待連対率`,
    kyi.`激走指数`,
    kyi.`蹄コード`,
    kyi.`重適性コード`,
    kyi.`ブリンカー`,
    kyi.`印コード_総合印`,
    kyi.`印コード_ＩＤＭ印`,
    kyi.`印コード_情報印`,
    kyi.`印コード_騎手印`,
    kyi.`印コード_厩舎印`,
    kyi.`印コード_調教印`,
    kyi.`印コード_激走印`,
    kyi.`展開予想データ_位置指数`,
    kyi.`展開予想データ_ペース予想`,
    kyi.`展開予想データ_道中順位`,
    kyi.`展開予想データ_道中差`,
    kyi.`展開予想データ_道中内外`,
    kyi.`展開予想データ_後３Ｆ順位`,
    kyi.`展開予想データ_後３Ｆ差`,
    kyi.`展開予想データ_後３Ｆ内外`,
    kyi.`展開予想データ_ゴール順位`,
    kyi.`展開予想データ_ゴール差`,
    kyi.`展開予想データ_ゴール内外`,
    kyi.`展開予想データ_展開記号`,
    kyi.`激走順位`,
    kyi.`LS指数順位`,
    kyi.`テン指数順位`,
    kyi.`ペース指数順位`,
    kyi.`上がり指数順位`,
    kyi.`位置指数順位`,
    kyi.`騎手期待単勝率`,
    kyi.`騎手期待３着内率`,
    kyi.`輸送区分`,
    kyi.`展開参考データ_馬スタート指数`,
    kyi.`展開参考データ_馬出遅率`,
    kyi.`万券指数`,
    kyi.`万券印`,
    kyi.`激走タイプ`,
    kyi.`休養理由分類コード`,
    kyi.`芝ダ障害フラグ`,
    kyi.`距離フラグ`,
    kyi.`クラスフラグ`,
    kyi.`転厩フラグ`,
    kyi.`去勢フラグ`,
    kyi.`乗替フラグ`,
    kyi.`放牧先ランク`,
    kyi.`厩舎ランク`
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

  -- TYBが公開される前に予測する可能性があるから left join
  left join
    {{ ref('stg_jrdb__tyb') }} tyb
  on
    kyi.`レースキー` = tyb.`レースキー`
    and kyi.`馬番` = tyb.`馬番`

  inner join
    {{ ref('stg_jrdb__kab') }} kab
  on
    kyi.`開催キー` = kab.`開催キー`
    and bac.`年月日` = kab.`年月日`

  inner join
    ukc_latest ukc
  on
    kyi.`血統登録番号` = ukc.`血統登録番号`

  left join
    {{ ref('jrdb__horse_form_codes') }} horse_form_codes_tyb
  on
    tyb.`馬体コード` = horse_form_codes_tyb.`code`

  left join
    {{ ref('jrdb__horse_form_codes') }} horse_form_codes_sed
  on
    sed.`ＪＲＤＢデータ_馬体コード` = horse_form_codes_sed.`code`

  left join
    {{ ref('jrdb__run_style_codes') }} run_style_codes_kyi
  on
    kyi.`脚質` = run_style_codes_kyi.`code`

  left join
    {{ ref('jrdb__run_style_codes') }} run_style_codes_sed
  on
    sed.`レース脚質` = run_style_codes_sed.`code`

  inner join
    {{ ref('int_win_odds') }} win_odds
  on
    kyi.`レースキー` = win_odds.`レースキー`
    and kyi.`馬番` = win_odds.`馬番`

  inner join
    {{ ref('int_place_odds') }} place_odds
  on
    kyi.`レースキー` = place_odds.`レースキー`
    and kyi.`馬番` = place_odds.`馬番`
  ),

  race_horses_streaks as (
  select
    `レースキー`,
    `馬番`,
    cast(sum(`単勝的中`) over (partition by `血統登録番号`, win_group order by `レース数`) as integer) as `連続1着`,
    cast(sum(`複勝的中`) over (partition by `血統登録番号`, place_group order by `レース数`) as integer) as `連続3着内`
  from (
    select
      `レースキー`,
      `馬番`,
      `血統登録番号`,
      `単勝的中`,
      `複勝的中`,
      `レース数`,
      sum(case when `単勝的中` = 0 then 1 else 0 end) over (partition by `血統登録番号` order by `レース数`) as win_group,
      sum(case when `複勝的中` = 0 then 1 else 0 end) over (partition by `血統登録番号` order by `レース数`) as place_group
    from
      race_horses_base
    )
  ),

  -- 参考:
  -- https://github.com/codeworks-data/mvp-horse-racing-prediction/blob/master/extract_features.py#L73
  -- https://medium.com/codeworksparis/horse-racing-prediction-a-machine-learning-approach-part-2-e9f5eb9a92e9
  race_horses as (
  select
    base.`レースキー`,
    base.`馬番`,
    base.`血統登録番号`,
    base.`発走日時`,
    base.`トラック種別`,
    base.`馬場差` as `実績_馬場差`,
    base.`着順` as `実績_着順`,
    base.`本賞金` as `実績_本賞金`,
    base.`異常区分`,

    -- General before race
    base.`事前_ＩＤＭ`,
    base.`事前_脚質`,
    base.`事前_単勝オッズ`,
    base.`事前_複勝オッズ`,
    base.`事前_馬体`,
    base.`事前_気配コード`,
    base.`事前_上昇度`,
    base.`事前_クラスコード`,
    base.`事前_テン指数`,
    base.`事前_ペース指数`,
    base.`事前_上がり指数`,

    -- General actual
    base.`実績_ＩＤＭ`,
    base.`実績_脚質`,
    base.`実績_単勝オッズ`,
    base.`実績_複勝オッズ`,
    base.`実績_馬体`,
    base.`実績_気配コード`,
    base.`実績_上昇度`,
    base.`実績_クラスコード`,
    base.`実績_テン指数`,
    base.`実績_ペース指数`,
    base.`実績_上がり指数`,

    -- General common
    base.`負担重量`,
    base.`馬体重`,
    -- The first race will always be 0
    coalesce(base.`馬体重` - lag(base.`馬体重`) over (partition by base.`血統登録番号` order by base.`発走日時`), 0) as `馬体重増減`,
    base.`性別`,
    case
      when base.`トラック種別` = 'ダート' then base.`ダート瞬発戦好走馬`
      else base.`芝瞬発戦好走馬`
    end `トラック種別瞬発戦好走馬`,
    case
      when base.`トラック種別` = 'ダート' then base.`ダート消耗戦好走馬`
      else base.`芝消耗戦好走馬`
    end `トラック種別消耗戦好走馬`,
    lag(base.`不利`, 1) over (partition by base.`血統登録番号` order by base.`発走日時`) as `一走前不利`,
    lag(base.`不利`, 2) over (partition by base.`血統登録番号` order by base.`発走日時`) as `二走前不利`,
    lag(base.`不利`, 3) over (partition by base.`血統登録番号` order by base.`発走日時`) as `三走前不利`,
    base.`一走前着順`,
    base.`二走前着順`,
    base.`三走前着順`,
    base.`四走前着順`,
    base.`五走前着順`,
    base.`六走前着順`,
    base.`騎手指数`,
    base.`情報指数`,
    base.`オッズ指数`,
    base.`パドック指数`,
    base.`総合指数`,
    base.`馬具変更情報`,
    base.`脚元情報`,
    base.`見習い区分`,
    base.`オッズ印`,
    base.`パドック印`,
    base.`直前総合印`,
    base.`距離適性`,
    base.`ローテーション`,
    base.`基準オッズ`,
    base.`基準人気順位`,
    base.`基準複勝オッズ`,
    base.`基準複勝人気順位`,
    base.`特定情報◎`,
    base.`特定情報○`,
    base.`特定情報▲`,
    base.`特定情報△`,
    base.`特定情報×`,
    base.`総合情報◎`,
    base.`総合情報○`,
    base.`総合情報▲`,
    base.`総合情報△`,
    base.`総合情報×`,
    base.`人気指数`,
    base.`調教指数`,
    base.`厩舎指数`,
    base.`調教矢印コード`,
    base.`厩舎評価コード`,
    base.`騎手期待連対率`,
    base.`激走指数`,
    base.`蹄コード`,
    base.`重適性コード`,
    base.`ブリンカー`,
    base.`印コード_総合印`,
    base.`印コード_ＩＤＭ印`,
    base.`印コード_情報印`,
    base.`印コード_騎手印`,
    base.`印コード_厩舎印`,
    base.`印コード_調教印`,
    base.`印コード_激走印`,
    base.`展開予想データ_位置指数`,
    base.`展開予想データ_ペース予想`,
    base.`展開予想データ_道中順位`,
    base.`展開予想データ_道中差`,
    base.`展開予想データ_道中内外`,
    base.`展開予想データ_後３Ｆ順位`,
    base.`展開予想データ_後３Ｆ差`,
    base.`展開予想データ_後３Ｆ内外`,
    base.`展開予想データ_ゴール順位`,
    base.`展開予想データ_ゴール差`,
    base.`展開予想データ_ゴール内外`,
    base.`展開予想データ_展開記号`,
    base.`激走順位`,
    base.`LS指数順位`,
    base.`テン指数順位`,
    base.`ペース指数順位`,
    base.`上がり指数順位`,
    base.`位置指数順位`,
    base.`騎手期待単勝率`,
    base.`騎手期待３着内率`,
    base.`輸送区分`,
    base.`展開参考データ_馬スタート指数`,
    base.`展開参考データ_馬出遅率`,
    base.`万券指数`,
    base.`万券印`,
    base.`激走タイプ`,
    base.`休養理由分類コード`,
    base.`芝ダ障害フラグ`,
    base.`距離フラグ`,
    base.`クラスフラグ`,
    base.`転厩フラグ`,
    base.`去勢フラグ`,
    base.`乗替フラグ`,
    base.`放牧先ランク`,
    base.`厩舎ランク`,

    -- whether the horse placed in the previous race
    -- last_place
    case
      when lag(`着順`) over (partition by base.`血統登録番号` order by `発走日時`) <= 3 then true
      else false
    end as `前走トップ3`,

    base.`枠番`,

    -- previous race draw
    lag(`枠番`) over (partition by base.`血統登録番号` order by `発走日時`) as `前走枠番`, -- last_draw

    -- days_since_last_race
    date_diff(`発走日時`, lag(`発走日時`) over (partition by base.`血統登録番号` order by `発走日時`)) as `休養日数`,

    -- horse_rest_time
    date_diff(`発走日時`, `入厩年月日`) as `入厩何日前`,

    -- horse_rest_lest14
    date_diff(`発走日時`, `入厩年月日`) < 15 as `入厩15日未満`,

    -- horse_rest_over35
    date_diff(`発走日時`, `入厩年月日`) >= 35 as `入厩35日以上`,

    -- distance
    `距離`,

    -- diff_distance from previous race
    coalesce(`距離` - lag(`距離`) over (partition by base.`血統登録番号` order by `発走日時`), 0) as `前走距離差`,

    -- horse_age in years
    -- extract(year from age(`発走日時`, `生年月日`)) + extract(month from age(`発走日時`, `生年月日`)) / 12 + extract(day from age(`発走日時`, `生年月日`)) / (12 * 30.44) AS `年齢`,
    -- months_between('2024-01-01', '2022-12-31') = 12.0322 / 12 = 1.0027 years
    (months_between(`発走日時`, `生年月日`) / 12) as `年齢`,

    -- horse_age_4 or less
    -- age(`発走日時`, `生年月日`) < '5 years' as `4歳以下`,
    (months_between(`発走日時`, `生年月日`) / 12) < 5 as `4歳以下`,

    cast(sum(
      case
        when (months_between(`発走日時`, `生年月日`) / 12) < 5 then 1
        else 0
      end
    ) over (partition by base.`レースキー`) as integer) as `4歳以下頭数`,

    coalesce(
      sum(
        case
          when (months_between(`発走日時`, `生年月日`) / 12) < 5 then 1
          else 0
        end
      ) over (partition by base.`レースキー`) / cast(`頭数` as double), 0) as `4歳以下割合`,

    `レース数`, -- horse_runs
    `1位完走`, -- horse_wins

    -- how many races this horse has placed in until now (incremented by one on the following race)
    -- horse_placed
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `トップ3完走`,

    -- ratio_win_horse
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `1位完走率`,

    -- ratio_place_horse
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `トップ3完走率`,

    -- Horse Win Percent: Horse’s win percent over the past 5 races.
    -- horse_win_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号` order by `発走日時` rows between 5 preceding and 1 preceding) - 1 as double)'
      )
    }}, 0) as `過去5走勝率`,

    -- horse_place_percent_past_5_races
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between 5 preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号` order by `発走日時` rows between 5 preceding and 1 preceding) - 1 as double)'
      )
    }}, 0) as `過去5走トップ3完走率`,

    -- horse_venue_runs
    coalesce(cast(count(*) over (partition by base.`血統登録番号`, `場コード` order by `発走日時`) - 1 as integer), 0) as `場所レース数`,

    -- horse_venue_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `場所1位完走`,

    -- horse_venue_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `場所トップ3完走`,

    -- ratio_win_horse_venue
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `場コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `場所1位完走率`,

    -- ratio_place_horse_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `場コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `場所トップ3完走率`,

    -- horse_surface_runs
    coalesce(cast(count(*) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時`) - 1 as integer), 0) as `トラック種別レース数`,

    -- horse_surface_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `トラック種別1位完走`,

    -- horse_surface_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `トラック種別トップ3完走`,

    -- ratio_win_horse_surface
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `トラック種別1位完走率`,

    -- ratio_place_horse_surface
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `トラック種別` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `トラック種別トップ3完走率`,

    -- horse_going_runs
    coalesce(cast(count(*) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時`) - 1 as integer), 0) as `馬場状態レース数`,

    -- horse_going_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬場状態1位完走`,

    -- horse_going_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬場状態トップ3完走`,

    -- ratio_win_horse_going
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬場状態1位完走率`,

    -- ratio_place_horse_going
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `馬場状態コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬場状態トップ3完走率`,

    -- horse_distance_runs
    coalesce(cast(count(*) over (partition by base.`血統登録番号`, `距離` order by `発走日時`) - 1 as integer), 0) as `距離レース数`,

    -- horse_distance_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `距離` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `距離1位完走`,

    -- horse_distance_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `距離` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `距離トップ3完走`,

    -- ratio_win_horse_distance
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `距離` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `距離` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `距離1位完走率`,

    -- ratio_place_horse_distance
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `距離` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `距離` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `距離トップ3完走率`,

    -- horse_quarter_runs
    coalesce(cast(count(*) over (partition by base.`血統登録番号`, `四半期` order by `発走日時`) - 1 as integer), 0) as `四半期レース数`,

    -- horse_quarter_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `四半期` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `四半期1位完走`,

    -- horse_quarter_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `四半期` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `四半期トップ3完走`,

    -- ratio_win_horse_quarter
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `四半期` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `四半期` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `四半期1位完走率`,

    -- ratio_place_horse_quarter
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `四半期` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `四半期` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `四半期トップ3完走率`,

    -- Compute the standard rank of the horse on his last 3 races giving us an overview of his state of form
    cast(coalesce(power(`一走前着順` - 1, 2) + power(`二走前着順` - 1, 2) + power(`三走前着順` - 1, 2), 0) as integer) as `過去3走順位平方和`, -- horse_std_rank

    -- prize_horse_cumulative
    coalesce(sum(`本賞金`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding), 0) as `本賞金累計`,

    -- avg_prize_wins_horse
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then `本賞金` else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        '`1位完走`'
      )
    }}, 0) as `1位完走平均賞金`,

    -- avg_prize_runs_horse
    coalesce({{
      dbt_utils.safe_divide(
        'sum(`本賞金`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        '`レース数`'
      )
    }}, 0) as `レース数平均賞金`,

    lag(race_horses_streaks.`連続1着`, 1, 0) over (partition by base.`血統登録番号` order by `発走日時`) as `連続1着`,
    lag(race_horses_streaks.`連続3着内`, 1, 0) over (partition by base.`血統登録番号` order by `発走日時`) as `連続3着内`,

    -- horse win weight diff
    -- assuming that horses that weigh in at close to their average winning body weight have a higher likelihood of winning
    -- https://nycdatascience.com/blog/student-works/can-machine-learning-make-horse-races-a-winning-proposition/
    coalesce(`馬体重` - avg(case when `着順` = 1 then `馬体重` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding), 0) `1着平均馬体重差`,
    -- horse place weight diff
    coalesce(`馬体重` - avg(case when `着順` <= 3 then `馬体重` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding), 0) `3着内平均馬体重差`,

    -- https://nycdatascience.com/blog/student-works/data-analyzing-horse-racing/
    -- (current weight - winning weight) / winning weight
    coalesce(
      (`馬体重` - avg(case when `着順` = 1 then `馬体重` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` = 1 then `馬体重` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `1着馬体重変動率`,

    coalesce(
      (`馬体重` - avg(case when `着順` <= 3 then `馬体重` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` <= 3 then `馬体重` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `3着内馬体重変動率`,

    coalesce(
      (`馬体重` - avg(`馬体重`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(`馬体重`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `馬体重変動率`

  from
    race_horses_base base
  inner join
    race_horses_streaks
  on
    race_horses_streaks.`レースキー` = base.`レースキー`
    and race_horses_streaks.`馬番` = base.`馬番`
  ),

  competitors as (
  select
    a.`レースキー`,
    a.`馬番`,

    -- 事前 ---------------------------------------------------------------------------------------------------------

    -- ＩＤＭ
    max(b.`事前_ＩＤＭ`) as `事前_競争相手最高ＩＤＭ`,
    min(b.`事前_ＩＤＭ`) as `事前_競争相手最低ＩＤＭ`,
    avg(b.`事前_ＩＤＭ`) as `事前_競争相手平均ＩＤＭ`,
    stddev_pop(b.`事前_ＩＤＭ`) as `事前_競争相手ＩＤＭ標準偏差`,

    -- 単勝オッズ
    max(b.`事前_単勝オッズ`) as `事前_競争相手最高単勝オッズ`,
    min(b.`事前_単勝オッズ`) as `事前_競争相手最低単勝オッズ`,
    avg(b.`事前_単勝オッズ`) as `事前_競争相手平均単勝オッズ`,
    stddev_pop(b.`事前_単勝オッズ`) as `事前_競争相手単勝オッズ標準偏差`,

    -- 複勝オッズ
    max(b.`事前_複勝オッズ`) as `事前_競争相手最高複勝オッズ`,
    min(b.`事前_複勝オッズ`) as `事前_競争相手最低複勝オッズ`,
    avg(b.`事前_複勝オッズ`) as `事前_競争相手平均複勝オッズ`,
    stddev_pop(b.`事前_複勝オッズ`) as `事前_競争相手複勝オッズ標準偏差`,

    -- テン指数
    max(b.`事前_テン指数`) as `事前_競争相手最高テン指数`,
    min(b.`事前_テン指数`) as `事前_競争相手最低テン指数`,
    avg(b.`事前_テン指数`) as `事前_競争相手平均テン指数`,
    stddev_pop(b.`事前_テン指数`) as `事前_競争相手テン指数標準偏差`,

    -- ペース指数
    max(b.`事前_ペース指数`) as `事前_競争相手最高ペース指数`,
    min(b.`事前_ペース指数`) as `事前_競争相手最低ペース指数`,
    avg(b.`事前_ペース指数`) as `事前_競争相手平均ペース指数`,
    stddev_pop(b.`事前_ペース指数`) as `事前_競争相手ペース指数標準偏差`,

    -- 上がり指数
    max(b.`事前_上がり指数`) as `事前_競争相手最高上がり指数`,
    min(b.`事前_上がり指数`) as `事前_競争相手最低上がり指数`,
    avg(b.`事前_上がり指数`) as `事前_競争相手平均上がり指数`,
    stddev_pop(b.`事前_上がり指数`) as `事前_競争相手上がり指数標準偏差`,

    -- 実績 ---------------------------------------------------------------------------------------------------------

    -- ＩＤＭ
    max(b.`実績_ＩＤＭ`) as `実績_競争相手最高ＩＤＭ`,
    min(b.`実績_ＩＤＭ`) as `実績_競争相手最低ＩＤＭ`,
    avg(b.`実績_ＩＤＭ`) as `実績_競争相手平均ＩＤＭ`,
    stddev_pop(b.`実績_ＩＤＭ`) as `実績_競争相手ＩＤＭ標準偏差`,

    -- 単勝オッズ
    max(b.`実績_単勝オッズ`) as `実績_競争相手最高単勝オッズ`,
    min(b.`実績_単勝オッズ`) as `実績_競争相手最低単勝オッズ`,
    avg(b.`実績_単勝オッズ`) as `実績_競争相手平均単勝オッズ`,
    stddev_pop(b.`実績_単勝オッズ`) as `実績_競争相手単勝オッズ標準偏差`,

    -- 複勝オッズ
    max(b.`実績_複勝オッズ`) as `実績_競争相手最高複勝オッズ`,
    min(b.`実績_複勝オッズ`) as `実績_競争相手最低複勝オッズ`,
    avg(b.`実績_複勝オッズ`) as `実績_競争相手平均複勝オッズ`,
    stddev_pop(b.`実績_複勝オッズ`) as `実績_競争相手複勝オッズ標準偏差`,

    -- テン指数
    max(b.`実績_テン指数`) as `実績_競争相手最高テン指数`,
    min(b.`実績_テン指数`) as `実績_競争相手最低テン指数`,
    avg(b.`実績_テン指数`) as `実績_競争相手平均テン指数`,
    stddev_pop(b.`実績_テン指数`) as `実績_競争相手テン指数標準偏差`,

    -- ペース指数
    max(b.`実績_ペース指数`) as `実績_競争相手最高ペース指数`,
    min(b.`実績_ペース指数`) as `実績_競争相手最低ペース指数`,
    avg(b.`実績_ペース指数`) as `実績_競争相手平均ペース指数`,
    stddev_pop(b.`実績_ペース指数`) as `実績_競争相手ペース指数標準偏差`,

    -- 上がり指数
    max(b.`実績_上がり指数`) as `実績_競争相手最高上がり指数`,
    min(b.`実績_上がり指数`) as `実績_競争相手最低上がり指数`,
    avg(b.`実績_上がり指数`) as `実績_競争相手平均上がり指数`,
    stddev_pop(b.`実績_上がり指数`) as `実績_競争相手上がり指数標準偏差`,

    -- General ---------------------------------------------------------------------------------------------------------

    -- 1着馬体重変動率
    max(b.`1着馬体重変動率`) as `競争相手最高1着馬体重変動率`,
    min(b.`1着馬体重変動率`) as `競争相手最低1着馬体重変動率`,
    avg(b.`1着馬体重変動率`) as `競争相手平均1着馬体重変動率`,
    stddev_pop(b.`1着馬体重変動率`) as `競争相手1着馬体重変動率標準偏差`,

    -- 3着内馬体重変動率
    max(b.`3着内馬体重変動率`) as `競争相手最高3着内馬体重変動率`,
    min(b.`3着内馬体重変動率`) as `競争相手最低3着内馬体重変動率`,
    avg(b.`3着内馬体重変動率`) as `競争相手平均3着内馬体重変動率`,
    stddev_pop(b.`3着内馬体重変動率`) as `競争相手3着内馬体重変動率標準偏差`,

    -- 馬体重変動率
    max(b.`馬体重変動率`) as `競争相手最高馬体重変動率`,
    min(b.`馬体重変動率`) as `競争相手最低馬体重変動率`,
    avg(b.`馬体重変動率`) as `競争相手平均馬体重変動率`,
    stddev_pop(b.`馬体重変動率`) as `競争相手馬体重変動率標準偏差`,

    -- 1着平均馬体重差
    max(b.`1着平均馬体重差`) as `競争相手最高1着平均馬体重差`,
    min(b.`1着平均馬体重差`) as `競争相手最低1着平均馬体重差`,
    avg(b.`1着平均馬体重差`) as `競争相手平均1着平均馬体重差`,
    stddev_pop(b.`1着平均馬体重差`) as `競争相手1着平均馬体重差標準偏差`,

    -- 3着内平均馬体重差
    max(b.`3着内平均馬体重差`) as `競争相手最高3着内平均馬体重差`,
    min(b.`3着内平均馬体重差`) as `競争相手最低3着内平均馬体重差`,
    avg(b.`3着内平均馬体重差`) as `競争相手平均3着内平均馬体重差`,
    stddev_pop(b.`3着内平均馬体重差`) as `競争相手3着内平均馬体重差標準偏差`,

    -- 負担重量
    max(b.`負担重量`) as `競争相手最高負担重量`,
    min(b.`負担重量`) as `競争相手最低負担重量`,
    avg(b.`負担重量`) as `競争相手平均負担重量`,
    stddev_pop(b.`負担重量`) as `競争相手負担重量標準偏差`,

    -- 馬体重
    max(b.`馬体重`) as `競争相手最高馬体重`,
    min(b.`馬体重`) as `競争相手最低馬体重`,
    avg(b.`馬体重`) as `競争相手平均馬体重`,
    stddev_pop(b.`馬体重`) as `競争相手馬体重標準偏差`,

    -- 馬体重増減
    max(b.`馬体重増減`) as `競争相手最高馬体重増減`,
    min(b.`馬体重増減`) as `競争相手最低馬体重増減`,
    avg(b.`馬体重増減`) as `競争相手平均馬体重増減`,
    stddev_pop(b.`馬体重増減`) as `競争相手馬体重増減標準偏差`,

    -- 性別
    sum(case when b.`性別` = '牡' then 1 else 0 end) / cast(count(*) as double) as `競争相手性別牡割合`,
    sum(case when b.`性別` = '牝' then 1 else 0 end) / cast(count(*) as double) as `競争相手性別牝割合`,
    sum(case when b.`性別` = 'セン' then 1 else 0 end) / cast(count(*) as double) as `競争相手性別セ割合`,

    -- トラック種別瞬発戦好走馬
    sum(case when b.`トラック種別瞬発戦好走馬` then 1 else 0 end) / cast(count(*) as double) as `競争相手トラック種別瞬発戦好走馬割合`,

    -- トラック種別消耗戦好走馬
    sum(case when b.`トラック種別消耗戦好走馬` then 1 else 0 end) / cast(count(*) as double) as `競争相手トラック種別消耗戦好走馬割合`,

    -- 一走前不利
    max(b.`一走前不利`) as `競争相手最高一走前不利`,
    min(b.`一走前不利`) as `競争相手最低一走前不利`,
    avg(b.`一走前不利`) as `競争相手平均一走前不利`,
    stddev_pop(b.`一走前不利`) as `競争相手一走前不利標準偏差`,

    -- 二走前不利
    max(b.`二走前不利`) as `競争相手最高二走前不利`,
    min(b.`二走前不利`) as `競争相手最低二走前不利`,
    avg(b.`二走前不利`) as `競争相手平均二走前不利`,
    stddev_pop(b.`二走前不利`) as `競争相手二走前不利標準偏差`,

    -- 三走前不利
    max(b.`三走前不利`) as `競争相手最高三走前不利`,
    min(b.`三走前不利`) as `競争相手最低三走前不利`,
    avg(b.`三走前不利`) as `競争相手平均三走前不利`,
    stddev_pop(b.`三走前不利`) as `競争相手三走前不利標準偏差`,

    -- 一走前着順
    max(b.`一走前着順`) as `競争相手最高一走前着順`,
    min(b.`一走前着順`) as `競争相手最低一走前着順`,
    avg(b.`一走前着順`) as `競争相手平均一走前着順`,
    stddev_pop(b.`一走前着順`) as `競争相手一走前着順標準偏差`,

    -- 二走前着順
    max(b.`二走前着順`) as `競争相手最高二走前着順`,
    min(b.`二走前着順`) as `競争相手最低二走前着順`,
    avg(b.`二走前着順`) as `競争相手平均二走前着順`,
    stddev_pop(b.`二走前着順`) as `競争相手二走前着順標準偏差`,

    -- 三走前着順
    max(b.`三走前着順`) as `競争相手最高三走前着順`,
    min(b.`三走前着順`) as `競争相手最低三走前着順`,
    avg(b.`三走前着順`) as `競争相手平均三走前着順`,
    stddev_pop(b.`三走前着順`) as `競争相手三走前着順標準偏差`,

    -- 四走前着順
    max(b.`四走前着順`) as `競争相手最高四走前着順`,
    min(b.`四走前着順`) as `競争相手最低四走前着順`,
    avg(b.`四走前着順`) as `競争相手平均四走前着順`,
    stddev_pop(b.`四走前着順`) as `競争相手四走前着順標準偏差`,

    -- 五走前着順
    max(b.`五走前着順`) as `競争相手最高五走前着順`,
    min(b.`五走前着順`) as `競争相手最低五走前着順`,
    avg(b.`五走前着順`) as `競争相手平均五走前着順`,
    stddev_pop(b.`五走前着順`) as `競争相手五走前着順標準偏差`,

    -- 六走前着順
    max(b.`六走前着順`) as `競争相手最高六走前着順`,
    min(b.`六走前着順`) as `競争相手最低六走前着順`,
    avg(b.`六走前着順`) as `競争相手平均六走前着順`,
    stddev_pop(b.`六走前着順`) as `競争相手六走前着順標準偏差`,

    -- 騎手指数
    max(b.`騎手指数`) as `競争相手最高騎手指数`,
    min(b.`騎手指数`) as `競争相手最低騎手指数`,
    avg(b.`騎手指数`) as `競争相手平均騎手指数`,
    stddev_pop(b.`騎手指数`) as `競争相手騎手指数標準偏差`,

    -- 情報指数
    max(b.`情報指数`) as `競争相手最高情報指数`,
    min(b.`情報指数`) as `競争相手最低情報指数`,
    avg(b.`情報指数`) as `競争相手平均情報指数`,
    stddev_pop(b.`情報指数`) as `競争相手情報指数標準偏差`,

    -- オッズ指数
    max(b.`オッズ指数`) as `競争相手最高オッズ指数`,
    min(b.`オッズ指数`) as `競争相手最低オッズ指数`,
    avg(b.`オッズ指数`) as `競争相手平均オッズ指数`,
    stddev_pop(b.`オッズ指数`) as `競争相手オッズ指数標準偏差`,

    -- パドック指数
    max(b.`パドック指数`) as `競争相手最高パドック指数`,
    min(b.`パドック指数`) as `競争相手最低パドック指数`,
    avg(b.`パドック指数`) as `競争相手平均パドック指数`,
    stddev_pop(b.`パドック指数`) as `競争相手パドック指数標準偏差`,

    -- 総合指数
    max(b.`総合指数`) as `競争相手最高総合指数`,
    min(b.`総合指数`) as `競争相手最低総合指数`,
    avg(b.`総合指数`) as `競争相手平均総合指数`,
    stddev_pop(b.`総合指数`) as `競争相手総合指数標準偏差`,

    -- ローテーション
    max(b.`ローテーション`) as `競争相手最高ローテーション`,
    min(b.`ローテーション`) as `競争相手最低ローテーション`,
    avg(b.`ローテーション`) as `競争相手平均ローテーション`,
    stddev_pop(b.`ローテーション`) as `競争相手ローテーション標準偏差`,

    -- 基準オッズ
    max(b.`基準オッズ`) as `競争相手最高基準オッズ`,
    min(b.`基準オッズ`) as `競争相手最低基準オッズ`,
    avg(b.`基準オッズ`) as `競争相手平均基準オッズ`,
    stddev_pop(b.`基準オッズ`) as `競争相手基準オッズ標準偏差`,

    -- 基準複勝オッズ
    max(b.`基準複勝オッズ`) as `競争相手最高基準複勝オッズ`,
    min(b.`基準複勝オッズ`) as `競争相手最低基準複勝オッズ`,
    avg(b.`基準複勝オッズ`) as `競争相手平均基準複勝オッズ`,
    stddev_pop(b.`基準複勝オッズ`) as `競争相手基準複勝オッズ標準偏差`,

    -- 人気指数
    max(b.`人気指数`) as `競争相手最高人気指数`,
    min(b.`人気指数`) as `競争相手最低人気指数`,
    avg(b.`人気指数`) as `競争相手平均人気指数`,
    stddev_pop(b.`人気指数`) as `競争相手人気指数標準偏差`,

    -- 調教指数
    max(b.`調教指数`) as `競争相手最高調教指数`,
    min(b.`調教指数`) as `競争相手最低調教指数`,
    avg(b.`調教指数`) as `競争相手平均調教指数`,
    stddev_pop(b.`調教指数`) as `競争相手調教指数標準偏差`,

    -- 厩舎指数
    max(b.`厩舎指数`) as `競争相手最高厩舎指数`,
    min(b.`厩舎指数`) as `競争相手最低厩舎指数`,
    avg(b.`厩舎指数`) as `競争相手平均厩舎指数`,
    stddev_pop(b.`厩舎指数`) as `競争相手厩舎指数標準偏差`,

    -- 騎手期待連対率
    max(b.`騎手期待連対率`) as `競争相手最高騎手期待連対率`,
    min(b.`騎手期待連対率`) as `競争相手最低騎手期待連対率`,
    avg(b.`騎手期待連対率`) as `競争相手平均騎手期待連対率`,
    stddev_pop(b.`騎手期待連対率`) as `競争相手騎手期待連対率標準偏差`,

    -- 激走指数
    max(b.`激走指数`) as `競争相手最高激走指数`,
    min(b.`激走指数`) as `競争相手最低激走指数`,
    avg(b.`激走指数`) as `競争相手平均激走指数`,
    stddev_pop(b.`激走指数`) as `競争相手激走指数標準偏差`,

    -- 展開予想データ_位置指数
    max(b.`展開予想データ_位置指数`) as `競争相手最高展開予想データ_位置指数`,
    min(b.`展開予想データ_位置指数`) as `競争相手最低展開予想データ_位置指数`,
    avg(b.`展開予想データ_位置指数`) as `競争相手平均展開予想データ_位置指数`,
    stddev_pop(b.`展開予想データ_位置指数`) as `競争相手展開予想データ_位置指数標準偏差`,

    -- 展開予想データ_道中差
    max(b.`展開予想データ_道中差`) as `競争相手最高展開予想データ_道中差`,
    min(b.`展開予想データ_道中差`) as `競争相手最低展開予想データ_道中差`,
    avg(b.`展開予想データ_道中差`) as `競争相手平均展開予想データ_道中差`,
    stddev_pop(b.`展開予想データ_道中差`) as `競争相手展開予想データ_道中差標準偏差`,

    -- 展開予想データ_後３Ｆ差
    max(b.`展開予想データ_後３Ｆ差`) as `競争相手最高展開予想データ_後３Ｆ差`,
    min(b.`展開予想データ_後３Ｆ差`) as `競争相手最低展開予想データ_後３Ｆ差`,
    avg(b.`展開予想データ_後３Ｆ差`) as `競争相手平均展開予想データ_後３Ｆ差`,
    stddev_pop(b.`展開予想データ_後３Ｆ差`) as `競争相手展開予想データ_後３Ｆ差標準偏差`,

    -- 展開予想データ_ゴール差
    max(b.`展開予想データ_ゴール差`) as `競争相手最高展開予想データ_ゴール差`,
    min(b.`展開予想データ_ゴール差`) as `競争相手最低展開予想データ_ゴール差`,
    avg(b.`展開予想データ_ゴール差`) as `競争相手平均展開予想データ_ゴール差`,
    stddev_pop(b.`展開予想データ_ゴール差`) as `競争相手展開予想データ_ゴール差標準偏差`,

    -- 騎手期待単勝率
    max(b.`騎手期待単勝率`) as `競争相手最高騎手期待単勝率`,
    min(b.`騎手期待単勝率`) as `競争相手最低騎手期待単勝率`,
    avg(b.`騎手期待単勝率`) as `競争相手平均騎手期待単勝率`,
    stddev_pop(b.`騎手期待単勝率`) as `競争相手騎手期待単勝率標準偏差`,

    -- 騎手期待３着内率
    max(b.`騎手期待３着内率`) as `競争相手最高騎手期待３着内率`,
    min(b.`騎手期待３着内率`) as `競争相手最低騎手期待３着内率`,
    avg(b.`騎手期待３着内率`) as `競争相手平均騎手期待３着内率`,
    stddev_pop(b.`騎手期待３着内率`) as `競争相手騎手期待３着内率標準偏差`,
  
    -- 展開参考データ_馬スタート指数
    max(b.`展開参考データ_馬スタート指数`) as `競争相手最高展開参考データ_馬スタート指数`,
    min(b.`展開参考データ_馬スタート指数`) as `競争相手最低展開参考データ_馬スタート指数`,
    avg(b.`展開参考データ_馬スタート指数`) as `競争相手平均展開参考データ_馬スタート指数`,
    stddev_pop(b.`展開参考データ_馬スタート指数`) as `競争相手展開参考データ_馬スタート指数標準偏差`,

    -- 展開参考データ_馬出遅率
    max(b.`展開参考データ_馬出遅率`) as `競争相手最高展開参考データ_馬出遅率`,
    min(b.`展開参考データ_馬出遅率`) as `競争相手最低展開参考データ_馬出遅率`,
    avg(b.`展開参考データ_馬出遅率`) as `競争相手平均展開参考データ_馬出遅率`,
    stddev_pop(b.`展開参考データ_馬出遅率`) as `競争相手展開参考データ_馬出遅率標準偏差`,

    -- 万券指数
    max(b.`万券指数`) as `競争相手最高万券指数`,
    min(b.`万券指数`) as `競争相手最低万券指数`,
    avg(b.`万券指数`) as `競争相手平均万券指数`,
    stddev_pop(b.`万券指数`) as `競争相手万券指数標準偏差`,

    -- 前走トップ3
    sum(case when b.`前走トップ3` then 1 else 0 end) / cast(count(*) as double) as `競争相手前走トップ3割合`,

    -- 休養日数
    max(b.`休養日数`) as `競争相手最高休養日数`,
    min(b.`休養日数`) as `競争相手最低休養日数`,
    avg(b.`休養日数`) as `競争相手平均休養日数`,
    stddev_pop(b.`休養日数`) as `競争相手休養日数標準偏差`,

    -- 入厩何日前
    max(b.`入厩何日前`) as `競争相手最高入厩何日前`,
    min(b.`入厩何日前`) as `競争相手最低入厩何日前`,
    avg(b.`入厩何日前`) as `競争相手平均入厩何日前`,
    stddev_pop(b.`入厩何日前`) as `競争相手入厩何日前標準偏差`,

    -- 前走距離差
    max(b.`前走距離差`) as `競争相手最高前走距離差`,
    min(b.`前走距離差`) as `競争相手最低前走距離差`,
    avg(b.`前走距離差`) as `競争相手平均前走距離差`,
    stddev_pop(b.`前走距離差`) as `競争相手前走距離差標準偏差`,

    -- 年齢
    max(b.`年齢`) as `競争相手最高年齢`,
    min(b.`年齢`) as `競争相手最低年齢`,
    avg(b.`年齢`) as `競争相手平均年齢`,
    stddev_pop(b.`年齢`) as `競争相手年齢標準偏差`,

    -- レース数
    max(b.`レース数`) as `競争相手最高レース数`,
    min(b.`レース数`) as `競争相手最低レース数`,
    avg(b.`レース数`) as `競争相手平均レース数`,
    stddev_pop(b.`レース数`) as `競争相手レース数標準偏差`,

    -- 1位完走
    max(b.`1位完走`) as `競争相手最高1位完走`,
    min(b.`1位完走`) as `競争相手最低1位完走`,
    avg(b.`1位完走`) as `競争相手平均1位完走`,
    stddev_pop(b.`1位完走`) as `競争相手1位完走標準偏差`,

    -- トップ3完走
    max(b.`トップ3完走`) as `競争相手最高トップ3完走`,
    min(b.`トップ3完走`) as `競争相手最低トップ3完走`,
    avg(b.`トップ3完走`) as `競争相手平均トップ3完走`,
    stddev_pop(b.`トップ3完走`) as `競争相手トップ3完走標準偏差`,

    -- 1位完走率
    max(b.`1位完走率`) as `競争相手最高1位完走率`,
    min(b.`1位完走率`) as `競争相手最低1位完走率`,
    avg(b.`1位完走率`) as `競争相手平均1位完走率`,
    stddev_pop(b.`1位完走率`) as `競争相手1位完走率標準偏差`,

    -- トップ3完走率
    max(b.`トップ3完走率`) as `競争相手最高トップ3完走率`,
    min(b.`トップ3完走率`) as `競争相手最低トップ3完走率`,
    avg(b.`トップ3完走率`) as `競争相手平均トップ3完走率`,
    stddev_pop(b.`トップ3完走率`) as `競争相手トップ3完走率標準偏差`,

    -- 過去5走勝率
    max(b.`過去5走勝率`) as `競争相手最高過去5走勝率`,
    min(b.`過去5走勝率`) as `競争相手最低過去5走勝率`,
    avg(b.`過去5走勝率`) as `競争相手平均過去5走勝率`,
    stddev_pop(b.`過去5走勝率`) as `競争相手過去5走勝率標準偏差`,

    -- 過去5走トップ3完走率
    max(b.`過去5走トップ3完走率`) as `競争相手最高過去5走トップ3完走率`,
    min(b.`過去5走トップ3完走率`) as `競争相手最低過去5走トップ3完走率`,
    avg(b.`過去5走トップ3完走率`) as `競争相手平均過去5走トップ3完走率`,
    stddev_pop(b.`過去5走トップ3完走率`) as `競争相手過去5走トップ3完走率標準偏差`,

    -- 場所レース数
    max(b.`場所レース数`) as `競争相手最高場所レース数`,
    min(b.`場所レース数`) as `競争相手最低場所レース数`,
    avg(b.`場所レース数`) as `競争相手平均場所レース数`,
    stddev_pop(b.`場所レース数`) as `競争相手場所レース数標準偏差`,

    -- 場所1位完走
    max(b.`場所1位完走`) as `競争相手最高場所1位完走`,
    min(b.`場所1位完走`) as `競争相手最低場所1位完走`,
    avg(b.`場所1位完走`) as `競争相手平均場所1位完走`,
    stddev_pop(b.`場所1位完走`) as `競争相手場所1位完走標準偏差`,

    -- 場所トップ3完走
    max(b.`場所トップ3完走`) as `競争相手最高場所トップ3完走`,
    min(b.`場所トップ3完走`) as `競争相手最低場所トップ3完走`,
    avg(b.`場所トップ3完走`) as `競争相手平均場所トップ3完走`,
    stddev_pop(b.`場所トップ3完走`) as `競争相手場所トップ3完走標準偏差`,

    -- 場所1位完走率
    max(b.`場所1位完走率`) as `競争相手最高場所1位完走率`,
    min(b.`場所1位完走率`) as `競争相手最低場所1位完走率`,
    avg(b.`場所1位完走率`) as `競争相手平均場所1位完走率`,
    stddev_pop(b.`場所1位完走率`) as `競争相手場所1位完走率標準偏差`,

    -- 場所トップ3完走率
    max(b.`場所トップ3完走率`) as `競争相手最高場所トップ3完走率`,
    min(b.`場所トップ3完走率`) as `競争相手最低場所トップ3完走率`,
    avg(b.`場所トップ3完走率`) as `競争相手平均場所トップ3完走率`,
    stddev_pop(b.`場所トップ3完走率`) as `競争相手場所トップ3完走率標準偏差`,

    -- トラック種別レース数
    max(b.`トラック種別レース数`) as `競争相手最高トラック種別レース数`,
    min(b.`トラック種別レース数`) as `競争相手最低トラック種別レース数`,
    avg(b.`トラック種別レース数`) as `競争相手平均トラック種別レース数`,
    stddev_pop(b.`トラック種別レース数`) as `競争相手トラック種別レース数標準偏差`,

    -- トラック種別1位完走
    max(b.`トラック種別1位完走`) as `競争相手最高トラック種別1位完走`,
    min(b.`トラック種別1位完走`) as `競争相手最低トラック種別1位完走`,
    avg(b.`トラック種別1位完走`) as `競争相手平均トラック種別1位完走`,
    stddev_pop(b.`トラック種別1位完走`) as `競争相手トラック種別1位完走標準偏差`,

    -- トラック種別トップ3完走
    max(b.`トラック種別トップ3完走`) as `競争相手最高トラック種別トップ3完走`,
    min(b.`トラック種別トップ3完走`) as `競争相手最低トラック種別トップ3完走`,
    avg(b.`トラック種別トップ3完走`) as `競争相手平均トラック種別トップ3完走`,
    stddev_pop(b.`トラック種別トップ3完走`) as `競争相手トラック種別トップ3完走標準偏差`,

    -- 馬場状態レース数
    max(b.`馬場状態レース数`) as `競争相手最高馬場状態レース数`,
    min(b.`馬場状態レース数`) as `競争相手最低馬場状態レース数`,
    avg(b.`馬場状態レース数`) as `競争相手平均馬場状態レース数`,
    stddev_pop(b.`馬場状態レース数`) as `競争相手馬場状態レース数標準偏差`,

    -- 馬場状態1位完走
    max(b.`馬場状態1位完走`) as `競争相手最高馬場状態1位完走`,
    min(b.`馬場状態1位完走`) as `競争相手最低馬場状態1位完走`,
    avg(b.`馬場状態1位完走`) as `競争相手平均馬場状態1位完走`,
    stddev_pop(b.`馬場状態1位完走`) as `競争相手馬場状態1位完走標準偏差`,

    -- 馬場状態トップ3完走
    max(b.`馬場状態トップ3完走`) as `競争相手最高馬場状態トップ3完走`,
    min(b.`馬場状態トップ3完走`) as `競争相手最低馬場状態トップ3完走`,
    avg(b.`馬場状態トップ3完走`) as `競争相手平均馬場状態トップ3完走`,
    stddev_pop(b.`馬場状態トップ3完走`) as `競争相手馬場状態トップ3完走標準偏差`,

    -- 距離レース数
    max(b.`距離レース数`) as `競争相手最高距離レース数`,
    min(b.`距離レース数`) as `競争相手最低距離レース数`,
    avg(b.`距離レース数`) as `競争相手平均距離レース数`,
    stddev_pop(b.`距離レース数`) as `競争相手距離レース数標準偏差`,

    -- 距離1位完走
    max(b.`距離1位完走`) as `競争相手最高距離1位完走`,
    min(b.`距離1位完走`) as `競争相手最低距離1位完走`,
    avg(b.`距離1位完走`) as `競争相手平均距離1位完走`,
    stddev_pop(b.`距離1位完走`) as `競争相手距離1位完走標準偏差`,

    -- 距離トップ3完走
    max(b.`距離トップ3完走`) as `競争相手最高距離トップ3完走`,
    min(b.`距離トップ3完走`) as `競争相手最低距離トップ3完走`,
    avg(b.`距離トップ3完走`) as `競争相手平均距離トップ3完走`,
    stddev_pop(b.`距離トップ3完走`) as `競争相手距離トップ3完走標準偏差`,

    -- 四半期レース数
    max(b.`四半期レース数`) as `競争相手最高四半期レース数`,
    min(b.`四半期レース数`) as `競争相手最低四半期レース数`,
    avg(b.`四半期レース数`) as `競争相手平均四半期レース数`,
    stddev_pop(b.`四半期レース数`) as `競争相手四半期レース数標準偏差`,

    -- 四半期1位完走
    max(b.`四半期1位完走`) as `競争相手最高四半期1位完走`,
    min(b.`四半期1位完走`) as `競争相手最低四半期1位完走`,
    avg(b.`四半期1位完走`) as `競争相手平均四半期1位完走`,
    stddev_pop(b.`四半期1位完走`) as `競争相手四半期1位完走標準偏差`,

    -- 四半期トップ3完走
    max(b.`四半期トップ3完走`) as `競争相手最高四半期トップ3完走`,
    min(b.`四半期トップ3完走`) as `競争相手最低四半期トップ3完走`,
    avg(b.`四半期トップ3完走`) as `競争相手平均四半期トップ3完走`,
    stddev_pop(b.`四半期トップ3完走`) as `競争相手四半期トップ3完走標準偏差`,

    -- 過去3走順位平方和
    max(b.`過去3走順位平方和`) as `競争相手最高過去3走順位平方和`,
    min(b.`過去3走順位平方和`) as `競争相手最低過去3走順位平方和`,
    avg(b.`過去3走順位平方和`) as `競争相手平均過去3走順位平方和`,
    stddev_pop(b.`過去3走順位平方和`) as `競争相手過去3走順位平方和標準偏差`,

    -- 本賞金累計
    max(b.`本賞金累計`) as `競争相手最高本賞金累計`,
    min(b.`本賞金累計`) as `競争相手最低本賞金累計`,
    avg(b.`本賞金累計`) as `競争相手平均本賞金累計`,
    stddev_pop(b.`本賞金累計`) as `競争相手本賞金累計標準偏差`,

    -- 1位完走平均賞金
    max(b.`1位完走平均賞金`) as `競争相手最高1位完走平均賞金`,
    min(b.`1位完走平均賞金`) as `競争相手最低1位完走平均賞金`,
    avg(b.`1位完走平均賞金`) as `競争相手平均1位完走平均賞金`,
    stddev_pop(b.`1位完走平均賞金`) as `競争相手1位完走平均賞金標準偏差`,

    -- レース数平均賞金
    max(b.`レース数平均賞金`) as `競争相手最高レース数平均賞金`,
    min(b.`レース数平均賞金`) as `競争相手最低レース数平均賞金`,
    avg(b.`レース数平均賞金`) as `競争相手平均レース数平均賞金`,
    stddev_pop(b.`レース数平均賞金`) as `競争相手レース数平均賞金標準偏差`,

    -- 連続1着
    max(b.`連続1着`) as `競争相手最高連続1着`,
    min(b.`連続1着`) as `競争相手最低連続1着`,
    avg(b.`連続1着`) as `競争相手平均連続1着`,
    stddev_pop(b.`連続1着`) as `競争相手連続1着標準偏差`,

    -- 連続3着内
    max(b.`連続3着内`) as `競争相手最高連続3着内`,
    min(b.`連続3着内`) as `競争相手最低連続3着内`,
    avg(b.`連続3着内`) as `競争相手平均連続3着内`,
    stddev_pop(b.`連続3着内`) as `競争相手連続3着内標準偏差`
  from
    race_horses a
  inner join
    race_horses b
  on
    a.`レースキー` = b.`レースキー`
    and a.`馬番` <> b.`馬番`
  group by
    a.`レースキー`,
    a.`馬番`
  ),

  final as (
  select
    -- Metadata (not used for training)
    race_horses.`レースキー` as `meta_int_race_horses_レースキー`,
    race_horses.`馬番` as `meta_int_race_horses_馬番`,
    race_horses.`血統登録番号` as `meta_int_race_horses_血統登録番号`,
    race_horses.`発走日時` as `meta_int_race_horses_発走日時`,
    race_horses.`異常区分` as `meta_int_race_horses_異常区分`,

    -- General known before
    race_horses.`事前_ＩＤＭ` as `num_事前_ＩＤＭ`,
    race_horses.`事前_脚質` as `cat_事前_脚質`,
    race_horses.`事前_単勝オッズ` as `num_事前_単勝オッズ`,
    race_horses.`事前_複勝オッズ` as `num_事前_複勝オッズ`,
    race_horses.`事前_馬体` as `cat_事前_馬体`,
    race_horses.`事前_気配コード` as `cat_事前_気配コード`,
    race_horses.`事前_上昇度` as `cat_事前_上昇度`,
    race_horses.`事前_クラスコード` as `cat_事前_クラスコード`, -- you need to understand what this means more
    race_horses.`事前_テン指数` as `num_事前_テン指数`,
    race_horses.`事前_ペース指数` as `num_事前_ペース指数`,
    race_horses.`事前_上がり指数` as `num_事前_上がり指数`,

    -- General actual
    race_horses.`実績_ＩＤＭ` as `num_実績_ＩＤＭ`,
    race_horses.`実績_脚質` as `cat_実績_脚質`,
    race_horses.`実績_単勝オッズ` as `num_実績_単勝オッズ`,
    race_horses.`実績_複勝オッズ` as `num_実績_複勝オッズ`,
    race_horses.`実績_馬体` as `cat_実績_馬体`,
    race_horses.`実績_気配コード` as `cat_実績_気配コード`,
    race_horses.`実績_上昇度` as `cat_実績_上昇度`,
    race_horses.`実績_クラスコード` as `cat_実績_クラスコード`,
    race_horses.`実績_テン指数` as `num_実績_テン指数`,
    race_horses.`実績_ペース指数` as `num_実績_ペース指数`,
    race_horses.`実績_上がり指数` as `num_実績_上がり指数`,

    -- General common
    race_horses.`負担重量` as `num_負担重量`, -- only 24 or so records with diff between before/actual
    race_horses.`馬体重` as `num_馬体重`,  -- barely any difference between before/actual
    race_horses.`馬体重増減` as `num_馬体重増減`,  -- calculated ourselves
    race_horses.`性別` as `cat_性別`,
    race_horses.`トラック種別瞬発戦好走馬` as `cat_トラック種別瞬発戦好走馬`,
    race_horses.`トラック種別消耗戦好走馬` as `cat_トラック種別消耗戦好走馬`,
    race_horses.`一走前不利` as `num_一走前不利`,
    race_horses.`二走前不利` as `num_二走前不利`,
    race_horses.`三走前不利` as `num_三走前不利`,
    race_horses.`一走前着順` as `num_一走前着順`,
    race_horses.`二走前着順` as `num_二走前着順`,
    race_horses.`三走前着順` as `num_三走前着順`,
    race_horses.`四走前着順` as `num_四走前着順`,
    race_horses.`五走前着順` as `num_五走前着順`,
    race_horses.`六走前着順` as `num_六走前着順`,
    race_horses.`騎手指数` as `num_騎手指数`,
    race_horses.`情報指数` as `num_情報指数`,
    race_horses.`オッズ指数` as `num_オッズ指数`,
    race_horses.`パドック指数` as `num_パドック指数`,
    race_horses.`総合指数` as `num_総合指数`,
    race_horses.`馬具変更情報` as `cat_馬具変更情報`,
    race_horses.`脚元情報` as `cat_脚元情報`,
    race_horses.`見習い区分` as `cat_見習い区分`,
    race_horses.`オッズ印` as `cat_オッズ印`,
    race_horses.`パドック印` as `cat_パドック印`,
    race_horses.`直前総合印` as `cat_直前総合印`,
    race_horses.`距離適性` as `cat_距離適性`,
    race_horses.`ローテーション` as `num_ローテーション`,
    race_horses.`基準オッズ` as `num_基準オッズ`,
    race_horses.`基準人気順位` as `num_基準人気順位`,
    race_horses.`基準複勝オッズ` as `num_基準複勝オッズ`,
    race_horses.`基準複勝人気順位` as `num_基準複勝人気順位`,
    race_horses.`特定情報◎` as `num_特定情報◎`,
    race_horses.`特定情報○` as `num_特定情報○`,
    race_horses.`特定情報▲` as `num_特定情報▲`,
    race_horses.`特定情報△` as `num_特定情報△`,
    race_horses.`特定情報×` as `num_特定情報×`,
    race_horses.`総合情報◎` as `num_総合情報◎`,
    race_horses.`総合情報○` as `num_総合情報○`,
    race_horses.`総合情報▲` as `num_総合情報▲`,
    race_horses.`総合情報△` as `num_総合情報△`,
    race_horses.`総合情報×` as `num_総合情報×`,
    race_horses.`人気指数` as `num_人気指数`,
    race_horses.`調教指数` as `num_調教指数`,
    race_horses.`厩舎指数` as `num_厩舎指数`,
    race_horses.`調教矢印コード` as `cat_調教矢印コード`,
    race_horses.`厩舎評価コード` as `cat_厩舎評価コード`,
    race_horses.`騎手期待連対率` as `num_騎手期待連対率`,
    race_horses.`激走指数` as `num_激走指数`,
    race_horses.`蹄コード` as `cat_蹄コード`,
    race_horses.`重適性コード` as `cat_重適性コード`,
    race_horses.`ブリンカー` as `cat_ブリンカー`,
    race_horses.`印コード_総合印` as `cat_印コード_総合印`,
    race_horses.`印コード_ＩＤＭ印` as `cat_印コード_ＩＤＭ印`,
    race_horses.`印コード_情報印` as `cat_印コード_情報印`,
    race_horses.`印コード_騎手印` as `cat_印コード_騎手印`,
    race_horses.`印コード_厩舎印` as `cat_印コード_厩舎印`,
    race_horses.`印コード_調教印` as `cat_印コード_調教印`,
    race_horses.`印コード_激走印` as `cat_印コード_激走印`,
    race_horses.`展開予想データ_位置指数` as `num_展開予想データ_位置指数`,
    race_horses.`展開予想データ_ペース予想` as `cat_展開予想データ_ペース予想`,
    race_horses.`展開予想データ_道中順位` as `num_展開予想データ_道中順位`,
    race_horses.`展開予想データ_道中差` as `num_展開予想データ_道中差`,
    race_horses.`展開予想データ_道中内外` as `cat_展開予想データ_道中内外`,
    race_horses.`展開予想データ_後３Ｆ順位` as `num_展開予想データ_後３Ｆ順位`,
    race_horses.`展開予想データ_後３Ｆ差` as `num_展開予想データ_後３Ｆ差`,
    race_horses.`展開予想データ_後３Ｆ内外` as `cat_展開予想データ_後３Ｆ内外`,
    race_horses.`展開予想データ_ゴール順位` as `num_展開予想データ_ゴール順位`,
    race_horses.`展開予想データ_ゴール差` as `num_展開予想データ_ゴール差`,
    race_horses.`展開予想データ_ゴール内外` as `cat_展開予想データ_ゴール内外`,
    race_horses.`展開予想データ_展開記号` as `cat_展開予想データ_展開記号`,
    race_horses.`激走順位` as `num_激走順位`,
    race_horses.`LS指数順位` as `num_LS指数順位`,
    race_horses.`テン指数順位` as `num_テン指数順位`,
    race_horses.`ペース指数順位` as `num_ペース指数順位`,
    race_horses.`上がり指数順位` as `num_上がり指数順位`,
    race_horses.`位置指数順位` as `num_位置指数順位`,
    race_horses.`騎手期待単勝率` as `num_騎手期待単勝率`,
    race_horses.`騎手期待３着内率` as `num_騎手期待３着内率`,
    race_horses.`輸送区分` as `cat_輸送区分`,
    race_horses.`展開参考データ_馬スタート指数` as `num_展開参考データ_馬スタート指数`,
    race_horses.`展開参考データ_馬出遅率` as `num_展開参考データ_馬出遅率`,
    race_horses.`万券指数` as `num_万券指数`,
    race_horses.`万券印` as `cat_万券印`,
    race_horses.`激走タイプ` as `cat_激走タイプ`,
    race_horses.`休養理由分類コード` as `cat_休養理由分類コード`,
    race_horses.`芝ダ障害フラグ` as `cat_芝ダ障害フラグ`,
    race_horses.`距離フラグ` as `cat_距離フラグ`,
    race_horses.`クラスフラグ` as `cat_クラスフラグ`,
    race_horses.`転厩フラグ` as `cat_転厩フラグ`,
    race_horses.`去勢フラグ` as `cat_去勢フラグ`,
    race_horses.`乗替フラグ` as `cat_乗替フラグ`,
    race_horses.`放牧先ランク` as `cat_放牧先ランク`,
    race_horses.`厩舎ランク` as `cat_厩舎ランク`,
    race_horses.`前走トップ3` as `cat_前走トップ3`,
    race_horses.`枠番` as `cat_枠番`,
    race_horses.`前走枠番` as `cat_前走枠番`,
    race_horses.`休養日数` as `num_休養日数`,
    race_horses.`入厩何日前` as `num_入厩何日前`, -- horse_rest_time
    race_horses.`入厩15日未満` as `cat_入厩15日未満`, -- horse_rest_lest14
    race_horses.`入厩35日以上` as `cat_入厩35日以上`, -- horse_rest_over35
    race_horses.`前走距離差` as `num_前走距離差`, -- diff_distance
    race_horses.`年齢` as `num_年齢`, -- horse_age (years)
    race_horses.`4歳以下` as `cat_4歳以下`,
    race_horses.`4歳以下頭数` as `num_4歳以下頭数`,
    race_horses.`4歳以下割合` as `num_4歳以下割合`,
    race_horses.`レース数` as `num_レース数`, -- horse_runs
    race_horses.`1位完走` as `num_1位完走`, -- horse_wins
    race_horses.`トップ3完走` as `num_トップ3完走`, -- horse_places
    race_horses.`1位完走率` as `num_1位完走率`,
    race_horses.`トップ3完走率` as `num_トップ3完走率`,
    race_horses.`過去5走勝率` as `num_過去5走勝率`,
    race_horses.`過去5走トップ3完走率` as `num_過去5走トップ3完走率`,
    race_horses.`場所レース数` as `num_場所レース数`, -- horse_venue_runs
    race_horses.`場所1位完走` as `num_場所1位完走`, -- horse_venue_wins
    race_horses.`場所トップ3完走` as `num_場所トップ3完走`, -- horse_venue_places
    race_horses.`場所1位完走率` as `num_場所1位完走率`, -- ratio_win_horse_venue
    race_horses.`場所トップ3完走率` as `num_場所トップ3完走率`, -- ratio_place_horse_venue
    race_horses.`トラック種別レース数` as `num_トラック種別レース数`, -- horse_surface_runs
    race_horses.`トラック種別1位完走` as `num_トラック種別1位完走`, -- horse_surface_wins
    race_horses.`トラック種別トップ3完走` as `num_トラック種別トップ3完走`, -- horse_surface_places
    race_horses.`トラック種別1位完走率` as `num_トラック種別1位完走率`, -- ratio_win_horse_surface
    race_horses.`トラック種別トップ3完走率` as `num_トラック種別トップ3完走率`, -- ratio_place_horse_surface
    race_horses.`馬場状態レース数` as `num_馬場状態レース数`, -- horse_going_runs
    race_horses.`馬場状態1位完走` as `num_馬場状態1位完走`, -- horse_going_wins
    race_horses.`馬場状態トップ3完走` as `num_馬場状態トップ3完走`, -- horse_going_places
    race_horses.`馬場状態1位完走率` as `num_馬場状態1位完走率`, -- ratio_win_horse_going
    race_horses.`馬場状態トップ3完走率` as `num_馬場状態トップ3完走率`, -- ratio_place_horse_going
    race_horses.`距離レース数` as `num_距離レース数`, -- horse_distance_runs
    race_horses.`距離1位完走` as `num_距離1位完走`, -- horse_distance_wins
    race_horses.`距離トップ3完走` as `num_距離トップ3完走`, -- horse_distance_places
    race_horses.`距離1位完走率` as `num_距離1位完走率`, -- ratio_win_horse_distance
    race_horses.`距離トップ3完走率` as `num_距離トップ3完走率`, -- ratio_place_horse_distance
    race_horses.`四半期レース数` as `num_四半期レース数`, -- horse_quarter_runs
    race_horses.`四半期1位完走` as `num_四半期1位完走`, -- horse_quarter_wins
    race_horses.`四半期トップ3完走` as `num_四半期トップ3完走`, -- horse_quarter_places
    race_horses.`四半期1位完走率` as `num_四半期1位完走率`, -- ratio_win_horse_quarter
    race_horses.`四半期トップ3完走率` as `num_四半期トップ3完走率`, -- ratio_place_horse_quarter
    race_horses.`過去3走順位平方和` as `num_過去3走順位平方和`,
    race_horses.`本賞金累計` as `num_本賞金累計`,
    race_horses.`1位完走平均賞金` as `num_1位完走平均賞金`,
    race_horses.`レース数平均賞金` as `num_レース数平均賞金`,
    race_horses.`連続1着` as `num_連続1着`,
    race_horses.`連続3着内` as `num_連続3着内`,
    race_horses.`1着平均馬体重差` as `num_1着平均馬体重差`,
    race_horses.`3着内平均馬体重差` as `num_3着内平均馬体重差`,

    -- a composite weighted winning percentage that also considered the recent number of wins.
    -- horse_win_percent_weighted
    (race_horses.`1位完走` + (race_horses.`1位完走` * 0.5)) / (race_horses.`レース数` + (race_horses.`レース数` * 0.5)) as `重み付き1着完走率`,
    (race_horses.`トップ3完走` + (race_horses.`トップ3完走` * 0.5)) / (race_horses.`レース数` + (race_horses.`レース数` * 0.5)) as `重み付き3着内完走率`,

    -- Competitors before race
    competitors.`事前_競争相手最高ＩＤＭ` as `num_事前_競争相手最高ＩＤＭ`,
    competitors.`事前_競争相手最低ＩＤＭ` as `num_事前_競争相手最低ＩＤＭ`,
    competitors.`事前_競争相手平均ＩＤＭ` as `num_事前_競争相手平均ＩＤＭ`,
    competitors.`事前_競争相手ＩＤＭ標準偏差` as `num_事前_競争相手ＩＤＭ標準偏差`,
    competitors.`事前_競争相手最高単勝オッズ` as `num_事前_競争相手最高単勝オッズ`,
    competitors.`事前_競争相手最低単勝オッズ` as `num_事前_競争相手最低単勝オッズ`,
    competitors.`事前_競争相手平均単勝オッズ` as `num_事前_競争相手平均単勝オッズ`,
    competitors.`事前_競争相手単勝オッズ標準偏差` as `num_事前_競争相手単勝オッズ標準偏差`,
    competitors.`事前_競争相手最高複勝オッズ` as `num_事前_競争相手最高複勝オッズ`,
    competitors.`事前_競争相手最低複勝オッズ` as `num_事前_競争相手最低複勝オッズ`,
    competitors.`事前_競争相手平均複勝オッズ` as `num_事前_競争相手平均複勝オッズ`,
    competitors.`事前_競争相手複勝オッズ標準偏差` as `num_事前_競争相手複勝オッズ標準偏差`,
    competitors.`事前_競争相手最高テン指数` as `num_事前_競争相手最高テン指数`,
    competitors.`事前_競争相手最低テン指数` as `num_事前_競争相手最低テン指数`,
    competitors.`事前_競争相手平均テン指数` as `num_事前_競争相手平均テン指数`,
    competitors.`事前_競争相手テン指数標準偏差` as `num_事前_競争相手テン指数標準偏差`,
    competitors.`事前_競争相手最高ペース指数` as `num_事前_競争相手最高ペース指数`,
    competitors.`事前_競争相手最低ペース指数` as `num_事前_競争相手最低ペース指数`,
    competitors.`事前_競争相手平均ペース指数` as `num_事前_競争相手平均ペース指数`,
    competitors.`事前_競争相手ペース指数標準偏差` as `num_事前_競争相手ペース指数標準偏差`,
    competitors.`事前_競争相手最高上がり指数` as `num_事前_競争相手最高上がり指数`,
    competitors.`事前_競争相手最低上がり指数` as `num_事前_競争相手最低上がり指数`,
    competitors.`事前_競争相手平均上がり指数` as `num_事前_競争相手平均上がり指数`,
    competitors.`事前_競争相手上がり指数標準偏差` as `num_事前_競争相手上がり指数標準偏差`,

    -- Competitors actual
    competitors.`実績_競争相手最高ＩＤＭ` as `num_実績_競争相手最高ＩＤＭ`,
    competitors.`実績_競争相手最低ＩＤＭ` as `num_実績_競争相手最低ＩＤＭ`,
    competitors.`実績_競争相手平均ＩＤＭ` as `num_実績_競争相手平均ＩＤＭ`,
    competitors.`実績_競争相手ＩＤＭ標準偏差` as `num_実績_競争相手ＩＤＭ標準偏差`,
    competitors.`実績_競争相手最高単勝オッズ` as `num_実績_競争相手最高単勝オッズ`,
    competitors.`実績_競争相手最低単勝オッズ` as `num_実績_競争相手最低単勝オッズ`,
    competitors.`実績_競争相手平均単勝オッズ` as `num_実績_競争相手平均単勝オッズ`,
    competitors.`実績_競争相手単勝オッズ標準偏差` as `num_実績_競争相手単勝オッズ標準偏差`,
    competitors.`実績_競争相手最高複勝オッズ` as `num_実績_競争相手最高複勝オッズ`,
    competitors.`実績_競争相手最低複勝オッズ` as `num_実績_競争相手最低複勝オッズ`,
    competitors.`実績_競争相手平均複勝オッズ` as `num_実績_競争相手平均複勝オッズ`,
    competitors.`実績_競争相手複勝オッズ標準偏差` as `num_実績_競争相手複勝オッズ標準偏差`,
    competitors.`実績_競争相手最高テン指数` as `num_実績_競争相手最高テン指数`,
    competitors.`実績_競争相手最低テン指数` as `num_実績_競争相手最低テン指数`,
    competitors.`実績_競争相手平均テン指数` as `num_実績_競争相手平均テン指数`,
    competitors.`実績_競争相手テン指数標準偏差` as `num_実績_競争相手テン指数標準偏差`,
    competitors.`実績_競争相手最高ペース指数` as `num_実績_競争相手最高ペース指数`,
    competitors.`実績_競争相手最低ペース指数` as `num_実績_競争相手最低ペース指数`,
    competitors.`実績_競争相手平均ペース指数` as `num_実績_競争相手平均ペース指数`,
    competitors.`実績_競争相手ペース指数標準偏差` as `num_実績_競争相手ペース指数標準偏差`,
    competitors.`実績_競争相手最高上がり指数` as `num_実績_競争相手最高上がり指数`,
    competitors.`実績_競争相手最低上がり指数` as `num_実績_競争相手最低上がり指数`,
    competitors.`実績_競争相手平均上がり指数` as `num_実績_競争相手平均上がり指数`,
    competitors.`実績_競争相手上がり指数標準偏差` as `num_実績_競争相手上がり指数標準偏差`,

    -- Competitors common
    competitors.`競争相手最高1着馬体重変動率` as `num_競争相手最高1着馬体重変動率`,
    competitors.`競争相手最低1着馬体重変動率` as `num_競争相手最低1着馬体重変動率`,
    competitors.`競争相手平均1着馬体重変動率` as `num_競争相手平均1着馬体重変動率`,
    competitors.`競争相手1着馬体重変動率標準偏差` as `num_競争相手1着馬体重変動率標準偏差`,
    competitors.`競争相手最高3着内馬体重変動率` as `num_競争相手最高3着内馬体重変動率`,
    competitors.`競争相手最低3着内馬体重変動率` as `num_競争相手最低3着内馬体重変動率`,
    competitors.`競争相手平均3着内馬体重変動率` as `num_競争相手平均3着内馬体重変動率`,
    competitors.`競争相手3着内馬体重変動率標準偏差` as `num_競争相手3着内馬体重変動率標準偏差`,
    competitors.`競争相手最高馬体重変動率` as `num_競争相手最高馬体重変動率`,
    competitors.`競争相手最低馬体重変動率` as `num_競争相手最低馬体重変動率`,
    competitors.`競争相手平均馬体重変動率` as `num_競争相手平均馬体重変動率`,
    competitors.`競争相手馬体重変動率標準偏差` as `num_競争相手馬体重変動率標準偏差`,
    competitors.`競争相手最高1着平均馬体重差` as `num_競争相手最高1着平均馬体重差`,
    competitors.`競争相手最低1着平均馬体重差` as `num_競争相手最低1着平均馬体重差`,
    competitors.`競争相手平均1着平均馬体重差` as `num_競争相手平均1着平均馬体重差`,
    competitors.`競争相手1着平均馬体重差標準偏差` as `num_競争相手1着平均馬体重差標準偏差`,
    competitors.`競争相手最高3着内平均馬体重差` as `num_競争相手最高3着内平均馬体重差`,
    competitors.`競争相手最低3着内平均馬体重差` as `num_競争相手最低3着内平均馬体重差`,
    competitors.`競争相手平均3着内平均馬体重差` as `num_競争相手平均3着内平均馬体重差`,
    competitors.`競争相手3着内平均馬体重差標準偏差` as `num_競争相手3着内平均馬体重差標準偏差`,
    competitors.`競争相手最高負担重量` as `num_競争相手最高負担重量`,
    competitors.`競争相手最低負担重量` as `num_競争相手最低負担重量`,
    competitors.`競争相手平均負担重量` as `num_競争相手平均負担重量`,
    competitors.`競争相手負担重量標準偏差` as `num_競争相手負担重量標準偏差`,
    competitors.`競争相手最高馬体重` as `num_競争相手最高馬体重`,
    competitors.`競争相手最低馬体重` as `num_競争相手最低馬体重`,
    competitors.`競争相手平均馬体重` as `num_競争相手平均馬体重`,
    competitors.`競争相手馬体重標準偏差` as `num_競争相手馬体重標準偏差`,
    competitors.`競争相手最高馬体重増減` as `num_競争相手最高馬体重増減`,
    competitors.`競争相手最低馬体重増減` as `num_競争相手最低馬体重増減`,
    competitors.`競争相手平均馬体重増減` as `num_競争相手平均馬体重増減`,
    competitors.`競争相手馬体重増減標準偏差` as `num_競争相手馬体重増減標準偏差`,
    competitors.`競争相手性別牡割合` as `num_競争相手性別牡割合`,
    competitors.`競争相手性別牝割合` as `num_競争相手性別牝割合`,
    competitors.`競争相手性別セ割合` as `num_競争相手性別セ割合`,
    competitors.`競争相手最高一走前不利` as `num_競争相手最高一走前不利`,
    competitors.`競争相手最低一走前不利` as `num_競争相手最低一走前不利`,
    competitors.`競争相手平均一走前不利` as `num_競争相手平均一走前不利`,
    competitors.`競争相手一走前不利標準偏差` as `num_競争相手一走前不利標準偏差`,
    competitors.`競争相手最高二走前不利` as `num_競争相手最高二走前不利`,
    competitors.`競争相手最低二走前不利` as `num_競争相手最低二走前不利`,
    competitors.`競争相手平均二走前不利` as `num_競争相手平均二走前不利`,
    competitors.`競争相手二走前不利標準偏差` as `num_競争相手二走前不利標準偏差`,
    competitors.`競争相手最高三走前不利` as `num_競争相手最高三走前不利`,
    competitors.`競争相手最低三走前不利` as `num_競争相手最低三走前不利`,
    competitors.`競争相手平均三走前不利` as `num_競争相手平均三走前不利`,
    competitors.`競争相手三走前不利標準偏差` as `num_競争相手三走前不利標準偏差`,
    competitors.`競争相手最高一走前着順` as `num_競争相手最高一走前着順`,
    competitors.`競争相手最低一走前着順` as `num_競争相手最低一走前着順`,
    competitors.`競争相手平均一走前着順` as `num_競争相手平均一走前着順`,
    competitors.`競争相手一走前着順標準偏差` as `num_競争相手一走前着順標準偏差`,
    competitors.`競争相手最高二走前着順` as `num_競争相手最高二走前着順`,
    competitors.`競争相手最低二走前着順` as `num_競争相手最低二走前着順`,
    competitors.`競争相手平均二走前着順` as `num_競争相手平均二走前着順`,
    competitors.`競争相手二走前着順標準偏差` as `num_競争相手二走前着順標準偏差`,
    competitors.`競争相手最高三走前着順` as `num_競争相手最高三走前着順`,
    competitors.`競争相手最低三走前着順` as `num_競争相手最低三走前着順`,
    competitors.`競争相手平均三走前着順` as `num_競争相手平均三走前着順`,
    competitors.`競争相手三走前着順標準偏差` as `num_競争相手三走前着順標準偏差`,
    competitors.`競争相手最高四走前着順` as `num_競争相手最高四走前着順`,
    competitors.`競争相手最低四走前着順` as `num_競争相手最低四走前着順`,
    competitors.`競争相手平均四走前着順` as `num_競争相手平均四走前着順`,
    competitors.`競争相手四走前着順標準偏差` as `num_競争相手四走前着順標準偏差`,
    competitors.`競争相手最高五走前着順` as `num_競争相手最高五走前着順`,
    competitors.`競争相手最低五走前着順` as `num_競争相手最低五走前着順`,
    competitors.`競争相手平均五走前着順` as `num_競争相手平均五走前着順`,
    competitors.`競争相手五走前着順標準偏差` as `num_競争相手五走前着順標準偏差`,
    competitors.`競争相手最高六走前着順` as `num_競争相手最高六走前着順`,
    competitors.`競争相手最低六走前着順` as `num_競争相手最低六走前着順`,
    competitors.`競争相手平均六走前着順` as `num_競争相手平均六走前着順`,
    competitors.`競争相手六走前着順標準偏差` as `num_競争相手六走前着順標準偏差`,
    competitors.`競争相手最高騎手指数` as `num_競争相手最高騎手指数`,
    competitors.`競争相手最低騎手指数` as `num_競争相手最低騎手指数`,
    competitors.`競争相手平均騎手指数` as `num_競争相手平均騎手指数`,
    competitors.`競争相手騎手指数標準偏差` as `num_競争相手騎手指数標準偏差`,
    competitors.`競争相手最高情報指数` as `num_競争相手最高情報指数`,
    competitors.`競争相手最低情報指数` as `num_競争相手最低情報指数`,
    competitors.`競争相手平均情報指数` as `num_競争相手平均情報指数`,
    competitors.`競争相手情報指数標準偏差` as `num_競争相手情報指数標準偏差`,
    competitors.`競争相手最高オッズ指数` as `num_競争相手最高オッズ指数`,
    competitors.`競争相手最低オッズ指数` as `num_競争相手最低オッズ指数`,
    competitors.`競争相手平均オッズ指数` as `num_競争相手平均オッズ指数`,
    competitors.`競争相手オッズ指数標準偏差` as `num_競争相手オッズ指数標準偏差`,
    competitors.`競争相手最高パドック指数` as `num_競争相手最高パドック指数`,
    competitors.`競争相手最低パドック指数` as `num_競争相手最低パドック指数`,
    competitors.`競争相手平均パドック指数` as `num_競争相手平均パドック指数`,
    competitors.`競争相手パドック指数標準偏差` as `num_競争相手パドック指数標準偏差`,
    competitors.`競争相手最高総合指数` as `num_競争相手最高総合指数`,
    competitors.`競争相手最低総合指数` as `num_競争相手最低総合指数`,
    competitors.`競争相手平均総合指数` as `num_競争相手平均総合指数`,
    competitors.`競争相手総合指数標準偏差` as `num_競争相手総合指数標準偏差`,
    competitors.`競争相手最高ローテーション` as `num_競争相手最高ローテーション`,
    competitors.`競争相手最低ローテーション` as `num_競争相手最低ローテーション`,
    competitors.`競争相手平均ローテーション` as `num_競争相手平均ローテーション`,
    competitors.`競争相手ローテーション標準偏差` as `num_競争相手ローテーション標準偏差`,
    competitors.`競争相手最高基準オッズ` as `num_競争相手最高基準オッズ`,
    competitors.`競争相手最低基準オッズ` as `num_競争相手最低基準オッズ`,
    competitors.`競争相手平均基準オッズ` as `num_競争相手平均基準オッズ`,
    competitors.`競争相手基準オッズ標準偏差` as `num_競争相手基準オッズ標準偏差`,
    competitors.`競争相手最高基準複勝オッズ` as `num_競争相手最高基準複勝オッズ`,
    competitors.`競争相手最低基準複勝オッズ` as `num_競争相手最低基準複勝オッズ`,
    competitors.`競争相手平均基準複勝オッズ` as `num_競争相手平均基準複勝オッズ`,
    competitors.`競争相手基準複勝オッズ標準偏差` as `num_競争相手基準複勝オッズ標準偏差`,
    competitors.`競争相手最高人気指数` as `num_競争相手最高人気指数`,
    competitors.`競争相手最低人気指数` as `num_競争相手最低人気指数`,
    competitors.`競争相手平均人気指数` as `num_競争相手平均人気指数`,
    competitors.`競争相手人気指数標準偏差` as `num_競争相手人気指数標準偏差`,
    competitors.`競争相手最高調教指数` as `num_競争相手最高調教指数`,
    competitors.`競争相手最低調教指数` as `num_競争相手最低調教指数`,
    competitors.`競争相手平均調教指数` as `num_競争相手平均調教指数`,
    competitors.`競争相手調教指数標準偏差` as `num_競争相手調教指数標準偏差`,
    competitors.`競争相手最高厩舎指数` as `num_競争相手最高厩舎指数`,
    competitors.`競争相手最低厩舎指数` as `num_競争相手最低厩舎指数`,
    competitors.`競争相手平均厩舎指数` as `num_競争相手平均厩舎指数`,
    competitors.`競争相手厩舎指数標準偏差` as `num_競争相手厩舎指数標準偏差`,
    competitors.`競争相手最高騎手期待連対率` as `num_競争相手最高騎手期待連対率`,
    competitors.`競争相手最低騎手期待連対率` as `num_競争相手最低騎手期待連対率`,
    competitors.`競争相手平均騎手期待連対率` as `num_競争相手平均騎手期待連対率`,
    competitors.`競争相手騎手期待連対率標準偏差` as `num_競争相手騎手期待連対率標準偏差`,
    competitors.`競争相手最高激走指数` as `num_競争相手最高激走指数`,
    competitors.`競争相手最低激走指数` as `num_競争相手最低激走指数`,
    competitors.`競争相手平均激走指数` as `num_競争相手平均激走指数`,
    competitors.`競争相手激走指数標準偏差` as `num_競争相手激走指数標準偏差`,
    competitors.`競争相手最高展開予想データ_位置指数` as `num_競争相手最高展開予想データ_位置指数`,
    competitors.`競争相手最低展開予想データ_位置指数` as `num_競争相手最低展開予想データ_位置指数`,
    competitors.`競争相手平均展開予想データ_位置指数` as `num_競争相手平均展開予想データ_位置指数`,
    competitors.`競争相手展開予想データ_位置指数標準偏差` as `num_競争相手展開予想データ_位置指数標準偏差`,
    competitors.`競争相手最高展開予想データ_道中差` as `num_競争相手最高展開予想データ_道中差`,
    competitors.`競争相手最低展開予想データ_道中差` as `num_競争相手最低展開予想データ_道中差`,
    competitors.`競争相手平均展開予想データ_道中差` as `num_競争相手平均展開予想データ_道中差`,
    competitors.`競争相手展開予想データ_道中差標準偏差` as `num_競争相手展開予想データ_道中差標準偏差`,
    competitors.`競争相手最高展開予想データ_後３Ｆ差` as `num_競争相手最高展開予想データ_後３Ｆ差`,
    competitors.`競争相手最低展開予想データ_後３Ｆ差` as `num_競争相手最低展開予想データ_後３Ｆ差`,
    competitors.`競争相手平均展開予想データ_後３Ｆ差` as `num_競争相手平均展開予想データ_後３Ｆ差`,
    competitors.`競争相手展開予想データ_後３Ｆ差標準偏差` as `num_競争相手展開予想データ_後３Ｆ差標準偏差`,
    competitors.`競争相手最高展開予想データ_ゴール差` as `num_競争相手最高展開予想データ_ゴール差`,
    competitors.`競争相手最低展開予想データ_ゴール差` as `num_競争相手最低展開予想データ_ゴール差`,
    competitors.`競争相手平均展開予想データ_ゴール差` as `num_競争相手平均展開予想データ_ゴール差`,
    competitors.`競争相手展開予想データ_ゴール差標準偏差` as `num_競争相手展開予想データ_ゴール差標準偏差`,
    competitors.`競争相手最高騎手期待単勝率` as `num_競争相手最高騎手期待単勝率`,
    competitors.`競争相手最低騎手期待単勝率` as `num_競争相手最低騎手期待単勝率`,
    competitors.`競争相手平均騎手期待単勝率` as `num_競争相手平均騎手期待単勝率`,
    competitors.`競争相手騎手期待単勝率標準偏差` as `num_競争相手騎手期待単勝率標準偏差`,
    competitors.`競争相手最高騎手期待３着内率` as `num_競争相手最高騎手期待３着内率`,
    competitors.`競争相手最低騎手期待３着内率` as `num_競争相手最低騎手期待３着内率`,
    competitors.`競争相手平均騎手期待３着内率` as `num_競争相手平均騎手期待３着内率`,
    competitors.`競争相手騎手期待３着内率標準偏差` as `num_競争相手騎手期待３着内率標準偏差`,
    competitors.`競争相手最高展開参考データ_馬スタート指数` as `num_競争相手最高展開参考データ_馬スタート指数`,
    competitors.`競争相手最低展開参考データ_馬スタート指数` as `num_競争相手最低展開参考データ_馬スタート指数`,
    competitors.`競争相手平均展開参考データ_馬スタート指数` as `num_競争相手平均展開参考データ_馬スタート指数`,
    competitors.`競争相手展開参考データ_馬スタート指数標準偏差` as `num_競争相手展開参考データ_馬スタート指数標準偏差`,
    competitors.`競争相手最高展開参考データ_馬出遅率` as `num_競争相手最高展開参考データ_馬出遅率`,
    competitors.`競争相手最低展開参考データ_馬出遅率` as `num_競争相手最低展開参考データ_馬出遅率`,
    competitors.`競争相手平均展開参考データ_馬出遅率` as `num_競争相手平均展開参考データ_馬出遅率`,
    competitors.`競争相手展開参考データ_馬出遅率標準偏差` as `num_競争相手展開参考データ_馬出遅率標準偏差`,
    competitors.`競争相手最高万券指数` as `num_競争相手最高万券指数`,
    competitors.`競争相手最低万券指数` as `num_競争相手最低万券指数`,
    competitors.`競争相手平均万券指数` as `num_競争相手平均万券指数`,
    competitors.`競争相手万券指数標準偏差` as `num_競争相手万券指数標準偏差`,
    competitors.`競争相手前走トップ3割合` as `num_競争相手前走トップ3割合`,
    competitors.`競争相手最高休養日数` as `num_競争相手最高休養日数`,
    competitors.`競争相手最低休養日数` as `num_競争相手最低休養日数`,
    competitors.`競争相手平均休養日数` as `num_競争相手平均休養日数`,
    competitors.`競争相手休養日数標準偏差` as `num_競争相手休養日数標準偏差`,
    competitors.`競争相手最高入厩何日前` as `num_競争相手最高入厩何日前`,
    competitors.`競争相手最低入厩何日前` as `num_競争相手最低入厩何日前`,
    competitors.`競争相手平均入厩何日前` as `num_競争相手平均入厩何日前`,
    competitors.`競争相手入厩何日前標準偏差` as `num_競争相手入厩何日前標準偏差`,
    competitors.`競争相手最高年齢` as `num_競争相手最高年齢`,
    competitors.`競争相手最低年齢` as `num_競争相手最低年齢`,
    competitors.`競争相手平均年齢` as `num_競争相手平均年齢`,
    competitors.`競争相手年齢標準偏差` as `num_競争相手年齢標準偏差`,
    competitors.`競争相手最高レース数` as `num_競争相手最高レース数`,
    competitors.`競争相手最低レース数` as `num_競争相手最低レース数`,
    competitors.`競争相手平均レース数` as `num_競争相手平均レース数`,
    competitors.`競争相手レース数標準偏差` as `num_競争相手レース数標準偏差`,
    competitors.`競争相手最高1位完走` as `num_競争相手最高1位完走`,
    competitors.`競争相手最低1位完走` as `num_競争相手最低1位完走`,
    competitors.`競争相手平均1位完走` as `num_競争相手平均1位完走`,
    competitors.`競争相手1位完走標準偏差` as `num_競争相手1位完走標準偏差`,
    competitors.`競争相手最高トップ3完走` as `num_競争相手最高トップ3完走`,
    competitors.`競争相手最低トップ3完走` as `num_競争相手最低トップ3完走`,
    competitors.`競争相手平均トップ3完走` as `num_競争相手平均トップ3完走`,
    competitors.`競争相手トップ3完走標準偏差` as `num_競争相手トップ3完走標準偏差`,
    competitors.`競争相手最高1位完走率` as `num_競争相手最高1位完走率`,
    competitors.`競争相手最低1位完走率` as `num_競争相手最低1位完走率`,
    competitors.`競争相手平均1位完走率` as `num_競争相手平均1位完走率`,
    competitors.`競争相手1位完走率標準偏差` as `num_競争相手1位完走率標準偏差`,
    competitors.`競争相手最高トップ3完走率` as `num_競争相手最高トップ3完走率`,
    competitors.`競争相手最低トップ3完走率` as `num_競争相手最低トップ3完走率`,
    competitors.`競争相手平均トップ3完走率` as `num_競争相手平均トップ3完走率`,
    competitors.`競争相手トップ3完走率標準偏差` as `num_競争相手トップ3完走率標準偏差`,
    competitors.`競争相手最高過去5走勝率` as `num_競争相手最高過去5走勝率`,
    competitors.`競争相手最低過去5走勝率` as `num_競争相手最低過去5走勝率`,
    competitors.`競争相手平均過去5走勝率` as `num_競争相手平均過去5走勝率`,
    competitors.`競争相手過去5走勝率標準偏差` as `num_競争相手過去5走勝率標準偏差`,
    competitors.`競争相手最高過去5走トップ3完走率` as `num_競争相手最高過去5走トップ3完走率`,
    competitors.`競争相手最低過去5走トップ3完走率` as `num_競争相手最低過去5走トップ3完走率`,
    competitors.`競争相手平均過去5走トップ3完走率` as `num_競争相手平均過去5走トップ3完走率`,
    competitors.`競争相手過去5走トップ3完走率標準偏差` as `num_競争相手過去5走トップ3完走率標準偏差`,
    competitors.`競争相手最高場所レース数` as `num_競争相手最高場所レース数`,
    competitors.`競争相手最低場所レース数` as `num_競争相手最低場所レース数`,
    competitors.`競争相手平均場所レース数` as `num_競争相手平均場所レース数`,
    competitors.`競争相手場所レース数標準偏差` as `num_競争相手場所レース数標準偏差`,
    competitors.`競争相手最高場所1位完走` as `num_競争相手最高場所1位完走`,
    competitors.`競争相手最低場所1位完走` as `num_競争相手最低場所1位完走`,
    competitors.`競争相手平均場所1位完走` as `num_競争相手平均場所1位完走`,
    competitors.`競争相手場所1位完走標準偏差` as `num_競争相手場所1位完走標準偏差`,
    competitors.`競争相手最高場所トップ3完走` as `num_競争相手最高場所トップ3完走`,
    competitors.`競争相手最低場所トップ3完走` as `num_競争相手最低場所トップ3完走`,
    competitors.`競争相手平均場所トップ3完走` as `num_競争相手平均場所トップ3完走`,
    competitors.`競争相手場所トップ3完走標準偏差` as `num_競争相手場所トップ3完走標準偏差`,
    competitors.`競争相手最高場所1位完走率` as `num_競争相手最高場所1位完走率`,
    competitors.`競争相手最低場所1位完走率` as `num_競争相手最低場所1位完走率`,
    competitors.`競争相手平均場所1位完走率` as `num_競争相手平均場所1位完走率`,
    competitors.`競争相手場所1位完走率標準偏差` as `num_競争相手場所1位完走率標準偏差`,
    competitors.`競争相手最高場所トップ3完走率` as `num_競争相手最高場所トップ3完走率`,
    competitors.`競争相手最低場所トップ3完走率` as `num_競争相手最低場所トップ3完走率`,
    competitors.`競争相手平均場所トップ3完走率` as `num_競争相手平均場所トップ3完走率`,
    competitors.`競争相手場所トップ3完走率標準偏差` as `num_競争相手場所トップ3完走率標準偏差`,
    competitors.`競争相手最高トラック種別レース数` as `num_競争相手最高トラック種別レース数`,
    competitors.`競争相手最低トラック種別レース数` as `num_競争相手最低トラック種別レース数`,
    competitors.`競争相手平均トラック種別レース数` as `num_競争相手平均トラック種別レース数`,
    competitors.`競争相手トラック種別レース数標準偏差` as `num_競争相手トラック種別レース数標準偏差`,
    competitors.`競争相手最高トラック種別1位完走` as `num_競争相手最高トラック種別1位完走`,
    competitors.`競争相手最低トラック種別1位完走` as `num_競争相手最低トラック種別1位完走`,
    competitors.`競争相手平均トラック種別1位完走` as `num_競争相手平均トラック種別1位完走`,
    competitors.`競争相手トラック種別1位完走標準偏差` as `num_競争相手トラック種別1位完走標準偏差`,
    competitors.`競争相手最高トラック種別トップ3完走` as `num_競争相手最高トラック種別トップ3完走`,
    competitors.`競争相手最低トラック種別トップ3完走` as `num_競争相手最低トラック種別トップ3完走`,
    competitors.`競争相手平均トラック種別トップ3完走` as `num_競争相手平均トラック種別トップ3完走`,
    competitors.`競争相手トラック種別トップ3完走標準偏差` as `num_競争相手トラック種別トップ3完走標準偏差`,
    competitors.`競争相手最高馬場状態レース数` as `num_競争相手最高馬場状態レース数`,
    competitors.`競争相手最低馬場状態レース数` as `num_競争相手最低馬場状態レース数`,
    competitors.`競争相手平均馬場状態レース数` as `num_競争相手平均馬場状態レース数`,
    competitors.`競争相手馬場状態レース数標準偏差` as `num_競争相手馬場状態レース数標準偏差`,
    competitors.`競争相手最高馬場状態1位完走` as `num_競争相手最高馬場状態1位完走`,
    competitors.`競争相手最低馬場状態1位完走` as `num_競争相手最低馬場状態1位完走`,
    competitors.`競争相手平均馬場状態1位完走` as `num_競争相手平均馬場状態1位完走`,
    competitors.`競争相手馬場状態1位完走標準偏差` as `num_競争相手馬場状態1位完走標準偏差`,
    competitors.`競争相手最高馬場状態トップ3完走` as `num_競争相手最高馬場状態トップ3完走`,
    competitors.`競争相手最低馬場状態トップ3完走` as `num_競争相手最低馬場状態トップ3完走`,
    competitors.`競争相手平均馬場状態トップ3完走` as `num_競争相手平均馬場状態トップ3完走`,
    competitors.`競争相手馬場状態トップ3完走標準偏差` as `num_競争相手馬場状態トップ3完走標準偏差`,
    competitors.`競争相手最高距離レース数` as `num_競争相手最高距離レース数`,
    competitors.`競争相手最低距離レース数` as `num_競争相手最低距離レース数`,
    competitors.`競争相手平均距離レース数` as `num_競争相手平均距離レース数`,
    competitors.`競争相手距離レース数標準偏差` as `num_競争相手距離レース数標準偏差`,
    competitors.`競争相手最高距離1位完走` as `num_競争相手最高距離1位完走`,
    competitors.`競争相手最低距離1位完走` as `num_競争相手最低距離1位完走`,
    competitors.`競争相手平均距離1位完走` as `num_競争相手平均距離1位完走`,
    competitors.`競争相手距離1位完走標準偏差` as `num_競争相手距離1位完走標準偏差`,
    competitors.`競争相手最高距離トップ3完走` as `num_競争相手最高距離トップ3完走`,
    competitors.`競争相手最低距離トップ3完走` as `num_競争相手最低距離トップ3完走`,
    competitors.`競争相手平均距離トップ3完走` as `num_競争相手平均距離トップ3完走`,
    competitors.`競争相手距離トップ3完走標準偏差` as `num_競争相手距離トップ3完走標準偏差`,
    competitors.`競争相手最高四半期レース数` as `num_競争相手最高四半期レース数`,
    competitors.`競争相手最低四半期レース数` as `num_競争相手最低四半期レース数`,
    competitors.`競争相手平均四半期レース数` as `num_競争相手平均四半期レース数`,
    competitors.`競争相手四半期レース数標準偏差` as `num_競争相手四半期レース数標準偏差`,
    competitors.`競争相手最高四半期1位完走` as `num_競争相手最高四半期1位完走`,
    competitors.`競争相手最低四半期1位完走` as `num_競争相手最低四半期1位完走`,
    competitors.`競争相手平均四半期1位完走` as `num_競争相手平均四半期1位完走`,
    competitors.`競争相手四半期1位完走標準偏差` as `num_競争相手四半期1位完走標準偏差`,
    competitors.`競争相手最高四半期トップ3完走` as `num_競争相手最高四半期トップ3完走`,
    competitors.`競争相手最低四半期トップ3完走` as `num_競争相手最低四半期トップ3完走`,
    competitors.`競争相手平均四半期トップ3完走` as `num_競争相手平均四半期トップ3完走`,
    competitors.`競争相手四半期トップ3完走標準偏差` as `num_競争相手四半期トップ3完走標準偏差`,
    competitors.`競争相手最高過去3走順位平方和` as `num_競争相手最高過去3走順位平方和`,
    competitors.`競争相手最低過去3走順位平方和` as `num_競争相手最低過去3走順位平方和`,
    competitors.`競争相手平均過去3走順位平方和` as `num_競争相手平均過去3走順位平方和`,
    competitors.`競争相手過去3走順位平方和標準偏差` as `num_競争相手過去3走順位平方和標準偏差`,
    competitors.`競争相手最高本賞金累計` as `num_競争相手最高本賞金累計`,
    competitors.`競争相手最低本賞金累計` as `num_競争相手最低本賞金累計`,
    competitors.`競争相手平均本賞金累計` as `num_競争相手平均本賞金累計`,
    competitors.`競争相手本賞金累計標準偏差` as `num_競争相手本賞金累計標準偏差`,
    competitors.`競争相手最高1位完走平均賞金` as `num_競争相手最高1位完走平均賞金`,
    competitors.`競争相手最低1位完走平均賞金` as `num_競争相手最低1位完走平均賞金`,
    competitors.`競争相手平均1位完走平均賞金` as `num_競争相手平均1位完走平均賞金`,
    competitors.`競争相手1位完走平均賞金標準偏差` as `num_競争相手1位完走平均賞金標準偏差`,
    competitors.`競争相手最高レース数平均賞金` as `num_競争相手最高レース数平均賞金`,
    competitors.`競争相手最低レース数平均賞金` as `num_競争相手最低レース数平均賞金`,
    competitors.`競争相手平均レース数平均賞金` as `num_競争相手平均レース数平均賞金`,
    competitors.`競争相手レース数平均賞金標準偏差` as `num_競争相手レース数平均賞金標準偏差`,
    competitors.`競争相手トラック種別瞬発戦好走馬割合` as `num_競争相手トラック種別瞬発戦好走馬割合`,
    competitors.`競争相手トラック種別消耗戦好走馬割合` as `num_競争相手トラック種別消耗戦好走馬割合`,
    competitors.`競争相手最高連続1着` as `num_競争相手最高連続1着`,
    competitors.`競争相手最低連続1着` as `num_競争相手最低連続1着`,
    competitors.`競争相手平均連続1着` as `num_競争相手平均連続1着`,
    competitors.`競争相手連続1着標準偏差` as `num_競争相手連続1着標準偏差`,
    competitors.`競争相手最高連続3着内` as `num_競争相手最高連続3着内`,
    competitors.`競争相手最低連続3着内` as `num_競争相手最低連続3着内`,
    competitors.`競争相手平均連続3着内` as `num_競争相手平均連続3着内`,
    competitors.`競争相手連続3着内標準偏差` as `num_競争相手連続3着内標準偏差`,

    -- Relative before race
    race_horses.`事前_ＩＤＭ` - competitors.`事前_競争相手平均ＩＤＭ` as `num_事前_競争相手平均ＩＤＭ差`,
    race_horses.`事前_単勝オッズ` - competitors.`事前_競争相手平均単勝オッズ` as `num_事前_競争相手平均単勝オッズ差`,
    race_horses.`事前_複勝オッズ` - competitors.`事前_競争相手平均複勝オッズ` as `num_事前_競争相手平均複勝オッズ差`,
    race_horses.`事前_テン指数` - competitors.`事前_競争相手平均テン指数` as `num_事前_競争相手平均テン指数差`,
    race_horses.`事前_ペース指数` - competitors.`事前_競争相手平均ペース指数` as `num_事前_競争相手平均ペース指数差`,
    race_horses.`事前_上がり指数` - competitors.`事前_競争相手平均上がり指数` as `num_事前_競争相手平均上がり指数差`,

    -- Relative actual
    race_horses.`実績_ＩＤＭ` - competitors.`実績_競争相手平均ＩＤＭ` as `num_実績_競争相手平均ＩＤＭ差`,
    race_horses.`実績_単勝オッズ` - competitors.`実績_競争相手平均単勝オッズ` as `num_実績_競争相手平均単勝オッズ差`,
    race_horses.`実績_複勝オッズ` - competitors.`実績_競争相手平均複勝オッズ` as `num_実績_競争相手平均複勝オッズ差`,
    race_horses.`実績_テン指数` - competitors.`実績_競争相手平均テン指数` as `num_実績_競争相手平均テン指数差`,
    race_horses.`実績_ペース指数` - competitors.`実績_競争相手平均ペース指数` as `num_実績_競争相手平均ペース指数差`,
    race_horses.`実績_上がり指数` - competitors.`実績_競争相手平均上がり指数` as `num_実績_競争相手平均上がり指数差`,

    -- Relative common
    race_horses.`1着馬体重変動率` - competitors.`競争相手平均1着馬体重変動率` as `num_競争相手平均1着馬体重変動率差`,
    race_horses.`3着内馬体重変動率` - competitors.`競争相手平均3着内馬体重変動率` as `num_競争相手平均3着内馬体重変動率差`,
    race_horses.`馬体重変動率` - competitors.`競争相手平均馬体重変動率` as `num_競争相手平均馬体重変動率差`,
    race_horses.`1着平均馬体重差` - competitors.`競争相手平均1着平均馬体重差` as `num_競争相手平均1着平均馬体重差差`,
    race_horses.`3着内平均馬体重差` - competitors.`競争相手平均3着内平均馬体重差` as `num_競争相手平均3着内平均馬体重差差`,
    race_horses.`負担重量` - competitors.`競争相手平均負担重量` as `num_競争相手平均負担重量差`,
    race_horses.`馬体重` - competitors.`競争相手平均馬体重` as `num_競争相手平均馬体重差`,
    race_horses.`馬体重増減` - competitors.`競争相手平均馬体重増減` as `num_競争相手平均馬体重増減差`,
    race_horses.`一走前不利` - competitors.`競争相手平均一走前不利` as `num_競争相手平均一走前不利差`,
    race_horses.`二走前不利` - competitors.`競争相手平均二走前不利` as `num_競争相手平均二走前不利差`,
    race_horses.`三走前不利` - competitors.`競争相手平均三走前不利` as `num_競争相手平均三走前不利差`,
    race_horses.`一走前着順` - competitors.`競争相手平均一走前着順` as `num_競争相手平均一走前着順差`,
    race_horses.`二走前着順` - competitors.`競争相手平均二走前着順` as `num_競争相手平均二走前着順差`,
    race_horses.`三走前着順` - competitors.`競争相手平均三走前着順` as `num_競争相手平均三走前着順差`,
    race_horses.`四走前着順` - competitors.`競争相手平均四走前着順` as `num_競争相手平均四走前着順差`,
    race_horses.`五走前着順` - competitors.`競争相手平均五走前着順` as `num_競争相手平均五走前着順差`,
    race_horses.`六走前着順` - competitors.`競争相手平均六走前着順` as `num_競争相手平均六走前着順差`,
    race_horses.`騎手指数` - competitors.`競争相手平均騎手指数` as `num_競争相手平均騎手指数差`,
    race_horses.`情報指数` - competitors.`競争相手平均情報指数` as `num_競争相手平均情報指数差`,
    race_horses.`オッズ指数` - competitors.`競争相手平均オッズ指数` as `num_競争相手平均オッズ指数差`,
    race_horses.`パドック指数` - competitors.`競争相手平均パドック指数` as `num_競争相手平均パドック指数差`,
    race_horses.`総合指数` - competitors.`競争相手平均総合指数` as `num_競争相手平均総合指数差`,
    race_horses.`ローテーション` - competitors.`競争相手平均ローテーション` as `num_競争相手平均ローテーション差`,
    race_horses.`基準オッズ` - competitors.`競争相手平均基準オッズ` as `num_競争相手平均基準オッズ差`,
    race_horses.`基準複勝オッズ` - competitors.`競争相手平均基準複勝オッズ` as `num_競争相手平均基準複勝オッズ差`,
    race_horses.`人気指数` - competitors.`競争相手平均人気指数` as `num_競争相手平均人気指数差`,
    race_horses.`調教指数` - competitors.`競争相手平均調教指数` as `num_競争相手平均調教指数差`,
    race_horses.`厩舎指数` - competitors.`競争相手平均厩舎指数` as `num_競争相手平均厩舎指数差`,
    race_horses.`騎手期待連対率` - competitors.`競争相手平均騎手期待連対率` as `num_競争相手平均騎手期待連対率差`,
    race_horses.`激走指数` - competitors.`競争相手平均激走指数` as `num_競争相手平均激走指数差`,
    race_horses.`展開予想データ_位置指数` - competitors.`競争相手平均展開予想データ_位置指数` as `num_競争相手平均展開予想データ_位置指数差`,
    race_horses.`展開予想データ_道中差` - competitors.`競争相手平均展開予想データ_道中差` as `num_競争相手平均展開予想データ_道中差差`,
    race_horses.`展開予想データ_後３Ｆ差` - competitors.`競争相手平均展開予想データ_後３Ｆ差` as `num_競争相手平均展開予想データ_後３Ｆ差差`,
    race_horses.`展開予想データ_ゴール差` - competitors.`競争相手平均展開予想データ_ゴール差` as `num_競争相手平均展開予想データ_ゴール差差`,
    race_horses.`騎手期待単勝率` - competitors.`競争相手平均騎手期待単勝率` as `num_競争相手平均騎手期待単勝率差`,
    race_horses.`騎手期待３着内率` - competitors.`競争相手平均騎手期待３着内率` as `num_競争相手平均騎手期待３着内率差`,
    race_horses.`展開参考データ_馬スタート指数` - competitors.`競争相手平均展開参考データ_馬スタート指数` as `num_競争相手平均展開参考データ_馬スタート指数差`,
    race_horses.`展開参考データ_馬出遅率` - competitors.`競争相手平均展開参考データ_馬出遅率` as `num_競争相手平均展開参考データ_馬出遅率差`,
    race_horses.`万券指数` - competitors.`競争相手平均万券指数` as `num_競争相手平均万券指数差`,
    race_horses.`休養日数` - competitors.`競争相手平均休養日数` as `num_競争相手平均休養日数差`,
    race_horses.`入厩何日前` - competitors.`競争相手平均入厩何日前` as `num_競争相手平均入厩何日前差`,
    race_horses.`前走距離差` - competitors.`競争相手平均前走距離差` as `num_競争相手平均前走距離差差`,
    race_horses.`年齢` - competitors.`競争相手平均年齢` as `num_競争相手平均年齢差`,
    race_horses.`レース数` - competitors.`競争相手平均レース数` as `num_競争相手平均レース数差`,
    race_horses.`1位完走` - competitors.`競争相手平均1位完走` as `num_競争相手平均1位完走差`,
    race_horses.`トップ3完走` - competitors.`競争相手平均トップ3完走` as `num_競争相手平均トップ3完走差`,
    race_horses.`1位完走率` - competitors.`競争相手平均1位完走率` as `num_競争相手平均1位完走率差`,
    race_horses.`トップ3完走率` - competitors.`競争相手平均トップ3完走率` as `num_競争相手平均トップ3完走率差`,
    race_horses.`過去5走勝率` - competitors.`競争相手平均過去5走勝率` as `num_競争相手平均過去5走勝率差`,
    race_horses.`過去5走トップ3完走率` - competitors.`競争相手平均過去5走トップ3完走率` as `num_競争相手平均過去5走トップ3完走率差`,
    race_horses.`場所レース数` - competitors.`競争相手平均場所レース数` as `num_競争相手平均場所レース数差`,
    race_horses.`場所1位完走` - competitors.`競争相手平均場所1位完走` as `num_競争相手平均場所1位完走差`,
    race_horses.`場所トップ3完走` - competitors.`競争相手平均場所トップ3完走` as `num_競争相手平均場所トップ3完走差`,
    race_horses.`場所1位完走率` - competitors.`競争相手平均場所1位完走率` as `num_競争相手平均場所1位完走率差`,
    race_horses.`場所トップ3完走率` - competitors.`競争相手平均場所トップ3完走率` as `num_競争相手平均場所トップ3完走率差`,
    race_horses.`トラック種別レース数` - competitors.`競争相手平均トラック種別レース数` as `num_競争相手平均トラック種別レース数差`,
    race_horses.`トラック種別1位完走` - competitors.`競争相手平均トラック種別1位完走` as `num_競争相手平均トラック種別1位完走差`,
    race_horses.`トラック種別トップ3完走` - competitors.`競争相手平均トラック種別トップ3完走` as `num_競争相手平均トラック種別トップ3完走差`,
    race_horses.`馬場状態レース数` - competitors.`競争相手平均馬場状態レース数` as `num_競争相手平均馬場状態レース数差`,
    race_horses.`馬場状態1位完走` - competitors.`競争相手平均馬場状態1位完走` as `num_競争相手平均馬場状態1位完走差`,
    race_horses.`馬場状態トップ3完走` - competitors.`競争相手平均馬場状態トップ3完走` as `num_競争相手平均馬場状態トップ3完走差`,
    race_horses.`距離レース数` - competitors.`競争相手平均距離レース数` as `num_競争相手平均距離レース数差`,
    race_horses.`距離1位完走` - competitors.`競争相手平均距離1位完走` as `num_競争相手平均距離1位完走差`,
    race_horses.`距離トップ3完走` - competitors.`競争相手平均距離トップ3完走` as `num_競争相手平均距離トップ3完走差`,
    race_horses.`四半期レース数` - competitors.`競争相手平均四半期レース数` as `num_競争相手平均四半期レース数差`,
    race_horses.`四半期1位完走` - competitors.`競争相手平均四半期1位完走` as `num_競争相手平均四半期1位完走差`,
    race_horses.`四半期トップ3完走` - competitors.`競争相手平均四半期トップ3完走` as `num_競争相手平均四半期トップ3完走差`,
    race_horses.`過去3走順位平方和` - competitors.`競争相手平均過去3走順位平方和` as `num_競争相手平均過去3走順位平方和差`,
    race_horses.`本賞金累計` - competitors.`競争相手平均本賞金累計` as `num_競争相手平均本賞金累計差`,
    race_horses.`1位完走平均賞金` - competitors.`競争相手平均1位完走平均賞金` as `num_競争相手平均1位完走平均賞金差`,
    race_horses.`レース数平均賞金` - competitors.`競争相手平均レース数平均賞金` as `num_競争相手平均レース数平均賞金差`,
    race_horses.`連続1着` - competitors.`競争相手平均連続1着` as `num_競争相手平均連続1着差`,
    race_horses.`連続3着内` - competitors.`競争相手平均連続3着内` as `num_競争相手平均連続3着内差`

  from
    race_horses
  -- 馬はどうするか。。inner joinだと初走の馬は結果に出てこなくなる
  -- 初走の馬にかけても意味がないので、inner joinでいい
  inner join
    competitors
  on
    race_horses.`レースキー` = competitors.`レースキー`
    and race_horses.`馬番` = competitors.`馬番`
  )

select
  *
from
  final
