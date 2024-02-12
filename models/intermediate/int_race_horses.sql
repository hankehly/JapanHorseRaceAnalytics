with
  bac as (
  select
    *
  from
    {{ ref('stg_jrdb__bac') }}
  ),

  kyi as (
  select
    *
  from
    {{ ref('stg_jrdb__kyi') }}
  ),

  sed as (
  select
    *
  from
    {{ ref('stg_jrdb__sed') }}
  ),

  tyb as (
  select
    *
  from
    {{ ref('stg_jrdb__tyb') }}
  ),

  kab as (
  select
    *
  from
    {{ ref('stg_jrdb__kab') }}
  ),

  ukc as (
  select
    *
  from
    {{ ref('stg_jrdb__ukc') }}
  ),

  -- Get the latest data for each horse (scd type 2)
  ukc_latest AS (
  select
    *
  from
    ukc
  where
    (`血統登録番号`, `データ年月日`) in (select `血統登録番号`, MAX(`データ年月日`) from ukc group by `血統登録番号`)
  ),

  horses_good_finish_turf as (
  select
    `競走成績キー_血統登録番号` as `血統登録番号`,
    sum(
      case
        when `ＪＲＤＢデータ_馬場差` <= -10 and `馬成績_着順` <= 3 then 1
        else 0
      end
    ) > 0 `瞬発戦好走馬`,
    sum(
      case
  	    when `ＪＲＤＢデータ_馬場差` >= 10 and `馬成績_着順` <= 3 then 1
  	    else 0
      end
    ) > 0 `消耗戦好走馬`
  from
    sed
  where
    `馬成績_異常区分` = '0'
    and `レース条件_トラック情報_芝ダ障害コード` = '芝'
  group by
    `血統登録番号`
  ),

  horses_good_finish_dirt as (
  select
    `競走成績キー_血統登録番号` as `血統登録番号`,
    sum(
      case
        when `ＪＲＤＢデータ_馬場差` <= -10 and `馬成績_着順` <= 3 then 1
        else 0
      end
    ) > 0 `瞬発戦好走馬`,
    sum(
      case
  	    when `ＪＲＤＢデータ_馬場差` >= 10 and `馬成績_着順` <= 3 then 1
  	    else 0
      end
    ) > 0 `消耗戦好走馬`
  from
    sed
  where
    `馬成績_異常区分` = '0'
    and `レース条件_トラック情報_芝ダ障害コード` = 'ダート'
  group by
    `血統登録番号`
  ),

  horses_good_finish_any as (
  select
    `競走成績キー_血統登録番号` as `血統登録番号`,
    sum(
      case
        when `ＪＲＤＢデータ_馬場差` <= -10 and `馬成績_着順` <= 3 then 1
        else 0
      end
    ) > 0 `瞬発戦好走馬`,
    sum(
      case
  	    when `ＪＲＤＢデータ_馬場差` >= 10 and `馬成績_着順` <= 3 then 1
  	    else 0
      end
    ) > 0 `消耗戦好走馬`
  from
    sed
  where
    `馬成績_異常区分` = '0'
  group by
    `血統登録番号`
  ),

  horses as (
  select
    ukc.`血統登録番号`,
    case
      when ukc.`性別コード` = '1' then '牡'
      when ukc.`性別コード` = '2' then '牝'
      else 'セン'
    end `性別`,
    ukc.`生年月日` as `生年月日`,
    horses_good_finish_turf.`瞬発戦好走馬` as `瞬発戦好走馬_芝`,
    horses_good_finish_turf.`消耗戦好走馬` as `消耗戦好走馬_芝`,
    horses_good_finish_dirt.`瞬発戦好走馬` as `瞬発戦好走馬_ダート`,
    horses_good_finish_dirt.`消耗戦好走馬` as `消耗戦好走馬_ダート`,
    horses_good_finish_any.`瞬発戦好走馬` as `総合瞬発戦好走馬`,
    horses_good_finish_any.`消耗戦好走馬` as `総合消耗戦好走馬`
  from
    ukc_latest as ukc
  left join
    horses_good_finish_turf 
  on
    ukc.`血統登録番号` = horses_good_finish_turf.`血統登録番号`
  left join
    horses_good_finish_dirt
  on
    ukc.`血統登録番号` = horses_good_finish_dirt.`血統登録番号`
  left join
    horses_good_finish_any
  on
    ukc.`血統登録番号` = horses_good_finish_any.`血統登録番号`
  ),

  race_horses_base as (
  select
    kyi.`レースキー`,
    kyi.`馬番`,
    kyi.`枠番`,
    bac.`発走日時`,
    kyi.`レースキー_場コード` as `場コード`,
    -- Todo: add later
    -- ＪＲＤＢデータ_不利
    -- ＪＲＤＢデータ_前不利
    -- ＪＲＤＢデータ_中不利
    -- ＪＲＤＢデータ_後不利
    case
      when extract(month from bac.`発走日時`) <= 3 then 1
      when extract(month from bac.`発走日時`) <= 6 then 2
      when extract(month from bac.`発走日時`) <= 9 then 3
      when extract(month from bac.`発走日時`) <= 12 then 4
    end as `四半期`,
    kyi.`血統登録番号`,
    kyi.`入厩年月日`,
    horses.`生年月日`,
    horses.`性別`,
    case
      when bac.`レース条件_トラック情報_芝ダ障害コード` = 'ダート' then horses.`瞬発戦好走馬_ダート`
      else horses.`瞬発戦好走馬_芝`
    end as `トラック種別瞬発戦好走馬`,
    case
      when bac.`レース条件_トラック情報_芝ダ障害コード` = 'ダート' then horses.`消耗戦好走馬_ダート`
      else horses.`消耗戦好走馬_芝`
    end as `トラック種別消耗戦好走馬`,
    horses.`総合瞬発戦好走馬`,
    horses.`総合消耗戦好走馬`,
    tyb.`馬体重`,
    tyb.`馬体重増減`,
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
    -- how many races this horse has run until now
    row_number() over (partition by kyi.`血統登録番号` order by bac.`発走日時`) - 1 as `レース数`,
    -- how many races this horse has won until now (incremented by one on the following race)
    coalesce(
      cast(
        sum(
          case
            when sed.`馬成績_着順` = 1 then 1
            else 0
          end
        ) over (partition by kyi.`血統登録番号` order by bac.`発走日時` rows between unbounded preceding and 1 preceding) as integer
      ), 0) as `1位完走`, -- horse_wins
    case when `着順` = 1 then 1 else 0 end as is_win,
    case when `着順` <= 3 then 1 else 0 end as is_place,
    coalesce(tyb.`ＩＤＭ`, kyi.`ＩＤＭ`) as `ＩＤＭ`,
    run_style_codes.`name` as `脚質`,
    coalesce(tyb.`単勝オッズ`, win_odds.`単勝オッズ`) as `単勝オッズ`,
    coalesce(tyb.`複勝オッズ`, place_odds.`複勝オッズ`) as `複勝オッズ`,
    coalesce(tyb.`騎手指数`, kyi.`騎手指数`) as `騎手指数`,
    coalesce(tyb.`情報指数`, kyi.`情報指数`) as `情報指数`,
    tyb.`オッズ指数`,
    tyb.`パドック指数`,
    coalesce(tyb.`総合指数`, kyi.`総合指数`) as `総合指数`,
    tyb.`馬具変更情報`,
    tyb.`脚元情報`,
    coalesce(tyb.`負担重量`, kyi.`負担重量`) as `負担重量`,
    coalesce(tyb.`見習い区分`, kyi.`見習い区分`) as `見習い区分`,
    tyb.`オッズ印`,
    tyb.`パドック印`,
    tyb.`直前総合印`,
    horse_form_codes.`name` as `馬体`,
    tyb.`気配コード`,
    kyi.`距離適性`,
    kyi.`上昇度`,
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
    kyi.`クラスコード`,
    kyi.`ブリンカー`,
    kyi.`印コード_総合印`,
    kyi.`印コード_ＩＤＭ印`,
    kyi.`印コード_情報印`,
    kyi.`印コード_騎手印`,
    kyi.`印コード_厩舎印`,
    kyi.`印コード_調教印`,
    kyi.`印コード_激走印`,
    kyi.`展開予想データ_テン指数`,
    kyi.`展開予想データ_ペース指数`,
    kyi.`展開予想データ_上がり指数`,
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
    kyi.`体型_全体`,
    kyi.`体型_背中`,
    kyi.`体型_胴`,
    kyi.`体型_尻`,
    kyi.`体型_トモ`,
    kyi.`体型_腹袋`,
    kyi.`体型_頭`,
    kyi.`体型_首`,
    kyi.`体型_胸`,
    kyi.`体型_肩`,
    kyi.`体型_前長`,
    kyi.`体型_後長`,
    kyi.`体型_前幅`,
    kyi.`体型_後幅`,
    kyi.`体型_前繋`,
    kyi.`体型_後繋`,
    kyi.`体型総合１`,
    kyi.`体型総合２`,
    kyi.`体型総合３`,
    kyi.`馬特記１`,
    kyi.`馬特記２`,
    kyi.`馬特記３`,
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
    kyi

  -- 前日系は inner join
  inner join
    bac
  on
    kyi.`レースキー` = bac.`レースキー`

  -- 実績系はレースキーがないかもしれないから left join
  left join
    sed
  on
    kyi.`レースキー` = sed.`レースキー`
    and kyi.`馬番` = sed.`馬番`

  -- TYBが公開される前に予測する可能性があるから left join
  left join
    tyb
  on
    kyi.`レースキー` = tyb.`レースキー`
    and kyi.`馬番` = tyb.`馬番`

  inner join
    kab
  on
    kyi.`開催キー` = kab.`開催キー`
    and bac.`年月日` = kab.`年月日`

  inner join
    horses
  on
    kyi.`血統登録番号` = horses.`血統登録番号`

  left join
    {{ ref('jrdb__horse_form_codes') }} horse_form_codes
  on
    tyb.`馬体コード` = horse_form_codes.`code`

  left join
    {{ ref('jrdb__run_style_codes') }} run_style_codes
  on
    kyi.`脚質` = run_style_codes.`code`

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
    cast(sum(is_win) over (partition by `血統登録番号`, win_group order by `レース数`) as integer) as `連続1着`,
    cast(sum(is_place) over (partition by `血統登録番号`, place_group order by `レース数`) as integer) as `連続3着内`
  from (
    select
      `レースキー`,
      `馬番`,
      `血統登録番号`,
      is_win,
      is_place,
      `レース数`,
      sum(case when is_win = 0 then 1 else 0 end) over (partition by `血統登録番号` order by `レース数`) as win_group,
      sum(case when is_place = 0 then 1 else 0 end) over (partition by `血統登録番号` order by `レース数`) as place_group
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
    base.`着順` as `先読み注意_着順`,
    base.`本賞金` as `先読み注意_本賞金`,
    base.`性別`,
    base.`トラック種別瞬発戦好走馬`,
    base.`トラック種別消耗戦好走馬`,
    base.`総合瞬発戦好走馬`,
    base.`総合消耗戦好走馬`,
    base.`一走前着順`,
    base.`二走前着順`,
    base.`三走前着順`,
    base.`四走前着順`,
    base.`五走前着順`,
    base.`六走前着順`,
    base.`ＩＤＭ`,
    base.`脚質`,
    base.`単勝オッズ`,
    base.`複勝オッズ`,
    base.`騎手指数`,
    base.`情報指数`,
    base.`オッズ指数`,
    base.`パドック指数`,
    base.`総合指数`,
    base.`馬具変更情報`,
    base.`脚元情報`,
    base.`負担重量`,
    base.`見習い区分`,
    base.`オッズ印`,
    base.`パドック印`,
    base.`直前総合印`,
    base.`馬体`,
    base.`気配コード`,
    base.`距離適性`,
    base.`上昇度`,
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
    base.`クラスコード`,
    base.`ブリンカー`,
    base.`印コード_総合印`,
    base.`印コード_ＩＤＭ印`,
    base.`印コード_情報印`,
    base.`印コード_騎手印`,
    base.`印コード_厩舎印`,
    base.`印コード_調教印`,
    base.`印コード_激走印`,
    base.`展開予想データ_テン指数`,
    base.`展開予想データ_ペース指数`,
    base.`展開予想データ_上がり指数`,
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
    base.`体型_全体`,
    base.`体型_背中`,
    base.`体型_胴`,
    base.`体型_尻`,
    base.`体型_トモ`,
    base.`体型_腹袋`,
    base.`体型_頭`,
    base.`体型_首`,
    base.`体型_胸`,
    base.`体型_肩`,
    base.`体型_前長`,
    base.`体型_後長`,
    base.`体型_前幅`,
    base.`体型_後幅`,
    base.`体型_前繋`,
    base.`体型_後繋`,
    base.`体型総合１`,
    base.`体型総合２`,
    base.`体型総合３`,
    base.`馬特記１`,
    base.`馬特記２`,
    base.`馬特記３`,
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

    -- horse_rest_time
    -- 発走日時` - `入厩年月日` as `入厩何日前`
    date_diff(`発走日時`, `入厩年月日`) as `入厩何日前`,

    -- horse_rest_lest14
    -- 発走日時` - `入厩年月日` < 15 as `入厩15日未満`
    date_diff(`発走日時`, `入厩年月日`) < 15 as `入厩15日未満`,

    -- horse_rest_over35
    -- 発走日時` - `入厩年月日` >= 35 as `入厩35日以上`
    date_diff(`発走日時`, `入厩年月日`) >= 35 as `入厩35日以上`,

    -- declared_weight
    `馬体重`,

    -- diff_declared_weight
    -- todo: calculate yourself and confirm match
    `馬体重増減` as `馬体重増減`,

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
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `トップ3完走`, -- horse_places

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
    lag(race_horses_streaks.`連続3着内`, 1, 0) over (partition by base.`血統登録番号` order by `発走日時`) as `連続3着内`

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

    -- 性別
    sum(case when b.`性別` = '牡' then 1 else 0 end) / cast(count(*) as double) as `競争相手性別牡割合`,
    sum(case when b.`性別` = '牝' then 1 else 0 end) / cast(count(*) as double) as `競争相手性別牝割合`,
    sum(case when b.`性別` = 'セン' then 1 else 0 end) / cast(count(*) as double) as `競争相手性別セ割合`,

    -- トラック種別瞬発戦好走馬
    sum(case when b.`トラック種別瞬発戦好走馬` then 1 else 0 end) / cast(count(*) as double) as `競争相手トラック種別瞬発戦好走馬割合`,

    -- トラック種別消耗戦好走馬
    sum(case when b.`トラック種別消耗戦好走馬` then 1 else 0 end) / cast(count(*) as double) as `競争相手トラック種別消耗戦好走馬割合`,

    -- 総合瞬発戦好走馬
    sum(case when b.`総合瞬発戦好走馬` then 1 else 0 end) / cast(count(*) as double) as `競争相手総合瞬発戦好走馬割合`,

    -- 総合消耗戦好走馬
    sum(case when b.`総合消耗戦好走馬` then 1 else 0 end) / cast(count(*) as double) as `競争相手総合消耗戦好走馬割合`,

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

    -- ＩＤＭ
    max(b.`ＩＤＭ`) as `競争相手最高ＩＤＭ`,
    min(b.`ＩＤＭ`) as `競争相手最低ＩＤＭ`,
    avg(b.`ＩＤＭ`) as `競争相手平均ＩＤＭ`,
    stddev_pop(b.`ＩＤＭ`) as `競争相手ＩＤＭ標準偏差`,

    -- IDM標準偏差
    -- 脚質

    -- 単勝オッズ
    max(b.`単勝オッズ`) as `競争相手最高単勝オッズ`,
    min(b.`単勝オッズ`) as `競争相手最低単勝オッズ`,
    avg(b.`単勝オッズ`) as `競争相手平均単勝オッズ`,
    stddev_pop(b.`単勝オッズ`) as `競争相手単勝オッズ標準偏差`,

    -- 複勝オッズ
    max(b.`複勝オッズ`) as `競争相手最高複勝オッズ`,
    min(b.`複勝オッズ`) as `競争相手最低複勝オッズ`,
    avg(b.`複勝オッズ`) as `競争相手平均複勝オッズ`,
    stddev_pop(b.`複勝オッズ`) as `競争相手複勝オッズ標準偏差`,

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

    -- 馬具変更情報
    -- 脚元情報

    -- 負担重量
    max(b.`負担重量`) as `競争相手最高負担重量`,
    min(b.`負担重量`) as `競争相手最低負担重量`,
    avg(b.`負担重量`) as `競争相手平均負担重量`,
    stddev_pop(b.`負担重量`) as `競争相手負担重量標準偏差`,

    -- 見習い区分
    -- オッズ印
    -- パドック印
    -- 直前総合印
    -- 馬体
    -- 気配コード
    -- 距離適性
    -- 上昇度

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

    -- 基準人気順位

    -- 基準複勝オッズ
    max(b.`基準複勝オッズ`) as `競争相手最高基準複勝オッズ`,
    min(b.`基準複勝オッズ`) as `競争相手最低基準複勝オッズ`,
    avg(b.`基準複勝オッズ`) as `競争相手平均基準複勝オッズ`,
    stddev_pop(b.`基準複勝オッズ`) as `競争相手基準複勝オッズ標準偏差`,

    -- 基準複勝人気順位
    -- 特定情報◎
    -- 特定情報○
    -- 特定情報▲
    -- 特定情報△
    -- 特定情報×
    -- 総合情報◎
    -- 総合情報○
    -- 総合情報▲
    -- 総合情報△
    -- 総合情報×

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

    -- 調教矢印コード
    -- 厩舎評価コード

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

    -- 蹄コード
    -- 重適性コード
    -- クラスコード
    -- ブリンカー
    -- 印コード_総合印
    -- 印コード_ＩＤＭ印
    -- 印コード_情報印
    -- 印コード_騎手印
    -- 印コード_厩舎印
    -- 印コード_調教印
    -- 印コード_激走印

    -- 展開予想データ_テン指数
    max(b.`展開予想データ_テン指数`) as `競争相手最高展開予想データ_テン指数`,
    min(b.`展開予想データ_テン指数`) as `競争相手最低展開予想データ_テン指数`,
    avg(b.`展開予想データ_テン指数`) as `競争相手平均展開予想データ_テン指数`,
    stddev_pop(b.`展開予想データ_テン指数`) as `競争相手展開予想データ_テン指数標準偏差`,

    -- 展開予想データ_ペース指数
    max(b.`展開予想データ_ペース指数`) as `競争相手最高展開予想データ_ペース指数`,
    min(b.`展開予想データ_ペース指数`) as `競争相手最低展開予想データ_ペース指数`,
    avg(b.`展開予想データ_ペース指数`) as `競争相手平均展開予想データ_ペース指数`,
    stddev_pop(b.`展開予想データ_ペース指数`) as `競争相手展開予想データ_ペース指数標準偏差`,

    -- 展開予想データ_上がり指数
    max(b.`展開予想データ_上がり指数`) as `競争相手最高展開予想データ_上がり指数`,
    min(b.`展開予想データ_上がり指数`) as `競争相手最低展開予想データ_上がり指数`,
    avg(b.`展開予想データ_上がり指数`) as `競争相手平均展開予想データ_上がり指数`,
    stddev_pop(b.`展開予想データ_上がり指数`) as `競争相手展開予想データ_上がり指数標準偏差`,

    -- 展開予想データ_位置指数
    max(b.`展開予想データ_位置指数`) as `競争相手最高展開予想データ_位置指数`,
    min(b.`展開予想データ_位置指数`) as `競争相手最低展開予想データ_位置指数`,
    avg(b.`展開予想データ_位置指数`) as `競争相手平均展開予想データ_位置指数`,
    stddev_pop(b.`展開予想データ_位置指数`) as `競争相手展開予想データ_位置指数標準偏差`,

    -- 展開予想データ_ペース予想
    -- 展開予想データ_道中順位

    -- 展開予想データ_道中差
    max(b.`展開予想データ_道中差`) as `競争相手最高展開予想データ_道中差`,
    min(b.`展開予想データ_道中差`) as `競争相手最低展開予想データ_道中差`,
    avg(b.`展開予想データ_道中差`) as `競争相手平均展開予想データ_道中差`,
    stddev_pop(b.`展開予想データ_道中差`) as `競争相手展開予想データ_道中差標準偏差`,

    -- 展開予想データ_道中内外
    -- 展開予想データ_後３Ｆ順位

    -- 展開予想データ_後３Ｆ差
    max(b.`展開予想データ_後３Ｆ差`) as `競争相手最高展開予想データ_後３Ｆ差`,
    min(b.`展開予想データ_後３Ｆ差`) as `競争相手最低展開予想データ_後３Ｆ差`,
    avg(b.`展開予想データ_後３Ｆ差`) as `競争相手平均展開予想データ_後３Ｆ差`,
    stddev_pop(b.`展開予想データ_後３Ｆ差`) as `競争相手展開予想データ_後３Ｆ差標準偏差`,

    -- 展開予想データ_後３Ｆ内外
    -- 展開予想データ_ゴール順位

    -- 展開予想データ_ゴール差
    max(b.`展開予想データ_ゴール差`) as `競争相手最高展開予想データ_ゴール差`,
    min(b.`展開予想データ_ゴール差`) as `競争相手最低展開予想データ_ゴール差`,
    avg(b.`展開予想データ_ゴール差`) as `競争相手平均展開予想データ_ゴール差`,
    stddev_pop(b.`展開予想データ_ゴール差`) as `競争相手展開予想データ_ゴール差標準偏差`,

    -- 展開予想データ_ゴール内外
    -- 展開予想データ_展開記号
    -- 激走順位
    -- LS指数順位
    -- テン指数順位
    -- ペース指数順位
    -- 上がり指数順位
    -- 位置指数順位

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

    -- 輸送区分
    -- 体型_全体
    -- 体型_背中
    -- 体型_胴
    -- 体型_尻
    -- 体型_トモ
    -- 体型_腹袋
    -- 体型_頭
    -- 体型_首
    -- 体型_胸
    -- 体型_肩
    -- 体型_前長
    -- 体型_後長
    -- 体型_前幅
    -- 体型_後幅
    -- 体型_前繋
    -- 体型_後繋
    -- 体型総合１
    -- 体型総合２
    -- 体型総合３
    -- 馬特記１
    -- 馬特記２
    -- 馬特記３
  
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

    -- 万券印
    -- 激走タイプ
    -- 休養理由分類コード
    -- 芝ダ障害フラグ
    -- 距離フラグ
    -- クラスフラグ
    -- 転厩フラグ
    -- 去勢フラグ
    -- 乗替フラグ
    -- 放牧先ランク
    -- 厩舎ランク

    -- 前走トップ3
    sum(case when b.`前走トップ3` then 1 else 0 end) / cast(count(*) as double) as `競争相手前走トップ3割合`,

    -- 前走枠番

    -- 入厩何日前
    max(b.`入厩何日前`) as `競争相手最高入厩何日前`,
    min(b.`入厩何日前`) as `競争相手最低入厩何日前`,
    avg(b.`入厩何日前`) as `競争相手平均入厩何日前`,
    stddev_pop(b.`入厩何日前`) as `競争相手入厩何日前標準偏差`,

    -- 入厩15日未満
    -- 入厩35日以上

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

    -- 距離

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

    -- 4歳以下
    -- 4歳以下頭数
    -- 4歳以下割合

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
    race_horses.`レースキー`,
    race_horses.`馬番`,
    race_horses.`血統登録番号`,
    race_horses.`発走日時`,
    race_horses.`先読み注意_着順`,
    race_horses.`先読み注意_本賞金`,

    -- Base features
    race_horses.`性別`,
    race_horses.`トラック種別瞬発戦好走馬`,
    race_horses.`トラック種別消耗戦好走馬`,
    race_horses.`総合瞬発戦好走馬`,
    race_horses.`総合消耗戦好走馬`,
    race_horses.`一走前着順`,
    race_horses.`二走前着順`,
    race_horses.`三走前着順`,
    race_horses.`四走前着順`,
    race_horses.`五走前着順`,
    race_horses.`六走前着順`,
    race_horses.`ＩＤＭ`,
    race_horses.`脚質`,
    race_horses.`単勝オッズ`,
    race_horses.`複勝オッズ`,
    race_horses.`騎手指数`,
    race_horses.`情報指数`,
    race_horses.`オッズ指数`,
    race_horses.`パドック指数`,
    race_horses.`総合指数`,
    race_horses.`馬具変更情報`,
    race_horses.`脚元情報`,
    race_horses.`負担重量`,
    race_horses.`見習い区分`,
    race_horses.`オッズ印`,
    race_horses.`パドック印`,
    race_horses.`直前総合印`,
    race_horses.`馬体`,
    race_horses.`気配コード`,
    race_horses.`距離適性`,
    race_horses.`上昇度`,
    race_horses.`ローテーション`,
    race_horses.`基準オッズ`,
    race_horses.`基準人気順位`,
    race_horses.`基準複勝オッズ`,
    race_horses.`基準複勝人気順位`,
    race_horses.`特定情報◎`,
    race_horses.`特定情報○`,
    race_horses.`特定情報▲`,
    race_horses.`特定情報△`,
    race_horses.`特定情報×`,
    race_horses.`総合情報◎`,
    race_horses.`総合情報○`,
    race_horses.`総合情報▲`,
    race_horses.`総合情報△`,
    race_horses.`総合情報×`,
    race_horses.`人気指数`,
    race_horses.`調教指数`,
    race_horses.`厩舎指数`,
    race_horses.`調教矢印コード`,
    race_horses.`厩舎評価コード`,
    race_horses.`騎手期待連対率`,
    race_horses.`激走指数`,
    race_horses.`蹄コード`,
    race_horses.`重適性コード`,
    race_horses.`クラスコード`,
    race_horses.`ブリンカー`,
    race_horses.`印コード_総合印`,
    race_horses.`印コード_ＩＤＭ印`,
    race_horses.`印コード_情報印`,
    race_horses.`印コード_騎手印`,
    race_horses.`印コード_厩舎印`,
    race_horses.`印コード_調教印`,
    race_horses.`印コード_激走印`,
    race_horses.`展開予想データ_テン指数`,
    race_horses.`展開予想データ_ペース指数`,
    race_horses.`展開予想データ_上がり指数`,
    race_horses.`展開予想データ_位置指数`,
    race_horses.`展開予想データ_ペース予想`,
    race_horses.`展開予想データ_道中順位`,
    race_horses.`展開予想データ_道中差`,
    race_horses.`展開予想データ_道中内外`,
    race_horses.`展開予想データ_後３Ｆ順位`,
    race_horses.`展開予想データ_後３Ｆ差`,
    race_horses.`展開予想データ_後３Ｆ内外`,
    race_horses.`展開予想データ_ゴール順位`,
    race_horses.`展開予想データ_ゴール差`,
    race_horses.`展開予想データ_ゴール内外`,
    race_horses.`展開予想データ_展開記号`,
    race_horses.`激走順位`,
    race_horses.`LS指数順位`,
    race_horses.`テン指数順位`,
    race_horses.`ペース指数順位`,
    race_horses.`上がり指数順位`,
    race_horses.`位置指数順位`,
    race_horses.`騎手期待単勝率`,
    race_horses.`騎手期待３着内率`,
    race_horses.`輸送区分`,
    race_horses.`体型_全体`,
    race_horses.`体型_背中`,
    race_horses.`体型_胴`,
    race_horses.`体型_尻`,
    race_horses.`体型_トモ`,
    race_horses.`体型_腹袋`,
    race_horses.`体型_頭`,
    race_horses.`体型_首`,
    race_horses.`体型_胸`,
    race_horses.`体型_肩`,
    race_horses.`体型_前長`,
    race_horses.`体型_後長`,
    race_horses.`体型_前幅`,
    race_horses.`体型_後幅`,
    race_horses.`体型_前繋`,
    race_horses.`体型_後繋`,
    race_horses.`体型総合１`,
    race_horses.`体型総合２`,
    race_horses.`体型総合３`,
    race_horses.`馬特記１`,
    race_horses.`馬特記２`,
    race_horses.`馬特記３`,
    race_horses.`展開参考データ_馬スタート指数`,
    race_horses.`展開参考データ_馬出遅率`,
    race_horses.`万券指数`,
    race_horses.`万券印`,
    race_horses.`激走タイプ`,
    race_horses.`休養理由分類コード`,
    race_horses.`芝ダ障害フラグ`,
    race_horses.`距離フラグ`,
    race_horses.`クラスフラグ`,
    race_horses.`転厩フラグ`,
    race_horses.`去勢フラグ`,
    race_horses.`乗替フラグ`,
    race_horses.`放牧先ランク`,
    race_horses.`厩舎ランク`,
    race_horses.`前走トップ3`,
    race_horses.`枠番`,
    race_horses.`前走枠番`,
    race_horses.`入厩何日前`, -- horse_rest_time
    race_horses.`入厩15日未満`, -- horse_rest_lest14
    race_horses.`入厩35日以上`, -- horse_rest_over35
    race_horses.`馬体重`, -- declared_weight
    race_horses.`馬体重増減`, -- diff_declared_weight (todo: check if this matches lag)
    race_horses.`前走距離差`, -- diff_distance
    race_horses.`年齢`, -- horse_age (years)
    race_horses.`4歳以下`,
    race_horses.`4歳以下頭数`,
    race_horses.`4歳以下割合`,
    race_horses.`レース数`, -- horse_runs
    race_horses.`1位完走`, -- horse_wins
    race_horses.`トップ3完走`, -- horse_places
    race_horses.`1位完走率`,
    race_horses.`トップ3完走率`,
    race_horses.`過去5走勝率`,
    race_horses.`過去5走トップ3完走率`,
    race_horses.`場所レース数`, -- horse_venue_runs
    race_horses.`場所1位完走`, -- horse_venue_wins
    race_horses.`場所トップ3完走`, -- horse_venue_places
    race_horses.`場所1位完走率`, -- ratio_win_horse_venue
    race_horses.`場所トップ3完走率`, -- ratio_place_horse_venue
    race_horses.`トラック種別レース数`, -- horse_surface_runs
    race_horses.`トラック種別1位完走`, -- horse_surface_wins
    race_horses.`トラック種別トップ3完走`, -- horse_surface_places
    race_horses.`トラック種別1位完走率`, -- ratio_win_horse_surface
    race_horses.`トラック種別トップ3完走率`, -- ratio_place_horse_surface
    race_horses.`馬場状態レース数`, -- horse_going_runs
    race_horses.`馬場状態1位完走`, -- horse_going_wins
    race_horses.`馬場状態トップ3完走`, -- horse_going_places
    race_horses.`馬場状態1位完走率`, -- ratio_win_horse_going
    race_horses.`馬場状態トップ3完走率`, -- ratio_place_horse_going
    race_horses.`距離レース数`, -- horse_distance_runs
    race_horses.`距離1位完走`, -- horse_distance_wins
    race_horses.`距離トップ3完走`, -- horse_distance_places
    race_horses.`距離1位完走率`, -- ratio_win_horse_distance
    race_horses.`距離トップ3完走率`, -- ratio_place_horse_distance
    race_horses.`四半期レース数`, -- horse_quarter_runs
    race_horses.`四半期1位完走`, -- horse_quarter_wins
    race_horses.`四半期トップ3完走`, -- horse_quarter_places
    race_horses.`四半期1位完走率`, -- ratio_win_horse_quarter
    race_horses.`四半期トップ3完走率`, -- ratio_place_horse_quarter
    race_horses.`過去3走順位平方和`,
    race_horses.`本賞金累計`,
    race_horses.`1位完走平均賞金`,
    race_horses.`レース数平均賞金`,
    race_horses.`連続1着`,
    race_horses.`連続3着内`,

    -- Competitors
    competitors.`競争相手性別牡割合`,
    competitors.`競争相手性別牝割合`,
    competitors.`競争相手性別セ割合`,
    competitors.`競争相手最高一走前着順`,
    competitors.`競争相手最低一走前着順`,
    competitors.`競争相手平均一走前着順`,
    competitors.`競争相手一走前着順標準偏差`,
    competitors.`競争相手最高二走前着順`,
    competitors.`競争相手最低二走前着順`,
    competitors.`競争相手平均二走前着順`,
    competitors.`競争相手二走前着順標準偏差`,
    competitors.`競争相手最高三走前着順`,
    competitors.`競争相手最低三走前着順`,
    competitors.`競争相手平均三走前着順`,
    competitors.`競争相手三走前着順標準偏差`,
    competitors.`競争相手最高四走前着順`,
    competitors.`競争相手最低四走前着順`,
    competitors.`競争相手平均四走前着順`,
    competitors.`競争相手四走前着順標準偏差`,
    competitors.`競争相手最高五走前着順`,
    competitors.`競争相手最低五走前着順`,
    competitors.`競争相手平均五走前着順`,
    competitors.`競争相手五走前着順標準偏差`,
    competitors.`競争相手最高六走前着順`,
    competitors.`競争相手最低六走前着順`,
    competitors.`競争相手平均六走前着順`,
    competitors.`競争相手六走前着順標準偏差`,
    competitors.`競争相手最高ＩＤＭ`,
    competitors.`競争相手最低ＩＤＭ`,
    competitors.`競争相手平均ＩＤＭ`,
    competitors.`競争相手ＩＤＭ標準偏差`,
    competitors.`競争相手最高単勝オッズ`,
    competitors.`競争相手最低単勝オッズ`,
    competitors.`競争相手平均単勝オッズ`,
    competitors.`競争相手単勝オッズ標準偏差`,
    competitors.`競争相手最高複勝オッズ`,
    competitors.`競争相手最低複勝オッズ`,
    competitors.`競争相手平均複勝オッズ`,
    competitors.`競争相手複勝オッズ標準偏差`,
    competitors.`競争相手最高騎手指数`,
    competitors.`競争相手最低騎手指数`,
    competitors.`競争相手平均騎手指数`,
    competitors.`競争相手騎手指数標準偏差`,
    competitors.`競争相手最高情報指数`,
    competitors.`競争相手最低情報指数`,
    competitors.`競争相手平均情報指数`,
    competitors.`競争相手情報指数標準偏差`,
    competitors.`競争相手最高オッズ指数`,
    competitors.`競争相手最低オッズ指数`,
    competitors.`競争相手平均オッズ指数`,
    competitors.`競争相手オッズ指数標準偏差`,
    competitors.`競争相手最高パドック指数`,
    competitors.`競争相手最低パドック指数`,
    competitors.`競争相手平均パドック指数`,
    competitors.`競争相手パドック指数標準偏差`,
    competitors.`競争相手最高総合指数`,
    competitors.`競争相手最低総合指数`,
    competitors.`競争相手平均総合指数`,
    competitors.`競争相手総合指数標準偏差`,
    competitors.`競争相手最高負担重量`,
    competitors.`競争相手最低負担重量`,
    competitors.`競争相手平均負担重量`,
    competitors.`競争相手負担重量標準偏差`,
    competitors.`競争相手最高ローテーション`,
    competitors.`競争相手最低ローテーション`,
    competitors.`競争相手平均ローテーション`,
    competitors.`競争相手ローテーション標準偏差`,
    competitors.`競争相手最高基準オッズ`,
    competitors.`競争相手最低基準オッズ`,
    competitors.`競争相手平均基準オッズ`,
    competitors.`競争相手基準オッズ標準偏差`,
    competitors.`競争相手最高基準複勝オッズ`,
    competitors.`競争相手最低基準複勝オッズ`,
    competitors.`競争相手平均基準複勝オッズ`,
    competitors.`競争相手基準複勝オッズ標準偏差`,
    competitors.`競争相手最高人気指数`,
    competitors.`競争相手最低人気指数`,
    competitors.`競争相手平均人気指数`,
    competitors.`競争相手人気指数標準偏差`,
    competitors.`競争相手最高調教指数`,
    competitors.`競争相手最低調教指数`,
    competitors.`競争相手平均調教指数`,
    competitors.`競争相手調教指数標準偏差`,
    competitors.`競争相手最高厩舎指数`,
    competitors.`競争相手最低厩舎指数`,
    competitors.`競争相手平均厩舎指数`,
    competitors.`競争相手厩舎指数標準偏差`,
    competitors.`競争相手最高騎手期待連対率`,
    competitors.`競争相手最低騎手期待連対率`,
    competitors.`競争相手平均騎手期待連対率`,
    competitors.`競争相手騎手期待連対率標準偏差`,
    competitors.`競争相手最高激走指数`,
    competitors.`競争相手最低激走指数`,
    competitors.`競争相手平均激走指数`,
    competitors.`競争相手激走指数標準偏差`,
    competitors.`競争相手最高展開予想データ_テン指数`,
    competitors.`競争相手最低展開予想データ_テン指数`,
    competitors.`競争相手平均展開予想データ_テン指数`,
    competitors.`競争相手展開予想データ_テン指数標準偏差`,
    competitors.`競争相手最高展開予想データ_ペース指数`,
    competitors.`競争相手最低展開予想データ_ペース指数`,
    competitors.`競争相手平均展開予想データ_ペース指数`,
    competitors.`競争相手展開予想データ_ペース指数標準偏差`,
    competitors.`競争相手最高展開予想データ_上がり指数`,
    competitors.`競争相手最低展開予想データ_上がり指数`,
    competitors.`競争相手平均展開予想データ_上がり指数`,
    competitors.`競争相手展開予想データ_上がり指数標準偏差`,
    competitors.`競争相手最高展開予想データ_位置指数`,
    competitors.`競争相手最低展開予想データ_位置指数`,
    competitors.`競争相手平均展開予想データ_位置指数`,
    competitors.`競争相手展開予想データ_位置指数標準偏差`,
    competitors.`競争相手最高展開予想データ_道中差`,
    competitors.`競争相手最低展開予想データ_道中差`,
    competitors.`競争相手平均展開予想データ_道中差`,
    competitors.`競争相手展開予想データ_道中差標準偏差`,
    competitors.`競争相手最高展開予想データ_後３Ｆ差`,
    competitors.`競争相手最低展開予想データ_後３Ｆ差`,
    competitors.`競争相手平均展開予想データ_後３Ｆ差`,
    competitors.`競争相手展開予想データ_後３Ｆ差標準偏差`,
    competitors.`競争相手最高展開予想データ_ゴール差`,
    competitors.`競争相手最低展開予想データ_ゴール差`,
    competitors.`競争相手平均展開予想データ_ゴール差`,
    competitors.`競争相手展開予想データ_ゴール差標準偏差`,
    competitors.`競争相手最高騎手期待単勝率`,
    competitors.`競争相手最低騎手期待単勝率`,
    competitors.`競争相手平均騎手期待単勝率`,
    competitors.`競争相手騎手期待単勝率標準偏差`,
    competitors.`競争相手最高騎手期待３着内率`,
    competitors.`競争相手最低騎手期待３着内率`,
    competitors.`競争相手平均騎手期待３着内率`,
    competitors.`競争相手騎手期待３着内率標準偏差`,
    competitors.`競争相手最高展開参考データ_馬スタート指数`,
    competitors.`競争相手最低展開参考データ_馬スタート指数`,
    competitors.`競争相手平均展開参考データ_馬スタート指数`,
    competitors.`競争相手展開参考データ_馬スタート指数標準偏差`,
    competitors.`競争相手最高展開参考データ_馬出遅率`,
    competitors.`競争相手最低展開参考データ_馬出遅率`,
    competitors.`競争相手平均展開参考データ_馬出遅率`,
    competitors.`競争相手展開参考データ_馬出遅率標準偏差`,
    competitors.`競争相手最高万券指数`,
    competitors.`競争相手最低万券指数`,
    competitors.`競争相手平均万券指数`,
    competitors.`競争相手万券指数標準偏差`,
    competitors.`競争相手前走トップ3割合`,
    competitors.`競争相手最高入厩何日前`,
    competitors.`競争相手最低入厩何日前`,
    competitors.`競争相手平均入厩何日前`,
    competitors.`競争相手入厩何日前標準偏差`,
    competitors.`競争相手最高馬体重`,
    competitors.`競争相手最低馬体重`,
    competitors.`競争相手平均馬体重`,
    competitors.`競争相手馬体重標準偏差`,
    competitors.`競争相手最高馬体重増減`,
    competitors.`競争相手最低馬体重増減`,
    competitors.`競争相手平均馬体重増減`,
    competitors.`競争相手馬体重増減標準偏差`,
    competitors.`競争相手最高年齢`,
    competitors.`競争相手最低年齢`,
    competitors.`競争相手平均年齢`,
    competitors.`競争相手年齢標準偏差`,
    competitors.`競争相手最高レース数`,
    competitors.`競争相手最低レース数`,
    competitors.`競争相手平均レース数`,
    competitors.`競争相手レース数標準偏差`,
    competitors.`競争相手最高1位完走`,
    competitors.`競争相手最低1位完走`,
    competitors.`競争相手平均1位完走`,
    competitors.`競争相手1位完走標準偏差`,
    competitors.`競争相手最高トップ3完走`,
    competitors.`競争相手最低トップ3完走`,
    competitors.`競争相手平均トップ3完走`,
    competitors.`競争相手トップ3完走標準偏差`,
    competitors.`競争相手最高1位完走率`,
    competitors.`競争相手最低1位完走率`,
    competitors.`競争相手平均1位完走率`,
    competitors.`競争相手1位完走率標準偏差`,
    competitors.`競争相手最高トップ3完走率`,
    competitors.`競争相手最低トップ3完走率`,
    competitors.`競争相手平均トップ3完走率`,
    competitors.`競争相手トップ3完走率標準偏差`,
    competitors.`競争相手最高過去5走勝率`,
    competitors.`競争相手最低過去5走勝率`,
    competitors.`競争相手平均過去5走勝率`,
    competitors.`競争相手過去5走勝率標準偏差`,
    competitors.`競争相手最高過去5走トップ3完走率`,
    competitors.`競争相手最低過去5走トップ3完走率`,
    competitors.`競争相手平均過去5走トップ3完走率`,
    competitors.`競争相手過去5走トップ3完走率標準偏差`,
    competitors.`競争相手最高場所レース数`,
    competitors.`競争相手最低場所レース数`,
    competitors.`競争相手平均場所レース数`,
    competitors.`競争相手場所レース数標準偏差`,
    competitors.`競争相手最高場所1位完走`,
    competitors.`競争相手最低場所1位完走`,
    competitors.`競争相手平均場所1位完走`,
    competitors.`競争相手場所1位完走標準偏差`,
    competitors.`競争相手最高場所トップ3完走`,
    competitors.`競争相手最低場所トップ3完走`,
    competitors.`競争相手平均場所トップ3完走`,
    competitors.`競争相手場所トップ3完走標準偏差`,
    competitors.`競争相手最高場所1位完走率`,
    competitors.`競争相手最低場所1位完走率`,
    competitors.`競争相手平均場所1位完走率`,
    competitors.`競争相手場所1位完走率標準偏差`,
    competitors.`競争相手最高場所トップ3完走率`,
    competitors.`競争相手最低場所トップ3完走率`,
    competitors.`競争相手平均場所トップ3完走率`,
    competitors.`競争相手場所トップ3完走率標準偏差`,
    competitors.`競争相手最高トラック種別レース数`,
    competitors.`競争相手最低トラック種別レース数`,
    competitors.`競争相手平均トラック種別レース数`,
    competitors.`競争相手トラック種別レース数標準偏差`,
    competitors.`競争相手最高トラック種別1位完走`,
    competitors.`競争相手最低トラック種別1位完走`,
    competitors.`競争相手平均トラック種別1位完走`,
    competitors.`競争相手トラック種別1位完走標準偏差`,
    competitors.`競争相手最高トラック種別トップ3完走`,
    competitors.`競争相手最低トラック種別トップ3完走`,
    competitors.`競争相手平均トラック種別トップ3完走`,
    competitors.`競争相手トラック種別トップ3完走標準偏差`,
    competitors.`競争相手最高馬場状態レース数`,
    competitors.`競争相手最低馬場状態レース数`,
    competitors.`競争相手平均馬場状態レース数`,
    competitors.`競争相手馬場状態レース数標準偏差`,
    competitors.`競争相手最高馬場状態1位完走`,
    competitors.`競争相手最低馬場状態1位完走`,
    competitors.`競争相手平均馬場状態1位完走`,
    competitors.`競争相手馬場状態1位完走標準偏差`,
    competitors.`競争相手最高馬場状態トップ3完走`,
    competitors.`競争相手最低馬場状態トップ3完走`,
    competitors.`競争相手平均馬場状態トップ3完走`,
    competitors.`競争相手馬場状態トップ3完走標準偏差`,
    competitors.`競争相手最高距離レース数`,
    competitors.`競争相手最低距離レース数`,
    competitors.`競争相手平均距離レース数`,
    competitors.`競争相手距離レース数標準偏差`,
    competitors.`競争相手最高距離1位完走`,
    competitors.`競争相手最低距離1位完走`,
    competitors.`競争相手平均距離1位完走`,
    competitors.`競争相手距離1位完走標準偏差`,
    competitors.`競争相手最高距離トップ3完走`,
    competitors.`競争相手最低距離トップ3完走`,
    competitors.`競争相手平均距離トップ3完走`,
    competitors.`競争相手距離トップ3完走標準偏差`,
    competitors.`競争相手最高四半期レース数`,
    competitors.`競争相手最低四半期レース数`,
    competitors.`競争相手平均四半期レース数`,
    competitors.`競争相手四半期レース数標準偏差`,
    competitors.`競争相手最高四半期1位完走`,
    competitors.`競争相手最低四半期1位完走`,
    competitors.`競争相手平均四半期1位完走`,
    competitors.`競争相手四半期1位完走標準偏差`,
    competitors.`競争相手最高四半期トップ3完走`,
    competitors.`競争相手最低四半期トップ3完走`,
    competitors.`競争相手平均四半期トップ3完走`,
    competitors.`競争相手四半期トップ3完走標準偏差`,
    competitors.`競争相手最高過去3走順位平方和`,
    competitors.`競争相手最低過去3走順位平方和`,
    competitors.`競争相手平均過去3走順位平方和`,
    competitors.`競争相手過去3走順位平方和標準偏差`,
    competitors.`競争相手最高本賞金累計`,
    competitors.`競争相手最低本賞金累計`,
    competitors.`競争相手平均本賞金累計`,
    competitors.`競争相手本賞金累計標準偏差`,
    competitors.`競争相手最高1位完走平均賞金`,
    competitors.`競争相手最低1位完走平均賞金`,
    competitors.`競争相手平均1位完走平均賞金`,
    competitors.`競争相手1位完走平均賞金標準偏差`,
    competitors.`競争相手最高レース数平均賞金`,
    competitors.`競争相手最低レース数平均賞金`,
    competitors.`競争相手平均レース数平均賞金`,
    competitors.`競争相手レース数平均賞金標準偏差`,
    competitors.`競争相手トラック種別瞬発戦好走馬割合`,
    competitors.`競争相手トラック種別消耗戦好走馬割合`,
    competitors.`競争相手総合瞬発戦好走馬割合`,
    competitors.`競争相手総合消耗戦好走馬割合`,
    competitors.`競争相手最高連続1着`,
    competitors.`競争相手最低連続1着`,
    competitors.`競争相手平均連続1着`,
    competitors.`競争相手連続1着標準偏差`,
    competitors.`競争相手最高連続3着内`,
    competitors.`競争相手最低連続3着内`,
    competitors.`競争相手平均連続3着内`,
    competitors.`競争相手連続3着内標準偏差`,

    -- Relative to competitors
    race_horses.`一走前着順` - competitors.`競争相手平均一走前着順` as `競争相手平均一走前着順差`,
    race_horses.`二走前着順` - competitors.`競争相手平均二走前着順` as `競争相手平均二走前着順差`,
    race_horses.`三走前着順` - competitors.`競争相手平均三走前着順` as `競争相手平均三走前着順差`,
    race_horses.`四走前着順` - competitors.`競争相手平均四走前着順` as `競争相手平均四走前着順差`,
    race_horses.`五走前着順` - competitors.`競争相手平均五走前着順` as `競争相手平均五走前着順差`,
    race_horses.`六走前着順` - competitors.`競争相手平均六走前着順` as `競争相手平均六走前着順差`,
    race_horses.`ＩＤＭ` - competitors.`競争相手平均ＩＤＭ` as `競争相手平均ＩＤＭ差`,
    race_horses.`単勝オッズ` - competitors.`競争相手平均単勝オッズ` as `競争相手平均単勝オッズ差`,
    race_horses.`複勝オッズ` - competitors.`競争相手平均複勝オッズ` as `競争相手平均複勝オッズ差`,
    race_horses.`騎手指数` - competitors.`競争相手平均騎手指数` as `競争相手平均騎手指数差`,
    race_horses.`情報指数` - competitors.`競争相手平均情報指数` as `競争相手平均情報指数差`,
    race_horses.`オッズ指数` - competitors.`競争相手平均オッズ指数` as `競争相手平均オッズ指数差`,
    race_horses.`パドック指数` - competitors.`競争相手平均パドック指数` as `競争相手平均パドック指数差`,
    race_horses.`総合指数` - competitors.`競争相手平均総合指数` as `競争相手平均総合指数差`,
    race_horses.`負担重量` - competitors.`競争相手平均負担重量` as `競争相手平均負担重量差`,
    race_horses.`ローテーション` - competitors.`競争相手平均ローテーション` as `競争相手平均ローテーション差`,
    race_horses.`基準オッズ` - competitors.`競争相手平均基準オッズ` as `競争相手平均基準オッズ差`,
    race_horses.`基準複勝オッズ` - competitors.`競争相手平均基準複勝オッズ` as `競争相手平均基準複勝オッズ差`,
    race_horses.`人気指数` - competitors.`競争相手平均人気指数` as `競争相手平均人気指数差`,
    race_horses.`調教指数` - competitors.`競争相手平均調教指数` as `競争相手平均調教指数差`,
    race_horses.`厩舎指数` - competitors.`競争相手平均厩舎指数` as `競争相手平均厩舎指数差`,
    race_horses.`騎手期待連対率` - competitors.`競争相手平均騎手期待連対率` as `競争相手平均騎手期待連対率差`,
    race_horses.`激走指数` - competitors.`競争相手平均激走指数` as `競争相手平均激走指数差`,
    race_horses.`展開予想データ_テン指数` - competitors.`競争相手平均展開予想データ_テン指数` as `競争相手平均展開予想データ_テン指数差`,
    race_horses.`展開予想データ_ペース指数` - competitors.`競争相手平均展開予想データ_ペース指数` as `競争相手平均展開予想データ_ペース指数差`,
    race_horses.`展開予想データ_上がり指数` - competitors.`競争相手平均展開予想データ_上がり指数` as `競争相手平均展開予想データ_上がり指数差`,
    race_horses.`展開予想データ_位置指数` - competitors.`競争相手平均展開予想データ_位置指数` as `競争相手平均展開予想データ_位置指数差`,
    race_horses.`展開予想データ_道中差` - competitors.`競争相手平均展開予想データ_道中差` as `競争相手平均展開予想データ_道中差差`,
    race_horses.`展開予想データ_後３Ｆ差` - competitors.`競争相手平均展開予想データ_後３Ｆ差` as `競争相手平均展開予想データ_後３Ｆ差差`,
    race_horses.`展開予想データ_ゴール差` - competitors.`競争相手平均展開予想データ_ゴール差` as `競争相手平均展開予想データ_ゴール差差`,
    race_horses.`騎手期待単勝率` - competitors.`競争相手平均騎手期待単勝率` as `競争相手平均騎手期待単勝率差`,
    race_horses.`騎手期待３着内率` - competitors.`競争相手平均騎手期待３着内率` as `競争相手平均騎手期待３着内率差`,
    race_horses.`展開参考データ_馬スタート指数` - competitors.`競争相手平均展開参考データ_馬スタート指数` as `競争相手平均展開参考データ_馬スタート指数差`,
    race_horses.`展開参考データ_馬出遅率` - competitors.`競争相手平均展開参考データ_馬出遅率` as `競争相手平均展開参考データ_馬出遅率差`,
    race_horses.`万券指数` - competitors.`競争相手平均万券指数` as `競争相手平均万券指数差`,
    race_horses.`入厩何日前` - competitors.`競争相手平均入厩何日前` as `競争相手平均入厩何日前差`,
    race_horses.`馬体重` - competitors.`競争相手平均馬体重` as `競争相手平均馬体重差`,
    race_horses.`馬体重増減` - competitors.`競争相手平均馬体重増減` as `競争相手平均馬体重増減差`,
    race_horses.`前走距離差` - competitors.`競争相手平均前走距離差` as `競争相手平均前走距離差差`,
    race_horses.`年齢` - competitors.`競争相手平均年齢` as `競争相手平均年齢差`,
    race_horses.`レース数` - competitors.`競争相手平均レース数` as `競争相手平均レース数差`,
    race_horses.`1位完走` - competitors.`競争相手平均1位完走` as `競争相手平均1位完走差`,
    race_horses.`トップ3完走` - competitors.`競争相手平均トップ3完走` as `競争相手平均トップ3完走差`,
    race_horses.`1位完走率` - competitors.`競争相手平均1位完走率` as `競争相手平均1位完走率差`,
    race_horses.`トップ3完走率` - competitors.`競争相手平均トップ3完走率` as `競争相手平均トップ3完走率差`,
    race_horses.`過去5走勝率` - competitors.`競争相手平均過去5走勝率` as `競争相手平均過去5走勝率差`,
    race_horses.`過去5走トップ3完走率` - competitors.`競争相手平均過去5走トップ3完走率` as `競争相手平均過去5走トップ3完走率差`,
    race_horses.`場所レース数` - competitors.`競争相手平均場所レース数` as `競争相手平均場所レース数差`,
    race_horses.`場所1位完走` - competitors.`競争相手平均場所1位完走` as `競争相手平均場所1位完走差`,
    race_horses.`場所トップ3完走` - competitors.`競争相手平均場所トップ3完走` as `競争相手平均場所トップ3完走差`,
    race_horses.`場所1位完走率` - competitors.`競争相手平均場所1位完走率` as `競争相手平均場所1位完走率差`,
    race_horses.`場所トップ3完走率` - competitors.`競争相手平均場所トップ3完走率` as `競争相手平均場所トップ3完走率差`,
    race_horses.`トラック種別レース数` - competitors.`競争相手平均トラック種別レース数` as `競争相手平均トラック種別レース数差`,
    race_horses.`トラック種別1位完走` - competitors.`競争相手平均トラック種別1位完走` as `競争相手平均トラック種別1位完走差`,
    race_horses.`トラック種別トップ3完走` - competitors.`競争相手平均トラック種別トップ3完走` as `競争相手平均トラック種別トップ3完走差`,
    race_horses.`馬場状態レース数` - competitors.`競争相手平均馬場状態レース数` as `競争相手平均馬場状態レース数差`,
    race_horses.`馬場状態1位完走` - competitors.`競争相手平均馬場状態1位完走` as `競争相手平均馬場状態1位完走差`,
    race_horses.`馬場状態トップ3完走` - competitors.`競争相手平均馬場状態トップ3完走` as `競争相手平均馬場状態トップ3完走差`,
    race_horses.`距離レース数` - competitors.`競争相手平均距離レース数` as `競争相手平均距離レース数差`,
    race_horses.`距離1位完走` - competitors.`競争相手平均距離1位完走` as `競争相手平均距離1位完走差`,
    race_horses.`距離トップ3完走` - competitors.`競争相手平均距離トップ3完走` as `競争相手平均距離トップ3完走差`,
    race_horses.`四半期レース数` - competitors.`競争相手平均四半期レース数` as `競争相手平均四半期レース数差`,
    race_horses.`四半期1位完走` - competitors.`競争相手平均四半期1位完走` as `競争相手平均四半期1位完走差`,
    race_horses.`四半期トップ3完走` - competitors.`競争相手平均四半期トップ3完走` as `競争相手平均四半期トップ3完走差`,
    race_horses.`過去3走順位平方和` - competitors.`競争相手平均過去3走順位平方和` as `競争相手平均過去3走順位平方和差`,
    race_horses.`本賞金累計` - competitors.`競争相手平均本賞金累計` as `競争相手平均本賞金累計差`,
    race_horses.`1位完走平均賞金` - competitors.`競争相手平均1位完走平均賞金` as `競争相手平均1位完走平均賞金差`,
    race_horses.`レース数平均賞金` - competitors.`競争相手平均レース数平均賞金` as `競争相手平均レース数平均賞金差`,
    race_horses.`連続1着` - competitors.`競争相手平均連続1着` as `競争相手平均連続1着差`,
    race_horses.`連続3着内` - competitors.`競争相手平均連続3着内` as `競争相手平均連続3着内差`

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
