{%
  set numeric_competitor_feature_names = [
      "負担重量",
      "馬体重",
      "馬体重増減",
      "1着馬体重変動率",
      "3着内馬体重変動率",
      "馬体重変動率",
      "一走前不利",
      "二走前不利",
      "三走前不利",
      "一走前着順",
      "二走前着順",
      "三走前着順",
      "四走前着順",
      "五走前着順",
      "六走前着順",
      "1走前上昇度",
      "2走前上昇度",
      "3走前上昇度",
      "4走前上昇度",
      "5走前上昇度",
      "騎手指数",
      "情報指数",
      "オッズ指数",
      "パドック指数",
      "総合指数",
      "ローテーション",
      "基準オッズ",
      "基準複勝オッズ",
      "人気指数",
      "調教指数",
      "厩舎指数",
      "騎手期待連対率",
      "激走指数",
      "展開予想データ_位置指数",
      "展開予想データ_道中差",
      "展開予想データ_後３Ｆ差",
      "展開予想データ_ゴール差",
      "騎手期待単勝率",
      "騎手期待３着内率",
      "展開参考データ_馬スタート指数",
      "展開参考データ_馬出遅率",
      "万券指数",
      "休養日数",
      "入厩何日前",
      "前走距離差",
      "年齢",
      "レース数",
      "1位完走",
      "トップ3完走",
      "1位完走率",
      "トップ3完走率",
      "過去5走1着率",
      "過去5走3着内率",
      "1走前1着タイム差",
      "1走前3着内タイム差",
      "重み付き1着率",
      "重み付き3着内率",
      "場所レース数",
      "場所1位完走",
      "場所トップ3完走",
      "場所1位完走率",
      "場所トップ3完走率",
      "トラック種別レース数",
      "トラック種別1位完走",
      "トラック種別トップ3完走",
      "馬場状態レース数",
      "馬場状態1位完走",
      "馬場状態トップ3完走",
      "距離レース数",
      "距離1位完走",
      "距離トップ3完走",
      "四半期レース数",
      "四半期1位完走",
      "四半期トップ3完走",
      "過去3走順位平方和",
      "本賞金累計",
      "1位完走平均賞金",
      "レース数平均賞金",
      "連続1着",
      "連続3着内",
      "1着ＩＤＭ変動率",
      "3着内ＩＤＭ変動率",
      "ＩＤＭ変動率",
      "事前ＩＤＭ",
      "事前単勝オッズ",
      "事前複勝オッズ",
      "事前テン指数",
      "事前ペース指数",
      "事前上がり指数",
      "1着騎手指数変動率",
      "3着内騎手指数変動率",
      "騎手指数変動率",
      "1着単勝オッズ変動率",
      "3着内単勝オッズ変動率",
      "単勝オッズ変動率",
      "1着複勝オッズ変動率",
      "3着内複勝オッズ変動率",
      "複勝オッズ変動率",
      "1着テン指数変動率",
      "3着内テン指数変動率",
      "テン指数変動率",
      "1着ペース指数変動率",
      "3着内ペース指数変動率",
      "ペース指数変動率",
      "1着上がり指数変動率",
      "3着内上がり指数変動率",
      "上がり指数変動率",
  ]
%}

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
    coalesce(tyb.`ＩＤＭ`, kyi.`ＩＤＭ`) as `事前ＩＤＭ`,
    run_style_codes_kyi.`name` as `事前脚質`,
    coalesce(tyb.`単勝オッズ`, win_odds.`単勝オッズ`) as `事前単勝オッズ`,
    coalesce(tyb.`複勝オッズ`, place_odds.`複勝オッズ`) as `事前複勝オッズ`,
    horse_form_codes_tyb.`name` as `事前馬体`,
    tyb.`気配コード` as `事前気配コード`,
    kyi.`上昇度` as `事前上昇度`,
    kyi.`クラスコード` as `事前クラスコード`,
    kyi.`展開予想データ_テン指数` as `事前テン指数`,
    kyi.`展開予想データ_ペース指数` as `事前ペース指数`,
    kyi.`展開予想データ_上がり指数` as `事前上がり指数`,

    -- General actual
    sed.`ＪＲＤＢデータ_ＩＤＭ` as `実績ＩＤＭ`,
    run_style_codes_sed.`name` as `実績脚質`,
    sed.`馬成績_確定単勝オッズ` as `実績単勝オッズ`,
    sed.`確定複勝オッズ下` as `実績複勝オッズ`,
    horse_form_codes_sed.`name` as `実績馬体`,
    sed.`ＪＲＤＢデータ_気配コード` as `実績気配コード`,
    sed.`ＪＲＤＢデータ_上昇度コード` as `実績上昇度`,
    sed.`ＪＲＤＢデータ_クラスコード` as `実績クラスコード`,
    sed.`ＪＲＤＢデータ_テン指数` as `実績テン指数`,
    sed.`ＪＲＤＢデータ_ペース指数` as `実績ペース指数`,
    sed.`ＪＲＤＢデータ_上がり指数` as `実績上がり指数`,

    -- General common
    coalesce(tyb.`負担重量`, kyi.`負担重量`) as `負担重量`,
    tyb.`馬体重` as `馬体重`,
    coalesce(
      avg(case when sed.`馬成績_着順` = 1 then tyb.`馬体重` end)
      over (partition by kyi.`血統登録番号` order by bac.`発走日時` rows between unbounded preceding and 1 preceding),
      tyb.`馬体重`
    ) as `1着平均馬体重`,
    coalesce(
      avg(case when sed.`馬成績_着順` <= 3 then tyb.`馬体重` end)
      over (partition by kyi.`血統登録番号` order by bac.`発走日時` rows between unbounded preceding and 1 preceding),
      tyb.`馬体重`
    ) as `3着内平均馬体重`,
    coalesce(
      avg(tyb.`馬体重`)
      over (partition by kyi.`血統登録番号` order by bac.`発走日時` rows between unbounded preceding and 1 preceding),
      tyb.`馬体重`
    ) as `平均馬体重`,
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
    case
      when bac.`レース条件_距離` <= 1400 then 'short'
      when bac.`レース条件_距離` <= 1800 then 'mid'
      else 'long'
    end as `距離区分`,
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
    lag(sed.`ＪＲＤＢデータ_上昇度コード`, 1) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `1走前上昇度`,
    lag(sed.`ＪＲＤＢデータ_上昇度コード`, 2) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `2走前上昇度`,
    lag(sed.`ＪＲＤＢデータ_上昇度コード`, 3) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `3走前上昇度`,
    lag(sed.`ＪＲＤＢデータ_上昇度コード`, 4) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `4走前上昇度`,
    lag(sed.`ＪＲＤＢデータ_上昇度コード`, 5) over (partition by kyi.`血統登録番号` order by bac.`発走日時`) as `5走前上昇度`,
    row_number() over (partition by kyi.`血統登録番号` order by bac.`発走日時`) - 1 as `レース数`,
    coalesce(
      cast(
        sum(case when sed.`馬成績_着順` = 1 then 1 else 0 end)
        over (partition by kyi.`血統登録番号` order by bac.`発走日時` rows between unbounded preceding and 1 preceding)
        as integer
      ),
    0) as `1位完走`,
    coalesce(
      cast(
        sum(case when sed.`馬成績_着順` <= 3 then 1 else 0 end)
        over (partition by kyi.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding)
        as integer
      ),
    0) as `トップ3完走`,

    -- Calculate Recent Winning Performance
    -- Recent Winning Percentage (RWP) = Recent Wins / Recent Races
    coalesce(
      cast(
        sum(case when sed.`馬成績_着順` = 1 then 1 else 0 end)
        over (partition by kyi.`血統登録番号` order by bac.`発走日時` rows between 5 preceding and 1 preceding)
        as integer
      ),
    0) / 5.0 as `過去5走1着率`,

    -- Calculate Recent Top 3 Performance
    -- Recent Top 3 Percentage (RTP) = Recent Top 3 / Recent
    coalesce(
      cast(
        sum(case when sed.`馬成績_着順` <= 3 then 1 else 0 end)
        over (partition by kyi.`血統登録番号` order by `発走日時` rows between 5 preceding and 1 preceding)
        as integer
      ),
    0) / 5.0 as `過去5走3着内率`,

    sed.`馬成績_タイム` as `タイム`,
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

  -- 同着があるためgroup byする
  race_1st_placers as (
  select
    `レースキー`,
    min(`タイム`) as `タイム`
  from
    race_horses_base
  where
    `着順` = 1
  group by
    `レースキー`
  ),
  race_3rd_placers as (
  select
    `レースキー`,
    min(`タイム`) as `タイム`
  from
    race_horses_base
  where
    `着順` = 3
  group by
    `レースキー`
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
    base.`馬場差` as `実績馬場差`,
    base.`着順` as `実績着順`,
    base.`本賞金` as `実績本賞金`,
    base.`異常区分`,
    base.`タイム` as `実績タイム`,

    -- General before race
    base.`事前ＩＤＭ`,
    base.`事前脚質`,
    base.`事前単勝オッズ`,
    base.`事前複勝オッズ`,
    base.`事前馬体`,
    base.`事前気配コード`,
    base.`事前上昇度`,
    base.`事前クラスコード`,
    base.`事前テン指数`,
    base.`事前ペース指数`,
    base.`事前上がり指数`,

    -- General actual
    base.`実績ＩＤＭ`,
    base.`実績脚質`,
    base.`実績単勝オッズ`,
    base.`実績複勝オッズ`,
    base.`実績馬体`,
    base.`実績気配コード`,
    base.`実績上昇度`,
    base.`実績クラスコード`,
    base.`実績テン指数`,
    base.`実績ペース指数`,
    base.`実績上がり指数`,

    -- General common
    base.`頭数`,
    lag(base.`頭数`, 1) over (partition by base.`血統登録番号` order by base.`発走日時`) as `1走前頭数`,
    lag(base.`頭数`, 2) over (partition by base.`血統登録番号` order by base.`発走日時`) as `2走前頭数`,
    lag(base.`頭数`, 3) over (partition by base.`血統登録番号` order by base.`発走日時`) as `3走前頭数`,
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
    base.`1走前上昇度`,
    base.`2走前上昇度`,
    base.`3走前上昇度`,
    base.`4走前上昇度`,
    base.`5走前上昇度`,
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
    lag(base.`休養理由分類コード`, 1) over (partition by base.`血統登録番号` order by base.`発走日時`) as `1走前休養理由分類コード`,
    lag(base.`休養理由分類コード`, 2) over (partition by base.`血統登録番号` order by base.`発走日時`) as `2走前休養理由分類コード`,
    lag(base.`休養理由分類コード`, 3) over (partition by base.`血統登録番号` order by base.`発走日時`) as `3走前休養理由分類コード`,
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
    -- distance category
    `距離区分`,

    -- diff_distance from previous race
    coalesce(`距離` - lag(`距離`) over (partition by base.`血統登録番号` order by `発走日時`), 0) as `前走距離差`,

    -- horse_age in years
    -- extract(year from age(`発走日時`, `生年月日`)) + extract(month from age(`発走日時`, `生年月日`)) / 12 + extract(day from age(`発走日時`, `生年月日`)) / (12 * 30.44) AS `年齢`,
    -- months_between('2024-01-01', '2022-12-31') = 12.0322 / 12 = 1.0027 years
    (months_between(`発走日時`, `生年月日`) / 12) as `年齢`,

    -- horse_age_4 or less
    -- age(`発走日時`, `生年月日`) < '5 years' as `4歳以下`,
    (months_between(`発走日時`, `生年月日`) / 12) < 5 as `4歳以下`,

    cast(
      sum(case when (months_between(`発走日時`, `生年月日`) / 12) < 5 then 1 else 0 end)
      over (partition by base.`レースキー`)
      as integer
    ) as `4歳以下頭数`,

    coalesce(
      sum(case when (months_between(`発走日時`, `生年月日`) / 12) < 5 then 1 else 0 end)
      over (partition by base.`レースキー`)
      / cast(`頭数` as double),
    0) as `4歳以下割合`,

    `レース数`, -- horse_runs
    `1位完走`, -- horse_wins
    `トップ3完走`, -- horse_placed
    `過去5走1着率`,
    `過去5走3着内率`,

    lag(race_1st_placers.`タイム` - base.`タイム`) over (partition by base.`血統登録番号` order by `発走日時`) as `1走前1着タイム差`,
    lag(race_3rd_placers.`タイム` - base.`タイム`) over (partition by base.`血統登録番号` order by `発走日時`) as `1走前3着内タイム差`,

    -- Compute the Composite Weighted Winning Percentage
    -- CWWP=(W_WP×WP)+(W_RWP​×RWP)
    -- where W_WP and W_RWP are the weights for the winning percentage and recent winning percentage, respectively.
    -- W_WP=0.4 and W_RWP=0.6
    0.4 * coalesce({{ dbt_utils.safe_divide('`1位完走`', '`レース数`') }}, 0) + 0.6 * `過去5走1着率` as `重み付き1着率`,
    0.4 * coalesce({{ dbt_utils.safe_divide('`トップ3完走`', '`レース数`') }}, 0) + 0.6 * `過去5走3着内率` as `重み付き3着内率`,

    -- ratio_win_horse
    coalesce({{ dbt_utils.safe_divide('`1位完走`', '`レース数`') }}, 0) as `1位完走率`,

    -- ratio_place_horse
    coalesce({{ dbt_utils.safe_divide('`トップ3完走`', '`レース数`') }}, 0) as `トップ3完走率`,

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
    coalesce(cast(count(*) over (partition by base.`血統登録番号`, `距離区分` order by `発走日時`) - 1 as integer), 0) as `距離レース数`,

    -- horse_distance_wins
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `距離区分` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `距離1位完走`,

    -- horse_distance_places
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `距離区分` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `距離トップ3完走`,

    -- ratio_win_horse_distance
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by base.`血統登録番号`, `距離区分` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `距離区分` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `距離1位完走率`,

    -- ratio_place_horse_distance
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by base.`血統登録番号`, `距離区分` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by base.`血統登録番号`, `距離区分` order by `発走日時`) - 1 as double)'
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
    coalesce(`馬体重` - `1着平均馬体重`, 0) as `1着平均馬体重差`,
    coalesce(`馬体重` - `3着内平均馬体重`, 0) as `3着内平均馬体重差`,

    -- https://nycdatascience.com/blog/student-works/data-analyzing-horse-racing/
    -- (current weight - winning weight) / winning weight
    coalesce((`馬体重` - `1着平均馬体重`) / `1着平均馬体重`, 0) as `1着馬体重変動率`,
    coalesce((`馬体重` - `3着内平均馬体重`) / `3着内平均馬体重`, 0) as `3着内馬体重変動率`,
    coalesce((`馬体重` - `平均馬体重`) / `平均馬体重`, 0) as `馬体重変動率`,

    -- Important: Using 事前 instead of 実績 when calculating the change
    -- 1着ＩＤＭ変動率
    coalesce(
      (`事前ＩＤＭ` - avg(case when `着順` = 1 then `実績ＩＤＭ` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` = 1 then `実績ＩＤＭ` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `1着ＩＤＭ変動率`,
    -- 3着内ＩＤＭ変動率
    coalesce(
      (`事前ＩＤＭ` - avg(case when `着順` <= 3 then `実績ＩＤＭ` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` <= 3 then `実績ＩＤＭ` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `3着内ＩＤＭ変動率`,
    -- ＩＤＭ変動率
    coalesce(
      (`事前ＩＤＭ` - avg(`実績ＩＤＭ`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(`実績ＩＤＭ`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `ＩＤＭ変動率`,

    -- 1着騎手指数変動率
    coalesce(
      (`騎手指数` - avg(case when `着順` = 1 then `騎手指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` = 1 then `騎手指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `1着騎手指数変動率`,
    -- 3着内騎手指数変動率
    coalesce(
      (`騎手指数` - avg(case when `着順` <= 3 then `騎手指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` <= 3 then `騎手指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `3着内騎手指数変動率`,
    -- 騎手指数変動率
    coalesce(
      (`騎手指数` - avg(`騎手指数`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(`騎手指数`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `騎手指数変動率`,

    -- 1着単勝オッズ変動率
    coalesce(
      (`事前単勝オッズ` - avg(case when `着順` = 1 then `実績単勝オッズ` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` = 1 then `実績単勝オッズ` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `1着単勝オッズ変動率`,
    -- 3着内単勝オッズ変動率
    coalesce(
      (`事前単勝オッズ` - avg(case when `着順` <= 3 then `実績単勝オッズ` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` <= 3 then `実績単勝オッズ` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `3着内単勝オッズ変動率`,
    -- 単勝オッズ変動率
    coalesce(
      (`事前単勝オッズ` - avg(`実績単勝オッズ`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(`実績単勝オッズ`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `単勝オッズ変動率`,

    -- 1着複勝オッズ変動率
    coalesce(
      (`事前複勝オッズ` - avg(case when `着順` = 1 then `実績複勝オッズ` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` = 1 then `実績複勝オッズ` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `1着複勝オッズ変動率`,
    -- 3着内複勝オッズ変動率
    coalesce(
      (`事前複勝オッズ` - avg(case when `着順` <= 3 then `実績複勝オッズ` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` <= 3 then `実績複勝オッズ` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `3着内複勝オッズ変動率`,
    -- 複勝オッズ変動率
    coalesce(
      (`事前複勝オッズ` - avg(`実績複勝オッズ`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(`実績複勝オッズ`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `複勝オッズ変動率`,

    -- 1着テン指数変動率
    coalesce(
      (`事前テン指数` - avg(case when `着順` = 1 then `実績テン指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` = 1 then `実績テン指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `1着テン指数変動率`,
    -- 3着内テン指数変動率
    coalesce(
      (`事前テン指数` - avg(case when `着順` <= 3 then `実績テン指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` <= 3 then `実績テン指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `3着内テン指数変動率`,
    -- テン指数変動率
    coalesce(
      (`事前テン指数` - avg(`実績テン指数`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(`実績テン指数`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `テン指数変動率`,

    -- 1着ペース指数変動率
    coalesce(
      (`事前ペース指数` - avg(case when `着順` = 1 then `実績ペース指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` = 1 then `実績ペース指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `1着ペース指数変動率`,
    -- 3着内ペース指数変動率
    coalesce(
      (`事前ペース指数` - avg(case when `着順` <= 3 then `実績ペース指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` <= 3 then `実績ペース指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `3着内ペース指数変動率`,
    -- ペース指数変動率
    coalesce(
      (`事前ペース指数` - avg(`実績ペース指数`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(`実績ペース指数`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `ペース指数変動率`,

    -- 1着上がり指数変動率
    coalesce(
      (`事前上がり指数` - avg(case when `着順` = 1 then `実績上がり指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` = 1 then `実績上がり指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `1着上がり指数変動率`,
    -- 3着内上がり指数変動率
    coalesce(
      (`事前上がり指数` - avg(case when `着順` <= 3 then `実績上がり指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(case when `着順` <= 3 then `実績上がり指数` end) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `3着内上がり指数変動率`,
    -- 上がり指数変動率
    coalesce(
      (`事前上がり指数` - avg(`実績上がり指数`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding))
      / avg(`実績上がり指数`) over (partition by base.`血統登録番号` order by `発走日時` rows between unbounded preceding and 1 preceding),
    0) as `上がり指数変動率`

  from
    race_horses_base base
  left join
    race_horses_streaks
  on
    race_horses_streaks.`レースキー` = base.`レースキー`
    and race_horses_streaks.`馬番` = base.`馬番`
  left join
    race_1st_placers
  on
    race_1st_placers.`レースキー` = base.`レースキー`
  left join
    race_3rd_placers
  on
    race_3rd_placers.`レースキー` = base.`レースキー`
  ),

  competitors as (
  select
    a.`レースキー`,
    a.`馬番`,

    {%- for feature_name in numeric_competitor_feature_names %}
    max(b.`{{ feature_name }}`) as `競争相手最高{{ feature_name }}`,
    min(b.`{{ feature_name }}`) as `競争相手最低{{ feature_name }}`,
    avg(b.`{{ feature_name }}`) as `競争相手平均{{ feature_name }}`,
    stddev_pop(b.`{{ feature_name }}`) as `競争相手{{ feature_name }}標準偏差`,
    {% endfor %}

    sum(case when b.`性別` = '牡' then 1 else 0 end) / cast(count(*) as double) as `競争相手性別牡割合`,
    sum(case when b.`性別` = '牝' then 1 else 0 end) / cast(count(*) as double) as `競争相手性別牝割合`,
    sum(case when b.`性別` = 'セン' then 1 else 0 end) / cast(count(*) as double) as `競争相手性別セ割合`,
    sum(case when b.`トラック種別瞬発戦好走馬` then 1 else 0 end) / cast(count(*) as double) as `競争相手トラック種別瞬発戦好走馬割合`,
    sum(case when b.`トラック種別消耗戦好走馬` then 1 else 0 end) / cast(count(*) as double) as `競争相手トラック種別消耗戦好走馬割合`,
    sum(case when b.`前走トップ3` then 1 else 0 end) / cast(count(*) as double) as `競争相手前走トップ3割合`
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
    race_horses.`事前ＩＤＭ` as `num_事前ＩＤＭ`,
    race_horses.`事前脚質` as `cat_事前脚質`,
    race_horses.`事前単勝オッズ` as `num_事前単勝オッズ`,
    race_horses.`事前複勝オッズ` as `num_事前複勝オッズ`,
    race_horses.`事前馬体` as `cat_事前馬体`,
    race_horses.`事前気配コード` as `cat_事前気配コード`,
    race_horses.`事前上昇度` as `cat_事前上昇度`,
    race_horses.`事前クラスコード` as `cat_事前クラスコード`, -- you need to understand what this means more
    race_horses.`事前テン指数` as `num_事前テン指数`,
    race_horses.`事前ペース指数` as `num_事前ペース指数`,
    race_horses.`事前上がり指数` as `num_事前上がり指数`,

    -- General actual
    -- race_horses.`実績ＩＤＭ` as `num_実績ＩＤＭ`,
    -- race_horses.`実績脚質` as `cat_実績脚質`,
    -- race_horses.`実績単勝オッズ` as `num_実績単勝オッズ`,
    race_horses.`実績複勝オッズ` as `num_実績複勝オッズ`,
    -- race_horses.`実績馬体` as `cat_実績馬体`,
    -- race_horses.`実績気配コード` as `cat_実績気配コード`,
    -- race_horses.`実績上昇度` as `cat_実績上昇度`,
    -- race_horses.`実績クラスコード` as `cat_実績クラスコード`,
    -- race_horses.`実績テン指数` as `num_実績テン指数`,
    -- race_horses.`実績ペース指数` as `num_実績ペース指数`,
    -- race_horses.`実績上がり指数` as `num_実績上がり指数`,

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
    race_horses.`1走前上昇度` as `num_1走前上昇度`,
    race_horses.`2走前上昇度` as `num_2走前上昇度`,
    race_horses.`3走前上昇度` as `num_3走前上昇度`,
    race_horses.`4走前上昇度` as `num_4走前上昇度`,
    race_horses.`5走前上昇度` as `num_5走前上昇度`,
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
    race_horses.`1走前休養理由分類コード` as `cat_1走前休養理由分類コード`,
    race_horses.`2走前休養理由分類コード` as `cat_2走前休養理由分類コード`,
    race_horses.`3走前休養理由分類コード` as `cat_3走前休養理由分類コード`,
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
    race_horses.`頭数` as `num_頭数`, -- only 4 records where before/actual diff exists (and only by 1)
    race_horses.`1走前頭数` as `num_1走前頭数`,
    race_horses.`2走前頭数` as `num_2走前頭数`,
    race_horses.`3走前頭数` as `num_3走前頭数`,
    race_horses.`レース数` as `num_レース数`, -- horse_runs
    race_horses.`1位完走` as `num_1位完走`, -- horse_wins
    race_horses.`トップ3完走` as `num_トップ3完走`, -- horse_places
    race_horses.`1位完走率` as `num_1位完走率`,
    race_horses.`トップ3完走率` as `num_トップ3完走率`,
    race_horses.`過去5走1着率` as `num_過去5走1着率`,
    race_horses.`過去5走3着内率` as `num_過去5走3着内率`,
    race_horses.`1走前1着タイム差` as `num_1走前1着タイム差`,
    race_horses.`1走前3着内タイム差` as `num_1走前3着内タイム差`,
    race_horses.`重み付き1着率` as `num_重み付き1着率`,
    race_horses.`重み付き3着内率` as `num_重み付き3着内率`,
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
    race_horses.`1着馬体重変動率` as `num_1着馬体重変動率`,
    race_horses.`3着内馬体重変動率` as `num_3着内馬体重変動率`,
    race_horses.`馬体重変動率` as `num_馬体重変動率`,
    race_horses.`1着ＩＤＭ変動率` as `num_1着ＩＤＭ変動率`,
    race_horses.`3着内ＩＤＭ変動率` as `num_3着内ＩＤＭ変動率`,
    race_horses.`ＩＤＭ変動率` as `num_ＩＤＭ変動率`,
    race_horses.`1着騎手指数変動率` as `num_1着騎手指数変動率`,
    race_horses.`3着内騎手指数変動率` as `num_3着内騎手指数変動率`,
    race_horses.`騎手指数変動率` as `num_騎手指数変動率`,
    race_horses.`1着単勝オッズ変動率` as `num_1着単勝オッズ変動率`,
    race_horses.`3着内単勝オッズ変動率` as `num_3着内単勝オッズ変動率`,
    race_horses.`単勝オッズ変動率` as `num_単勝オッズ変動率`,
    race_horses.`1着複勝オッズ変動率` as `num_1着複勝オッズ変動率`,
    race_horses.`3着内複勝オッズ変動率` as `num_3着内複勝オッズ変動率`,
    race_horses.`複勝オッズ変動率` as `num_複勝オッズ変動率`,
    race_horses.`1着テン指数変動率` as `num_1着テン指数変動率`,
    race_horses.`3着内テン指数変動率` as `num_3着内テン指数変動率`,
    race_horses.`テン指数変動率` as `num_テン指数変動率`,
    race_horses.`1着ペース指数変動率` as `num_1着ペース指数変動率`,
    race_horses.`3着内ペース指数変動率` as `num_3着内ペース指数変動率`,
    race_horses.`ペース指数変動率` as `num_ペース指数変動率`,
    race_horses.`1着上がり指数変動率` as `num_1着上がり指数変動率`,
    race_horses.`3着内上がり指数変動率` as `num_3着内上がり指数変動率`,
    race_horses.`上がり指数変動率` as `num_上がり指数変動率`,

    -- Competitors
    {%- for feature_name in numeric_competitor_feature_names %}
    `競争相手最高{{ feature_name }}` as `num_競争相手最高{{ feature_name }}`,
    `競争相手最低{{ feature_name }}` as `num_競争相手最低{{ feature_name }}`,
    `競争相手平均{{ feature_name }}` as `num_競争相手平均{{ feature_name }}`,
    `競争相手{{ feature_name }}標準偏差` as `num_競争相手{{ feature_name }}標準偏差`,
    {% endfor %}

    competitors.`競争相手性別牡割合` as `num_競争相手性別牡割合`,
    competitors.`競争相手性別牝割合` as `num_競争相手性別牝割合`,
    competitors.`競争相手性別セ割合` as `num_競争相手性別セ割合`,
    competitors.`競争相手前走トップ3割合` as `num_競争相手前走トップ3割合`,
    competitors.`競争相手トラック種別瞬発戦好走馬割合` as `num_競争相手トラック種別瞬発戦好走馬割合`,
    competitors.`競争相手トラック種別消耗戦好走馬割合` as `num_競争相手トラック種別消耗戦好走馬割合`,

    -- Relative common
    {%- for feature_name in numeric_competitor_feature_names %}
    race_horses.`{{ feature_name }}` - competitors.`競争相手平均{{ feature_name }}` as `num_競争相手平均{{ feature_name }}差`
    {%- if not loop.last %},{% endif -%}
    {% endfor %}

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
