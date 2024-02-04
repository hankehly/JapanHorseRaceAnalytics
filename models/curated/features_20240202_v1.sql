{{
  config(
    materialized='table'
  )
}}

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

  race_weather as (
  select
    *
  from
    {{ ref('int_race_weather') }}
  ),

  win_odds as (
  select
    *
  from
    {{ ref('int_win_odds') }}
  ),

  place_odds as (
  select
    *
  from
    {{ ref('int_place_odds') }}
  ),

  win_payouts as (
  select
    *
  from
    {{ ref('int_win_payouts') }}
  ),

  place_payouts as (
  select
      *
  from
      {{ ref('int_place_payouts') }}
  ),

  base as (
  select
    kyi."レースキー",
    kyi."馬番",
    kyi."枠番",
    bac."年月日",
    bac."発走時間",
    kyi."レースキー_場コード" as "場コード",
    coalesce(tyb."騎手コード", kyi."騎手コード") as "騎手コード",
    kyi."調教師コード",
    kyi."レースキー_Ｒ",
    case
      when extract(month from bac."年月日") <= 3 then 1
      when extract(month from bac."年月日") <= 6 then 2
      when extract(month from bac."年月日") <= 9 then 3
      when extract(month from bac."年月日") <= 12 then 4
    end as "四半期",
    kyi."血統登録番号",
    kyi."入厩年月日",
    -- Assumption: TYB is available (~15 minutes before race)
    tyb."馬体重",
    tyb."馬体重増減",
    bac."レース条件_距離" as "距離",
    coalesce(
      tyb."馬場状態コード",
      case
        -- 障害コースの場合は芝馬場状態を使用する
        when bac."レース条件_トラック情報_芝ダ障害コード" = 'ダート' then kab."ダ馬場状態コード"
        else kab."芝馬場状態コード"
      end
    ) as "馬場状態コード",
    bac."レース条件_トラック情報_右左",
    bac."レース条件_トラック情報_内外",
    bac."レース条件_種別",
    bac."レース条件_条件",
    bac."レース条件_記号",
    bac."レース条件_重量",
    bac."レース条件_グレード",
    sed."本賞金",
    bac."頭数",
    bac."レース条件_トラック情報_芝ダ障害コード" as "トラック種別",
    sed."馬成績_着順" as "着順",
    -- 障害コースの場合は芝馬場差を使用する（一応）
    case
      when bac."レース条件_トラック情報_芝ダ障害コード" = 'ダート' then kab."ダ馬場差"
      else kab."芝馬場差"
    end "馬場差",
    kab."芝馬場状態内",
    kab."芝馬場状態中",
    kab."芝馬場状態外",
    kab."直線馬場差最内",
    kab."直線馬場差内",
    kab."直線馬場差中",
    kab."直線馬場差外",
    kab."直線馬場差大外",
    kab."ダ馬場状態内",
    kab."ダ馬場状態中",
    kab."ダ馬場状態外",
    kab."芝種類",
    kab."草丈",
    kab."転圧",
    kab."凍結防止剤",
    kab."中間降水量",
    coalesce(tyb."ＩＤＭ", kyi."ＩＤＭ") as "ＩＤＭ",
    -- https://note.com/jrdbn/n/n0b3d06e39768
    coalesce(stddev_pop(coalesce(tyb."ＩＤＭ", kyi."ＩＤＭ")) over (partition by kyi."レースキー"), 0) as "IDM標準偏差",
    (SELECT "name" FROM {{ ref('脚質コード') }} WHERE "code" = kyi."脚質") as "脚質",
    coalesce(tyb."単勝オッズ", win_odds."単勝オッズ") as "単勝オッズ",
    coalesce(tyb."複勝オッズ", place_odds."複勝オッズ") as "複勝オッズ",
    coalesce(tyb."騎手指数", kyi."騎手指数") as "騎手指数",
    coalesce(tyb."情報指数", kyi."情報指数") as "情報指数",
    tyb."オッズ指数",
    tyb."パドック指数",
    coalesce(tyb."総合指数", kyi."総合指数") as "総合指数",
    tyb."馬具変更情報",
    tyb."脚元情報",
    coalesce(tyb."負担重量", kyi."負担重量") as "負担重量",
    coalesce(tyb."見習い区分", kyi."見習い区分") as "見習い区分",
    tyb."オッズ印",
    tyb."パドック印",
    tyb."直前総合印",
    (SELECT "name" FROM {{ ref("馬体コード") }} WHERE "code" = tyb."馬体コード") as "馬体",
    tyb."気配コード",
    kyi."距離適性",
    kyi."上昇度",
    kyi."ローテーション",
    kyi."基準オッズ",
    kyi."基準人気順位",
    kyi."基準複勝オッズ",
    kyi."基準複勝人気順位",
    kyi."特定情報◎",
    kyi."特定情報○",
    kyi."特定情報▲",
    kyi."特定情報△",
    kyi."特定情報×",
    kyi."総合情報◎",
    kyi."総合情報○",
    kyi."総合情報▲",
    kyi."総合情報△",
    kyi."総合情報×",
    kyi."人気指数",
    kyi."調教指数",
    kyi."厩舎指数",
    kyi."調教矢印コード",
    kyi."厩舎評価コード",
    kyi."騎手期待連対率",
    kyi."激走指数",
    kyi."蹄コード",
    kyi."重適性コード",
    kyi."クラスコード",
    kyi."ブリンカー",
    kyi."印コード_総合印",
    kyi."印コード_ＩＤＭ印",
    kyi."印コード_情報印",
    kyi."印コード_騎手印",
    kyi."印コード_厩舎印",
    kyi."印コード_調教印",
    kyi."印コード_激走印",
    kyi."展開予想データ_テン指数",
    kyi."展開予想データ_ペース指数",
    kyi."展開予想データ_上がり指数",
    kyi."展開予想データ_位置指数",
    kyi."展開予想データ_ペース予想",
    kyi."展開予想データ_道中順位",
    kyi."展開予想データ_道中差",
    kyi."展開予想データ_道中内外",
    kyi."展開予想データ_後３Ｆ順位",
    kyi."展開予想データ_後３Ｆ差",
    kyi."展開予想データ_後３Ｆ内外",
    kyi."展開予想データ_ゴール順位",
    kyi."展開予想データ_ゴール差",
    kyi."展開予想データ_ゴール内外",
    kyi."展開予想データ_展開記号",
    kyi."激走順位",
    kyi."LS指数順位",
    kyi."テン指数順位",
    kyi."ペース指数順位",
    kyi."上がり指数順位",
    kyi."位置指数順位",
    kyi."騎手期待単勝率",
    kyi."騎手期待３着内率",
    kyi."輸送区分",
    kyi."体型_全体",
    kyi."体型_背中",
    kyi."体型_胴",
    kyi."体型_尻",
    kyi."体型_トモ",
    kyi."体型_腹袋",
    kyi."体型_頭",
    kyi."体型_首",
    kyi."体型_胸",
    kyi."体型_肩",
    kyi."体型_前長",
    kyi."体型_後長",
    kyi."体型_前幅",
    kyi."体型_後幅",
    kyi."体型_前繋",
    kyi."体型_後繋",
    kyi."体型総合１",
    kyi."体型総合２",
    kyi."体型総合３",
    kyi."馬特記１",
    kyi."馬特記２",
    kyi."馬特記３",
    kyi."展開参考データ_馬スタート指数",
    kyi."展開参考データ_馬出遅率",
    kyi."万券指数",
    kyi."万券印",
    kyi."激走タイプ",
    kyi."休養理由分類コード",
    kyi."芝ダ障害フラグ",
    kyi."距離フラグ",
    kyi."クラスフラグ",
    kyi."転厩フラグ",
    kyi."去勢フラグ",
    kyi."乗替フラグ",
    kyi."放牧先ランク",
    kyi."厩舎ランク",
    coalesce(tyb."天候コード", kab."天候コード") as "天候コード",
    coalesce(win_payouts."払戻金", 0) > 0 as "単勝的中",
    coalesce(win_payouts."払戻金", 0) as "単勝払戻金",
    coalesce(place_payouts."払戻金", 0) > 0 as "複勝的中",
    coalesce(place_payouts."払戻金", 0) as "複勝払戻金"
  from
    kyi

  -- 前日系は inner join
  inner join
    bac
  on
    kyi."レースキー" = bac."レースキー"

  -- 実績系はレースキーがないかもしれないから left join
  left join
    sed
  on
    kyi."レースキー" = sed."レースキー"
    and kyi."馬番" = sed."馬番"

  -- TYBが公開される前に予測する可能性があるから left join
  left join
    tyb
  on
    kyi."レースキー" = tyb."レースキー"
    and kyi."馬番" = tyb."馬番"

  inner join
    kab
  on
    kyi."開催キー" = kab."開催キー"
    and bac."年月日" = kab."年月日"

  inner join
    win_odds
  on
    kyi."レースキー" = win_odds."レースキー"
    and kyi."馬番" = win_odds."馬番"

  inner join
    place_odds
  on
    kyi."レースキー" = place_odds."レースキー"
    and kyi."馬番" = place_odds."馬番"

  left join
    win_payouts
  on
    kyi."レースキー" = win_payouts."レースキー"
    and kyi."馬番" = win_payouts."馬番"

  left join
    place_payouts
  on
    kyi."レースキー" = place_payouts."レースキー"
    and kyi."馬番" = place_payouts."馬番"
  ),

  combined_features as (
  select
    base."レースキー",
    base."馬番",

    -- runs_horse_jockey
    coalesce(cast(count(*) over (partition by "血統登録番号", "騎手コード" order by "年月日", "レースキー_Ｒ") - 1 as integer), 0) as "馬騎手レース数",

    -- wins_horse_jockey
    coalesce(cast(sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "騎手コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding) as integer), 0) as "馬騎手1位完走",

    -- ratio_win_horse_jockey
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "騎手コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by "血統登録番号", "騎手コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }}, 0) as "馬騎手1位完走率",

    -- places_horse_jockey
    coalesce(cast(sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "騎手コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding) as integer), 0) as "馬騎手トップ3完走",

    -- ratio_place_horse_jockey
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "騎手コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by "血統登録番号", "騎手コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }}, 0) as "馬騎手トップ3完走率",

    -- first_second_jockey
    case when cast(count(*) over (partition by "血統登録番号", "騎手コード" order by "年月日", "レースキー_Ｒ") - 1 as integer) < 2 then true else false end as "馬騎手初二走",

    -- same_last_jockey (horse jockey combination was same last race)
    case when lag("騎手コード") over (partition by "血統登録番号" order by "年月日", "レースキー_Ｒ") = "騎手コード" then true else false end as "馬騎手同騎手",

    -- runs_horse_jockey_venue
    coalesce(cast(count(*) over (partition by "血統登録番号", "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ") - 1 as integer), 0) as "馬騎手場所レース数",

    -- wins_horse_jockey_venue
    coalesce(cast(sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding) as integer), 0) as "馬騎手場所1位完走",

    -- ratio_win_horse_jockey_venue
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by "血統登録番号", "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }}, 0) as "馬騎手場所1位完走率",

    -- places_horse_jockey_venue
    coalesce(cast(sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding) as integer), 0) as "馬騎手場所トップ3完走",

    -- ratio_place_horse_jockey_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by "血統登録番号", "騎手コード", "場コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }}, 0) as "馬騎手場所トップ3完走率",

    -- runs_horse_trainer
    coalesce(cast(count(*) over (partition by "血統登録番号", "調教師コード" order by "年月日", "レースキー_Ｒ") - 1 as integer), 0) as "馬調教師レース数",

    -- wins_horse_trainer
    coalesce(cast(sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "調教師コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding) as integer), 0) as "馬調教師1位完走",

    -- ratio_win_horse_trainer
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "調教師コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by "血統登録番号", "調教師コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }}, 0) as "馬調教師1位完走率",

    -- places_horse_trainer
    coalesce(cast(sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "調教師コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding) as integer), 0) as "馬調教師トップ3完走",

    -- ratio_place_horse_trainer
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "調教師コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by "血統登録番号", "調教師コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }}, 0) as "馬調教師トップ3完走率",

    -- first_second_trainer
    case when cast(count(*) over (partition by "血統登録番号", "調教師コード" order by "年月日", "レースキー_Ｒ") - 1 as integer) < 2 then true else false end as "馬調教師初二走",

    -- same_last_trainer
    case when lag("調教師コード") over (partition by "血統登録番号" order by "年月日", "レースキー_Ｒ") = "調教師コード" then true else false end as "馬調教師同調教師",

    -- runs_horse_trainer_venue
    coalesce(cast(count(*) over (partition by "血統登録番号", "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ") - 1 as integer), 0) as "馬調教師場所レース数",

    -- wins_horse_trainer_venue
    coalesce(cast(sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding) as integer), 0) as "馬調教師場所1位完走",

    -- ratio_win_horse_trainer_venue
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when "着順" = 1 then 1 else 0 end) over (partition by "血統登録番号", "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by "血統登録番号", "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }}, 0) as "馬調教師場所1位完走率",

    -- places_horse_trainer_venue
    coalesce(cast(sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding) as integer), 0) as "馬調教師場所トップ3完走",

    -- ratio_place_horse_trainer_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when "着順" <= 3 then 1 else 0 end) over (partition by "血統登録番号", "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ" rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by "血統登録番号", "調教師コード", "場コード" order by "年月日", "レースキー_Ｒ") - 1 as numeric)'
      )
    }}, 0) as "馬調教師場所トップ3完走率"
  from
    base
  ),

  -- Features with >= 700 importance in both turf/dirt models
  -- IDM
  -- place_streak
  -- win_streak
  -- 入厩何日前
  -- 馬体重
  -- 展開予想データ_テン指数
  -- 展開予想データ_ペース指数
  -- 展開予想データ_上がり指数
  -- 展開予想データ_位置指数
  -- 展開参考データ_馬出遅率
  -- 万券指数
  -- 調教指数
  -- 厩舎指数
  -- 激走指数
  -- 人気指数
  -- 単勝オッズ
  -- 複勝オッズ
  -- 基準オッズ
  -- 基準複勝オッズ
  -- 年齢
  -- 調教師1位完走平均賞金
  -- 調教師場所1位完走率
  -- 騎手1位完走率
  -- 騎手距離1位完走率
  -- 調教師場所トップ3完走率
  -- 調教師レース数
  -- 騎手期待３着内率
  -- 騎手1位完走平均賞金
  -- 騎手距離レース数
  -- 騎手距離トップ3完走率
  -- 調教師レース数平均賞金
  -- 過去3走順位平方和
  -- 調教師トップ3完走率
  -- 騎手場所1位完走率
  -- 騎手期待単勝率
  -- 調教師場所レース数

  -- Calculate mean, max, min, stddev of competitor features
  competitors_base as (
  select
    a."レースキー",
    a."馬番",

    -- ＩＤＭ
    max(b."ＩＤＭ") as "競争相手最高IDM",
    min(b."ＩＤＭ") as "競争相手最低IDM",
    avg(b."ＩＤＭ") as "競争相手平均IDM",
    stddev_pop(b."ＩＤＭ") as "競争相手IDM標準偏差",
    -- a."ＩＤＭ" - avg(b."ＩＤＭ") as "競争相手平均IDM差",

    -- 脚質
    sum(case when b."脚質" = '逃げ' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手脚質逃げ割合",
    sum(case when b."脚質" = '先行' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手脚質先行割合",
    sum(case when b."脚質" = '差し' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手脚質差し割合",
    sum(case when b."脚質" = '追込' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手脚質追込割合",
    sum(case when b."脚質" = '好位差し' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手脚質好位差し割合",
    sum(case when b."脚質" = '自在' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手脚質自在割合",

    -- "単勝オッズ"
    max(b."単勝オッズ") as "競争相手最高単勝オッズ",
    min(b."単勝オッズ") as "競争相手最低単勝オッズ",
    avg(b."単勝オッズ") as "競争相手平均単勝オッズ",
    stddev_pop(b."単勝オッズ") as "競争相手単勝オッズ標準偏差",
    -- a."単勝オッズ" - avg(b."単勝オッズ") as "競争相手平均単勝オッズ差",

    -- "複勝オッズ"
    max(b."複勝オッズ") as "競争相手最高複勝オッズ",
    min(b."複勝オッズ") as "競争相手最低複勝オッズ",
    avg(b."複勝オッズ") as "競争相手平均複勝オッズ",
    stddev_pop(b."複勝オッズ") as "競争相手複勝オッズ標準偏差",
    -- a."複勝オッズ" - avg(b."複勝オッズ") as "競争相手平均複勝オッズ差",

    -- "騎手指数"
    max(b."騎手指数") as "競争相手最高騎手指数",
    min(b."騎手指数") as "競争相手最低騎手指数",
    avg(b."騎手指数") as "競争相手平均騎手指数",
    stddev_pop(b."騎手指数") as "競争相手騎手指数標準偏差",
    -- a."騎手指数" - avg(b."騎手指数") as "競争相手平均騎手指数差",

    -- "情報指数"
    max(b."情報指数") as "競争相手最高情報指数",
    min(b."情報指数") as "競争相手最低情報指数",
    avg(b."情報指数") as "競争相手平均情報指数",
    stddev_pop(b."情報指数") as "競争相手情報指数標準偏差",
    -- a."情報指数" - avg(b."情報指数") as "競争相手平均情報指数差",

    -- "オッズ指数"
    max(b."オッズ指数") as "競争相手最高オッズ指数",
    min(b."オッズ指数") as "競争相手最低オッズ指数",
    avg(b."オッズ指数") as "競争相手平均オッズ指数",
    stddev_pop(b."オッズ指数") as "競争相手オッズ指数標準偏差",
    -- a."オッズ指数" - avg(b."オッズ指数") as "競争相手平均オッズ指数差",

    -- "パドック指数"
    max(b."パドック指数") as "競争相手最高パドック指数",
    min(b."パドック指数") as "競争相手最低パドック指数",
    avg(b."パドック指数") as "競争相手平均パドック指数",
    stddev_pop(b."パドック指数") as "競争相手パドック指数標準偏差",
    -- a."パドック指数" - avg(b."パドック指数") as "競争相手平均パドック指数差",

    -- "総合指数"
    max(b."総合指数") as "競争相手最高総合指数",
    min(b."総合指数") as "競争相手最低総合指数",
    avg(b."総合指数") as "競争相手平均総合指数",
    stddev_pop(b."総合指数") as "競争相手総合指数標準偏差",
    -- a."総合指数" - avg(b."総合指数") as "競争相手平均総合指数差",

    -- "馬具変更情報"
    sum(case when b."馬具変更情報" = '0' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手馬具変更情報0割合",
    sum(case when b."馬具変更情報" = '1' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手馬具変更情報1割合",
    sum(case when b."馬具変更情報" = '2' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手馬具変更情報2割合",

    -- "脚元情報" (無視)

    -- "負担重量"
    max(b."負担重量") as "競争相手最高負担重量",
    min(b."負担重量") as "競争相手最低負担重量",
    avg(b."負担重量") as "競争相手平均負担重量",
    stddev_pop(b."負担重量") as "競争相手負担重量標準偏差",
    -- a."負担重量" - avg(b."負担重量") as "競争相手平均負担重量差",

    -- "見習い区分" (ordinal, treat as integer)
    max(cast(b."見習い区分" as integer)) as "競争相手最高見習い区分",
    min(cast(b."見習い区分" as integer)) as "競争相手最低見習い区分",
    avg(cast(b."見習い区分" as integer)) as "競争相手平均見習い区分",
    stddev_pop(cast(b."見習い区分" as integer)) as "競争相手見習い区分標準偏差",
    -- cast(a."見習い区分" as numeric) - avg(cast(b."見習い区分" as integer)) as "競争相手平均見習い区分差",

    -- "オッズ印"
    sum(case when b."オッズ印" = '1' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手オッズ印1割合",
    sum(case when b."オッズ印" = '2' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手オッズ印2割合",
    sum(case when b."オッズ印" = '3' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手オッズ印3割合",
    sum(case when b."オッズ印" = '4' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手オッズ印4割合",

    -- "パドック印"
    sum(case when b."パドック印" = '1' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手パドック印1割合",
    sum(case when b."パドック印" = '2' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手パドック印2割合",
    sum(case when b."パドック印" = '3' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手パドック印3割合",
    sum(case when b."パドック印" = '4' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手パドック印4割合",

    -- "直前総合印"
    sum(case when b."直前総合印" = '1' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手直前総合印1割合",
    sum(case when b."直前総合印" = '2' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手直前総合印2割合",
    sum(case when b."直前総合印" = '3' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手直前総合印3割合",
    sum(case when b."直前総合印" = '4' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手直前総合印4割合",

    -- "馬体" (無視)
    -- "気配コード" (無視)

    -- "距離適性"
    sum(case when b."距離適性" = '1' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手距離適性1割合",
    sum(case when b."距離適性" = '2' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手距離適性2割合",
    sum(case when b."距離適性" = '3' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手距離適性3割合",
    sum(case when b."距離適性" = '5' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手距離適性5割合",
    sum(case when b."距離適性" = '6' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手距離適性6割合",

    -- "上昇度" (ordinal, treat as integer)
    max(cast(b."上昇度" as integer)) as "競争相手最高上昇度",
    min(cast(b."上昇度" as integer)) as "競争相手最低上昇度",
    avg(cast(b."上昇度" as integer)) as "競争相手平均上昇度",
    stddev_pop(cast(b."上昇度" as integer)) as "競争相手上昇度標準偏差",
    -- cast(a."上昇度" as numeric) - avg(cast(b."上昇度" as integer)) as "競争相手平均上昇度差",

    -- "ローテーション"
    max(b."ローテーション") as "競争相手最高ローテーション",
    min(b."ローテーション") as "競争相手最低ローテーション",
    avg(b."ローテーション") as "競争相手平均ローテーション",
    stddev_pop(b."ローテーション") as "競争相手ローテーション標準偏差",
    -- a."ローテーション" - avg(b."ローテーション") as "競争相手平均ローテーション差",

    -- "基準オッズ"
    max(b."基準オッズ") as "競争相手最高基準オッズ",
    min(b."基準オッズ") as "競争相手最低基準オッズ",
    avg(b."基準オッズ") as "競争相手平均基準オッズ",
    stddev_pop(b."基準オッズ") as "競争相手基準オッズ標準偏差",
    -- a."基準オッズ" - avg(b."基準オッズ") as "競争相手平均基準オッズ差",

    -- "基準人気順位" (already relative, 無視)

    -- "基準複勝オッズ"
    max(b."基準複勝オッズ") as "競争相手最高基準複勝オッズ",
    min(b."基準複勝オッズ") as "競争相手最低基準複勝オッズ",
    avg(b."基準複勝オッズ") as "競争相手平均基準複勝オッズ",
    stddev_pop(b."基準複勝オッズ") as "競争相手基準複勝オッズ標準偏差",
    -- a."基準複勝オッズ" - avg(b."基準複勝オッズ") as "競争相手平均基準複勝オッズ差",

    -- "基準複勝人気順位" (already relative, 無視)

    -- "特定情報◎"
    -- "特定情報○"
    -- "特定情報▲"
    -- "特定情報△"
    -- "特定情報×"
    -- "総合情報◎"
    -- "総合情報○"
    -- "総合情報▲"
    -- "総合情報△"
    -- "総合情報×"

    -- "人気指数"
    max(b."人気指数") as "競争相手最高人気指数",
    min(b."人気指数") as "競争相手最低人気指数",
    avg(b."人気指数") as "競争相手平均人気指数",
    stddev_pop(b."人気指数") as "競争相手人気指数標準偏差",
    -- a."人気指数" - avg(b."人気指数") as "競争相手平均人気指数差",

    -- "調教指数"
    max(b."調教指数") as "競争相手最高調教指数",
    min(b."調教指数") as "競争相手最低調教指数",
    avg(b."調教指数") as "競争相手平均調教指数",
    stddev_pop(b."調教指数") as "競争相手調教指数標準偏差",
    -- a."調教指数" - avg(b."調教指数") as "競争相手平均調教指数差",

    -- "厩舎指数"
    max(b."厩舎指数") as "競争相手最高厩舎指数",
    min(b."厩舎指数") as "競争相手最低厩舎指数",
    avg(b."厩舎指数") as "競争相手平均厩舎指数",
    stddev_pop(b."厩舎指数") as "競争相手厩舎指数標準偏差",
    -- a."厩舎指数" - avg(b."厩舎指数") as "競争相手平均厩舎指数差",

    -- "調教矢印コード"
    -- "厩舎評価コード"
    -- "騎手期待連対率"

    -- "激走指数"
    max(b."激走指数") as "競争相手最高激走指数",
    min(b."激走指数") as "競争相手最低激走指数",
    avg(b."激走指数") as "競争相手平均激走指数",
    stddev_pop(b."激走指数") as "競争相手激走指数標準偏差",
    -- a."激走指数" - avg(b."激走指数") as "競争相手平均激走指数差",

    -- "蹄コード"
    -- "重適性コード"
    -- "クラスコード"

    -- "ブリンカー" (1:初装着,2:再装着,3:ブリンカ)
    sum(case when b."ブリンカー" = '1' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手ブリンカー1割合",
    sum(case when b."ブリンカー" = '2' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手ブリンカー2割合",
    sum(case when b."ブリンカー" = '3' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手ブリンカー3割合",

    -- "印コード_総合印"
    -- "印コード_ＩＤＭ印"
    -- "印コード_情報印"
    -- "印コード_騎手印"
    -- "印コード_厩舎印"
    -- "印コード_調教印"
    -- "印コード_激走印"

    -- "展開予想データ_テン指数"
    max(b."展開予想データ_テン指数") as "競争相手最高展開予想データ_テン指数",
    min(b."展開予想データ_テン指数") as "競争相手最低展開予想データ_テン指数",
    avg(b."展開予想データ_テン指数") as "競争相手平均展開予想データ_テン指数",
    stddev_pop(b."展開予想データ_テン指数") as "競争相手展開予想データ_テン指数標準偏差",
    -- a."展開予想データ_テン指数" - avg(b."展開予想データ_テン指数") as "競争相手平均展開予想データ_テン指数差",

    -- "展開予想データ_ペース指数"
    max(b."展開予想データ_ペース指数") as "競争相手最高展開予想データ_ペース指数",
    min(b."展開予想データ_ペース指数") as "競争相手最低展開予想データ_ペース指数",
    avg(b."展開予想データ_ペース指数") as "競争相手平均展開予想データ_ペース指数",
    stddev_pop(b."展開予想データ_ペース指数") as "競争相手展開予想データ_ペース指数標準偏差",
    -- a."展開予想データ_ペース指数" - avg(b."展開予想データ_ペース指数") as "競争相手平均展開予想データ_ペース指数差",

    -- "展開予想データ_上がり指数"
    max(b."展開予想データ_上がり指数") as "競争相手最高展開予想データ_上がり指数",
    min(b."展開予想データ_上がり指数") as "競争相手最低展開予想データ_上がり指数",
    avg(b."展開予想データ_上がり指数") as "競争相手平均展開予想データ_上がり指数",
    stddev_pop(b."展開予想データ_上がり指数") as "競争相手展開予想データ_上がり指数標準偏差",
    -- a."展開予想データ_上がり指数" - avg(b."展開予想データ_上がり指数") as "競争相手平均展開予想データ_上がり指数差",

    -- "展開予想データ_位置指数"
    max(b."展開予想データ_位置指数") as "競争相手最高展開予想データ_位置指数",
    min(b."展開予想データ_位置指数") as "競争相手最低展開予想データ_位置指数",
    avg(b."展開予想データ_位置指数") as "競争相手平均展開予想データ_位置指数",
    stddev_pop(b."展開予想データ_位置指数") as "競争相手展開予想データ_位置指数標準偏差",
    -- a."展開予想データ_位置指数" - avg(b."展開予想データ_位置指数") as "競争相手平均展開予想データ_位置指数差",

    -- "展開予想データ_ペース予想" (H,M,S)
    sum(case when b."展開予想データ_ペース予想" = 'H' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手展開予想データ_ペース予想H割合",
    sum(case when b."展開予想データ_ペース予想" = 'M' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手展開予想データ_ペース予想M割合",
    sum(case when b."展開予想データ_ペース予想" = 'S' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手展開予想データ_ペース予想S割合",

    -- "展開予想データ_道中順位"
    -- "展開予想データ_道中差"
    -- "展開予想データ_道中内外"
    -- "展開予想データ_後３Ｆ順位"
    -- "展開予想データ_後３Ｆ差"
    -- "展開予想データ_後３Ｆ内外"
    -- "展開予想データ_ゴール順位"
    -- "展開予想データ_ゴール差"
    -- "展開予想データ_ゴール内外"
    -- "展開予想データ_展開記号"
    -- "激走順位"
    -- "LS指数順位"
    -- "テン指数順位"
    -- "ペース指数順位"
    -- "上がり指数順位"
    -- "位置指数順位"

    -- "騎手期待単勝率"
    max(b."騎手期待単勝率") as "競争相手最高騎手期待単勝率",
    min(b."騎手期待単勝率") as "競争相手最低騎手期待単勝率",
    avg(b."騎手期待単勝率") as "競争相手平均騎手期待単勝率",
    stddev_pop(b."騎手期待単勝率") as "競争相手騎手期待単勝率標準偏差",
    -- a."騎手期待単勝率" - avg(b."騎手期待単勝率") as "競争相手平均騎手期待単勝率差",

    -- "騎手期待３着内率"
    max(b."騎手期待３着内率") as "競争相手最高騎手期待３着内率",
    min(b."騎手期待３着内率") as "競争相手最低騎手期待３着内率",
    avg(b."騎手期待３着内率") as "競争相手平均騎手期待３着内率",
    stddev_pop(b."騎手期待３着内率") as "競争相手騎手期待３着内率標準偏差",
    -- a."騎手期待３着内率" - avg(b."騎手期待３着内率") as "競争相手平均騎手期待３着内率差",

    -- "輸送区分"
    -- "体型_全体"
    -- "体型_背中"
    -- "体型_胴"
    -- "体型_尻"
    -- "体型_トモ"
    -- "体型_腹袋"
    -- "体型_頭"
    -- "体型_首"
    -- "体型_胸"
    -- "体型_肩"
    -- "体型_前長"
    -- "体型_後長"
    -- "体型_前幅"
    -- "体型_後幅"
    -- "体型_前繋"
    -- "体型_後繋"
    -- "体型総合１"
    -- "体型総合２"
    -- "体型総合３"
    -- "馬特記１"
    -- "馬特記２"
    -- "馬特記３"

    -- "展開参考データ_馬スタート指数" (13% are null)
    max(b."展開参考データ_馬スタート指数") as "競争相手最高展開参考データ_馬スタート指数",
    min(b."展開参考データ_馬スタート指数") as "競争相手最低展開参考データ_馬スタート指数",
    avg(b."展開参考データ_馬スタート指数") as "競争相手平均展開参考データ_馬スタート指数",
    stddev_pop(b."展開参考データ_馬スタート指数") as "競争相手展開参考データ_馬スタート指数標準偏差",
    -- a."展開参考データ_馬スタート指数" - avg(b."展開参考データ_馬スタート指数") as "競争相手平均展開参考データ_馬スタート指数差",

    -- "展開参考データ_馬出遅率"
    max(b."展開参考データ_馬出遅率") as "競争相手最高展開参考データ_馬出遅率",
    min(b."展開参考データ_馬出遅率") as "競争相手最低展開参考データ_馬出遅率",
    avg(b."展開参考データ_馬出遅率") as "競争相手平均展開参考データ_馬出遅率",
    stddev_pop(b."展開参考データ_馬出遅率") as "競争相手展開参考データ_馬出遅率標準偏差",
    -- a."展開参考データ_馬出遅率" - avg(b."展開参考データ_馬出遅率") as "競争相手平均展開参考データ_馬出遅率差",

    -- "万券指数"
    max(b."万券指数") as "競争相手最高万券指数",
    min(b."万券指数") as "競争相手最低万券指数",
    avg(b."万券指数") as "競争相手平均万券指数",
    stddev_pop(b."万券指数") as "競争相手万券指数標準偏差",
    -- a."万券指数" - avg(b."万券指数") as "競争相手平均万券指数差",

    -- "万券印"
    sum(case when b."万券印" = '1' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手万券印1割合",
    sum(case when b."万券印" = '2' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手万券印2割合",
    sum(case when b."万券印" = '3' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手万券印3割合",
    sum(case when b."万券印" = '4' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手万券印4割合"

    -- "激走タイプ"
    -- "休養理由分類コード"
    -- "芝ダ障害フラグ"
    -- "距離フラグ"
    -- "クラスフラグ"
    -- "転厩フラグ"
    -- "去勢フラグ"
    -- "乗替フラグ"
  from
    base a
  inner join
    base b
  ON
    a."レースキー" = b."レースキー"
    and a."馬番" <> b."馬番"
  group by
    a."レースキー",
    a."馬番"
  ),

  -- Calculate mean, max, min, stddev of competitor horse features
  competitors_horse as (
  select
    a."レースキー",
    a."馬番",

    -- "性別"
    sum(case when b."性別" = '牡' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手性別牡割合",
    sum(case when b."性別" = '牝' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手性別牝割合",
    sum(case when b."性別" = 'セン' then 1 else 0 end) / cast(count(*) as numeric) as "競争相手性別セ割合",

    -- "一走前着順"
    max(b."一走前着順") as "競争相手最高一走前着順",
    min(b."一走前着順") as "競争相手最低一走前着順",
    avg(b."一走前着順") as "競争相手平均一走前着順",
    stddev_pop(b."一走前着順") as "競争相手一走前着順標準偏差",
    -- a."一走前着順" - avg(b."一走前着順") as "競争相手平均一走前着順差",

    -- "二走前着順"
    max(b."二走前着順") as "競争相手最高二走前着順",
    min(b."二走前着順") as "競争相手最低二走前着順",
    avg(b."二走前着順") as "競争相手平均二走前着順",
    stddev_pop(b."二走前着順") as "競争相手二走前着順標準偏差",
    -- a."二走前着順" - avg(b."二走前着順") as "競争相手平均二走前着順差",

    -- "三走前着順"
    max(b."三走前着順") as "競争相手最高三走前着順",
    min(b."三走前着順") as "競争相手最低三走前着順",
    avg(b."三走前着順") as "競争相手平均三走前着順",
    stddev_pop(b."三走前着順") as "競争相手三走前着順標準偏差",
    -- a."三走前着順" - avg(b."三走前着順") as "競争相手平均三走前着順差",

    -- "四走前着順"
    max(b."四走前着順") as "競争相手最高四走前着順",
    min(b."四走前着順") as "競争相手最低四走前着順",
    avg(b."四走前着順") as "競争相手平均四走前着順",
    stddev_pop(b."四走前着順") as "競争相手四走前着順標準偏差",
    -- a."四走前着順" - avg(b."四走前着順") as "競争相手平均四走前着順差",

    -- "五走前着順"
    max(b."五走前着順") as "競争相手最高五走前着順",
    min(b."五走前着順") as "競争相手最低五走前着順",
    avg(b."五走前着順") as "競争相手平均五走前着順",
    stddev_pop(b."五走前着順") as "競争相手五走前着順標準偏差",
    -- a."五走前着順" - avg(b."五走前着順") as "競争相手平均五走前着順差",

    -- "六走前着順"
    max(b."六走前着順") as "競争相手最高六走前着順",
    min(b."六走前着順") as "競争相手最低六走前着順",
    avg(b."六走前着順") as "競争相手平均六走前着順",
    stddev_pop(b."六走前着順") as "競争相手六走前着順標準偏差",
    -- a."六走前着順" - avg(b."六走前着順") as "競争相手平均六走前着順差",

    -- "前走トップ3"
    sum(case when b."前走トップ3" then 1 else 0 end) / cast(count(*) as numeric) as "競争相手前走トップ3割合",

    -- "前走枠番"

    -- "入厩何日前"
    max(b."入厩何日前") as "競争相手最高入厩何日前",
    min(b."入厩何日前") as "競争相手最低入厩何日前",
    avg(b."入厩何日前") as "競争相手平均入厩何日前",
    stddev_pop(b."入厩何日前") as "競争相手入厩何日前標準偏差",
    -- a."入厩何日前" - avg(b."入厩何日前") as "競争相手平均入厩何日前差",

    -- "入厩15日未満"
    -- "入厩35日以上"

    -- "馬体重"
    max(b."馬体重") as "競争相手最高馬体重",
    min(b."馬体重") as "競争相手最低馬体重",
    avg(b."馬体重") as "競争相手平均馬体重",
    stddev_pop(b."馬体重") as "競争相手馬体重標準偏差",
    -- a."馬体重" - avg(b."馬体重") as "競争相手平均馬体重差",

    -- "馬体重増減"
    max(b."馬体重増減") as "競争相手最高馬体重増減",
    min(b."馬体重増減") as "競争相手最低馬体重増減",
    avg(b."馬体重増減") as "競争相手平均馬体重増減",
    stddev_pop(b."馬体重増減") as "競争相手馬体重増減標準偏差",
    -- a."馬体重増減" - avg(b."馬体重増減") as "競争相手平均馬体重増減差",

    -- "距離"

    -- "前走距離差"
    max(b."前走距離差") as "競争相手最高前走距離差",
    min(b."前走距離差") as "競争相手最低前走距離差",
    avg(b."前走距離差") as "競争相手平均前走距離差",
    stddev_pop(b."前走距離差") as "競争相手前走距離差標準偏差",
    -- a."前走距離差" - avg(b."前走距離差") as "競争相手平均前走距離差差",

    -- "年齢"
    max(b."年齢") as "競争相手最高年齢",
    min(b."年齢") as "競争相手最低年齢",
    avg(b."年齢") as "競争相手平均年齢",
    stddev_pop(b."年齢") as "競争相手年齢標準偏差",
    -- a."年齢" - avg(b."年齢") as "競争相手平均年齢差",

    -- "4歳以下"
    -- "4歳以下頭数"
    -- "4歳以下割合"

    -- "レース数"
    max(b."レース数") as "競争相手最高レース数",
    min(b."レース数") as "競争相手最低レース数",
    avg(b."レース数") as "競争相手平均レース数",
    stddev_pop(b."レース数") as "競争相手レース数標準偏差",
    -- a."レース数" - avg(b."レース数") as "競争相手平均レース数差",

    -- "1位完走"
    max(b."1位完走") as "競争相手最高1位完走",
    min(b."1位完走") as "競争相手最低1位完走",
    avg(b."1位完走") as "競争相手平均1位完走",
    stddev_pop(b."1位完走") as "競争相手1位完走標準偏差",
    -- a."1位完走" - avg(b."1位完走") as "競争相手平均1位完走差",

    -- "トップ3完走"
    max(b."トップ3完走") as "競争相手最高トップ3完走",
    min(b."トップ3完走") as "競争相手最低トップ3完走",
    avg(b."トップ3完走") as "競争相手平均トップ3完走",
    stddev_pop(b."トップ3完走") as "競争相手トップ3完走標準偏差",
    -- a."トップ3完走" - avg(b."トップ3完走") as "競争相手平均トップ3完走差",

    -- "1位完走率"
    max(b."1位完走率") as "競争相手最高1位完走率",
    min(b."1位完走率") as "競争相手最低1位完走率",
    avg(b."1位完走率") as "競争相手平均1位完走率",
    stddev_pop(b."1位完走率") as "競争相手1位完走率標準偏差",
    -- a."1位完走率" - avg(b."1位完走率") as "競争相手平均1位完走率差",

    -- "トップ3完走率"
    max(b."トップ3完走率") as "競争相手最高トップ3完走率",
    min(b."トップ3完走率") as "競争相手最低トップ3完走率",
    avg(b."トップ3完走率") as "競争相手平均トップ3完走率",
    stddev_pop(b."トップ3完走率") as "競争相手トップ3完走率標準偏差",
    -- a."トップ3完走率" - avg(b."トップ3完走率") as "競争相手平均トップ3完走率差",

    -- "過去5走勝率"
    max(b."過去5走勝率") as "競争相手最高過去5走勝率",
    min(b."過去5走勝率") as "競争相手最低過去5走勝率",
    avg(b."過去5走勝率") as "競争相手平均過去5走勝率",
    stddev_pop(b."過去5走勝率") as "競争相手過去5走勝率標準偏差",
    -- a."過去5走勝率" - avg(b."過去5走勝率") as "競争相手平均過去5走勝率差",

    -- "過去5走トップ3完走率"
    max(b."過去5走トップ3完走率") as "競争相手最高過去5走トップ3完走率",
    min(b."過去5走トップ3完走率") as "競争相手最低過去5走トップ3完走率",
    avg(b."過去5走トップ3完走率") as "競争相手平均過去5走トップ3完走率",
    stddev_pop(b."過去5走トップ3完走率") as "競争相手過去5走トップ3完走率標準偏差",
    -- a."過去5走トップ3完走率" - avg(b."過去5走トップ3完走率") as "競争相手平均過去5走トップ3完走率差",

    -- "場所レース数"
    max(b."場所レース数") as "競争相手最高場所レース数",
    min(b."場所レース数") as "競争相手最低場所レース数",
    avg(b."場所レース数") as "競争相手平均場所レース数",
    stddev_pop(b."場所レース数") as "競争相手場所レース数標準偏差",
    -- a."場所レース数" - avg(b."場所レース数") as "競争相手平均場所レース数差",

    -- "場所1位完走"
    max(b."場所1位完走") as "競争相手最高場所1位完走",
    min(b."場所1位完走") as "競争相手最低場所1位完走",
    avg(b."場所1位完走") as "競争相手平均場所1位完走",
    stddev_pop(b."場所1位完走") as "競争相手場所1位完走標準偏差",
    -- a."場所1位完走" - avg(b."場所1位完走") as "競争相手平均場所1位完走差",

    -- "場所トップ3完走"
    max(b."場所トップ3完走") as "競争相手最高場所トップ3完走",
    min(b."場所トップ3完走") as "競争相手最低場所トップ3完走",
    avg(b."場所トップ3完走") as "競争相手平均場所トップ3完走",
    stddev_pop(b."場所トップ3完走") as "競争相手場所トップ3完走標準偏差",
    -- a."場所トップ3完走" - avg(b."場所トップ3完走") as "競争相手平均場所トップ3完走差",

    -- "場所1位完走率"
    max(b."場所1位完走率") as "競争相手最高場所1位完走率",
    min(b."場所1位完走率") as "競争相手最低場所1位完走率",
    avg(b."場所1位完走率") as "競争相手平均場所1位完走率",
    stddev_pop(b."場所1位完走率") as "競争相手場所1位完走率標準偏差",
    -- a."場所1位完走率" - avg(b."場所1位完走率") as "競争相手平均場所1位完走率差",

    -- "場所トップ3完走率"
    max(b."場所トップ3完走率") as "競争相手最高場所トップ3完走率",
    min(b."場所トップ3完走率") as "競争相手最低場所トップ3完走率",
    avg(b."場所トップ3完走率") as "競争相手平均場所トップ3完走率",
    stddev_pop(b."場所トップ3完走率") as "競争相手場所トップ3完走率標準偏差",
    -- a."場所トップ3完走率" - avg(b."場所トップ3完走率") as "競争相手平均場所トップ3完走率差",

    -- "トラック種別レース数"
    max(b."トラック種別レース数") as "競争相手最高トラック種別レース数",
    min(b."トラック種別レース数") as "競争相手最低トラック種別レース数",
    avg(b."トラック種別レース数") as "競争相手平均トラック種別レース数",
    stddev_pop(b."トラック種別レース数") as "競争相手トラック種別レース数標準偏差",
    -- a."トラック種別レース数" - avg(b."トラック種別レース数") as "競争相手平均トラック種別レース数差",

    -- "トラック種別1位完走"
    max(b."トラック種別1位完走") as "競争相手最高トラック種別1位完走",
    min(b."トラック種別1位完走") as "競争相手最低トラック種別1位完走",
    avg(b."トラック種別1位完走") as "競争相手平均トラック種別1位完走",
    stddev_pop(b."トラック種別1位完走") as "競争相手トラック種別1位完走標準偏差",
    -- a."トラック種別1位完走" - avg(b."トラック種別1位完走") as "競争相手平均トラック種別1位完走差",

    -- "トラック種別トップ3完走"
    max(b."トラック種別トップ3完走") as "競争相手最高トラック種別トップ3完走",
    min(b."トラック種別トップ3完走") as "競争相手最低トラック種別トップ3完走",
    avg(b."トラック種別トップ3完走") as "競争相手平均トラック種別トップ3完走",
    stddev_pop(b."トラック種別トップ3完走") as "競争相手トラック種別トップ3完走標準偏差",
    -- a."トラック種別トップ3完走" - avg(b."トラック種別トップ3完走") as "競争相手平均トラック種別トップ3完走差",

    -- "馬場状態レース数"
    max(b."馬場状態レース数") as "競争相手最高馬場状態レース数",
    min(b."馬場状態レース数") as "競争相手最低馬場状態レース数",
    avg(b."馬場状態レース数") as "競争相手平均馬場状態レース数",
    stddev_pop(b."馬場状態レース数") as "競争相手馬場状態レース数標準偏差",
    -- a."馬場状態レース数" - avg(b."馬場状態レース数") as "競争相手平均馬場状態レース数差",

    -- "馬場状態1位完走"
    max(b."馬場状態1位完走") as "競争相手最高馬場状態1位完走",
    min(b."馬場状態1位完走") as "競争相手最低馬場状態1位完走",
    avg(b."馬場状態1位完走") as "競争相手平均馬場状態1位完走",
    stddev_pop(b."馬場状態1位完走") as "競争相手馬場状態1位完走標準偏差",
    -- a."馬場状態1位完走" - avg(b."馬場状態1位完走") as "競争相手平均馬場状態1位完走差",

    -- "馬場状態トップ3完走"
    max(b."馬場状態トップ3完走") as "競争相手最高馬場状態トップ3完走",
    min(b."馬場状態トップ3完走") as "競争相手最低馬場状態トップ3完走",
    avg(b."馬場状態トップ3完走") as "競争相手平均馬場状態トップ3完走",
    stddev_pop(b."馬場状態トップ3完走") as "競争相手馬場状態トップ3完走標準偏差",
    -- a."馬場状態トップ3完走" - avg(b."馬場状態トップ3完走") as "競争相手平均馬場状態トップ3完走差",

    -- "距離レース数"
    max(b."距離レース数") as "競争相手最高距離レース数",
    min(b."距離レース数") as "競争相手最低距離レース数",
    avg(b."距離レース数") as "競争相手平均距離レース数",
    stddev_pop(b."距離レース数") as "競争相手距離レース数標準偏差",
    -- a."距離レース数" - avg(b."距離レース数") as "競争相手平均距離レース数差",

    -- "距離1位完走"
    max(b."距離1位完走") as "競争相手最高距離1位完走",
    min(b."距離1位完走") as "競争相手最低距離1位完走",
    avg(b."距離1位完走") as "競争相手平均距離1位完走",
    stddev_pop(b."距離1位完走") as "競争相手距離1位完走標準偏差",
    -- a."距離1位完走" - avg(b."距離1位完走") as "競争相手平均距離1位完走差",

    -- "距離トップ3完走"
    max(b."距離トップ3完走") as "競争相手最高距離トップ3完走",
    min(b."距離トップ3完走") as "競争相手最低距離トップ3完走",
    avg(b."距離トップ3完走") as "競争相手平均距離トップ3完走",
    stddev_pop(b."距離トップ3完走") as "競争相手距離トップ3完走標準偏差",
    -- a."距離トップ3完走" - avg(b."距離トップ3完走") as "競争相手平均距離トップ3完走差",

    -- "四半期レース数"
    max(b."四半期レース数") as "競争相手最高四半期レース数",
    min(b."四半期レース数") as "競争相手最低四半期レース数",
    avg(b."四半期レース数") as "競争相手平均四半期レース数",
    stddev_pop(b."四半期レース数") as "競争相手四半期レース数標準偏差",
    -- a."四半期レース数" - avg(b."四半期レース数") as "競争相手平均四半期レース数差",

    -- "四半期1位完走"
    max(b."四半期1位完走") as "競争相手最高四半期1位完走",
    min(b."四半期1位完走") as "競争相手最低四半期1位完走",
    avg(b."四半期1位完走") as "競争相手平均四半期1位完走",
    stddev_pop(b."四半期1位完走") as "競争相手四半期1位完走標準偏差",
    -- a."四半期1位完走" - avg(b."四半期1位完走") as "競争相手平均四半期1位完走差",

    -- "四半期トップ3完走"
    max(b."四半期トップ3完走") as "競争相手最高四半期トップ3完走",
    min(b."四半期トップ3完走") as "競争相手最低四半期トップ3完走",
    avg(b."四半期トップ3完走") as "競争相手平均四半期トップ3完走",
    stddev_pop(b."四半期トップ3完走") as "競争相手四半期トップ3完走標準偏差",
    -- a."四半期トップ3完走" - avg(b."四半期トップ3完走") as "競争相手平均四半期トップ3完走差",

    -- "過去3走順位平方和"
    max(b."過去3走順位平方和") as "競争相手最高過去3走順位平方和",
    min(b."過去3走順位平方和") as "競争相手最低過去3走順位平方和",
    avg(b."過去3走順位平方和") as "競争相手平均過去3走順位平方和",
    stddev_pop(b."過去3走順位平方和") as "競争相手過去3走順位平方和標準偏差",
    -- a."過去3走順位平方和" - avg(b."過去3走順位平方和") as "競争相手平均過去3走順位平方和差",

    -- "本賞金累計"
    max(b."本賞金累計") as "競争相手最高本賞金累計",
    min(b."本賞金累計") as "競争相手最低本賞金累計",
    avg(b."本賞金累計") as "競争相手平均本賞金累計",
    stddev_pop(b."本賞金累計") as "競争相手本賞金累計標準偏差",
    -- a."本賞金累計" - avg(b."本賞金累計") as "競争相手平均本賞金累計差",

    -- "1位完走平均賞金"
    max(b."1位完走平均賞金") as "競争相手最高1位完走平均賞金",
    min(b."1位完走平均賞金") as "競争相手最低1位完走平均賞金",
    avg(b."1位完走平均賞金") as "競争相手平均1位完走平均賞金",
    stddev_pop(b."1位完走平均賞金") as "競争相手1位完走平均賞金標準偏差",
    -- a."1位完走平均賞金" - avg(b."1位完走平均賞金") as "競争相手平均1位完走平均賞金差",

    -- "レース数平均賞金"
    max(b."レース数平均賞金") as "競争相手最高レース数平均賞金",
    min(b."レース数平均賞金") as "競争相手最低レース数平均賞金",
    avg(b."レース数平均賞金") as "競争相手平均レース数平均賞金",
    stddev_pop(b."レース数平均賞金") as "競争相手レース数平均賞金標準偏差",
    -- a."レース数平均賞金" - avg(b."レース数平均賞金") as "競争相手平均レース数平均賞金差",

    -- "瞬発戦好走馬_芝"
    sum(case when b."瞬発戦好走馬_芝" then 1 else 0 end) / cast(count(*) as numeric) as "競争相手瞬発戦好走馬_芝割合",

    -- "消耗戦好走馬_芝"
    sum(case when b."消耗戦好走馬_芝" then 1 else 0 end) / cast(count(*) as numeric) as "競争相手消耗戦好走馬_芝割合",

    -- "瞬発戦好走馬_ダート"
    sum(case when b."瞬発戦好走馬_ダート" then 1 else 0 end) / cast(count(*) as numeric) as "競争相手瞬発戦好走馬_ダート割合",

    -- "消耗戦好走馬_ダート"
    sum(case when b."消耗戦好走馬_ダート" then 1 else 0 end) / cast(count(*) as numeric) as "競争相手消耗戦好走馬_ダート割合",

    -- "瞬発戦好走馬_総合"
    sum(case when b."瞬発戦好走馬_総合" then 1 else 0 end) / cast(count(*) as numeric) as "競争相手瞬発戦好走馬_総合割合",

    -- "消耗戦好走馬_総合"
    sum(case when b."消耗戦好走馬_総合" then 1 else 0 end) / cast(count(*) as numeric) as "競争相手消耗戦好走馬_総合割合",

    -- "連続1着"
    max(b."連続1着") as "競争相手最高連続1着",
    min(b."連続1着") as "競争相手最低連続1着",
    avg(b."連続1着") as "競争相手平均連続1着",
    stddev_pop(b."連続1着") as "競争相手連続1着標準偏差",
    -- a."連続1着" - avg(b."連続1着") as "競争相手平均連続1着差",

    -- "連続3着以内"
    max(b."連続3着以内") as "競争相手最高連続3着以内",
    min(b."連続3着以内") as "競争相手最低連続3着以内",
    avg(b."連続3着以内") as "競争相手平均連続3着以内",
    stddev_pop(b."連続3着以内") as "競争相手連続3着以内標準偏差"
    -- a."連続3着以内" - avg(b."連続3着以内") as "競争相手平均連続3着以内差"

  from
    {{ ref("int_race_horses") }} a
  inner join
    {{ ref("int_race_horses") }} b
  on
    a."レースキー" = b."レースキー"
    and a."馬番" <> b."馬番"
  group by
    a."レースキー",
    a."馬番"
  ),

  competitors as (
  select
    competitors_base."レースキー",
    competitors_base."馬番",
    competitors_base."競争相手最高IDM",
    competitors_base."競争相手最低IDM",
    competitors_base."競争相手平均IDM",
    competitors_base."競争相手IDM標準偏差",
    competitors_base."競争相手脚質逃げ割合",
    competitors_base."競争相手脚質先行割合",
    competitors_base."競争相手脚質追込割合",
    competitors_base."競争相手脚質自在割合",
    competitors_base."競争相手最高単勝オッズ",
    competitors_base."競争相手最低単勝オッズ",
    competitors_base."競争相手平均単勝オッズ",
    competitors_base."競争相手単勝オッズ標準偏差",
    competitors_base."競争相手最高複勝オッズ",
    competitors_base."競争相手最低複勝オッズ",
    competitors_base."競争相手平均複勝オッズ",
    competitors_base."競争相手複勝オッズ標準偏差",
    competitors_base."競争相手最高騎手指数",
    competitors_base."競争相手最低騎手指数",
    competitors_base."競争相手平均騎手指数",
    competitors_base."競争相手騎手指数標準偏差",
    competitors_base."競争相手最高情報指数",
    competitors_base."競争相手最低情報指数",
    competitors_base."競争相手平均情報指数",
    competitors_base."競争相手情報指数標準偏差",
    competitors_base."競争相手最高オッズ指数",
    competitors_base."競争相手最低オッズ指数",
    competitors_base."競争相手平均オッズ指数",
    competitors_base."競争相手オッズ指数標準偏差",
    competitors_base."競争相手最高パドック指数",
    competitors_base."競争相手最低パドック指数",
    competitors_base."競争相手平均パドック指数",
    competitors_base."競争相手パドック指数標準偏差",
    competitors_base."競争相手最高総合指数",
    competitors_base."競争相手最低総合指数",
    competitors_base."競争相手平均総合指数",
    competitors_base."競争相手総合指数標準偏差",
    competitors_base."競争相手馬具変更情報0割合",
    competitors_base."競争相手馬具変更情報1割合",
    competitors_base."競争相手馬具変更情報2割合",
    competitors_base."競争相手最高負担重量",
    competitors_base."競争相手最低負担重量",
    competitors_base."競争相手平均負担重量",
    competitors_base."競争相手負担重量標準偏差",
    competitors_base."競争相手最高見習い区分",
    competitors_base."競争相手最低見習い区分",
    competitors_base."競争相手平均見習い区分",
    competitors_base."競争相手見習い区分標準偏差",
    competitors_base."競争相手オッズ印1割合",
    competitors_base."競争相手オッズ印2割合",
    competitors_base."競争相手オッズ印3割合",
    competitors_base."競争相手オッズ印4割合",
    competitors_base."競争相手パドック印1割合",
    competitors_base."競争相手パドック印2割合",
    competitors_base."競争相手パドック印3割合",
    competitors_base."競争相手パドック印4割合",
    competitors_base."競争相手直前総合印1割合",
    competitors_base."競争相手直前総合印2割合",
    competitors_base."競争相手直前総合印3割合",
    competitors_base."競争相手直前総合印4割合",
    competitors_base."競争相手距離適性1割合",
    competitors_base."競争相手距離適性2割合",
    competitors_base."競争相手距離適性3割合",
    competitors_base."競争相手距離適性5割合",
    competitors_base."競争相手距離適性6割合",
    competitors_base."競争相手最高上昇度",
    competitors_base."競争相手最低上昇度",
    competitors_base."競争相手平均上昇度",
    competitors_base."競争相手上昇度標準偏差",
    competitors_base."競争相手最高ローテーション",
    competitors_base."競争相手最低ローテーション",
    competitors_base."競争相手平均ローテーション",
    competitors_base."競争相手ローテーション標準偏差",
    competitors_base."競争相手最高基準オッズ",
    competitors_base."競争相手最低基準オッズ",
    competitors_base."競争相手平均基準オッズ",
    competitors_base."競争相手基準オッズ標準偏差",
    competitors_base."競争相手最高基準複勝オッズ",
    competitors_base."競争相手最低基準複勝オッズ",
    competitors_base."競争相手平均基準複勝オッズ",
    competitors_base."競争相手基準複勝オッズ標準偏差",
    competitors_base."競争相手最高人気指数",
    competitors_base."競争相手最低人気指数",
    competitors_base."競争相手平均人気指数",
    competitors_base."競争相手人気指数標準偏差",
    competitors_base."競争相手最高調教指数",
    competitors_base."競争相手最低調教指数",
    competitors_base."競争相手平均調教指数",
    competitors_base."競争相手調教指数標準偏差",
    competitors_base."競争相手最高厩舎指数",
    competitors_base."競争相手最低厩舎指数",
    competitors_base."競争相手平均厩舎指数",
    competitors_base."競争相手厩舎指数標準偏差",
    competitors_base."競争相手最高激走指数",
    competitors_base."競争相手最低激走指数",
    competitors_base."競争相手平均激走指数",
    competitors_base."競争相手激走指数標準偏差",
    competitors_base."競争相手ブリンカー1割合",
    competitors_base."競争相手ブリンカー2割合",
    competitors_base."競争相手ブリンカー3割合",
    competitors_base."競争相手最高展開予想データ_テン指数",
    competitors_base."競争相手最低展開予想データ_テン指数",
    competitors_base."競争相手平均展開予想データ_テン指数",
    competitors_base."競争相手展開予想データ_テン指数標準偏差",
    competitors_base."競争相手最高展開予想データ_ペース指数",
    competitors_base."競争相手最低展開予想データ_ペース指数",
    competitors_base."競争相手平均展開予想データ_ペース指数",
    competitors_base."競争相手展開予想データ_ペース指数標準偏差",
    competitors_base."競争相手最高展開予想データ_上がり指数",
    competitors_base."競争相手最低展開予想データ_上がり指数",
    competitors_base."競争相手平均展開予想データ_上がり指数",
    competitors_base."競争相手展開予想データ_上がり指数標準偏差",
    competitors_base."競争相手最高展開予想データ_位置指数",
    competitors_base."競争相手最低展開予想データ_位置指数",
    competitors_base."競争相手平均展開予想データ_位置指数",
    competitors_base."競争相手展開予想データ_位置指数標準偏差",
    competitors_base."競争相手展開予想データ_ペース予想H割合",
    competitors_base."競争相手展開予想データ_ペース予想M割合",
    competitors_base."競争相手展開予想データ_ペース予想S割合",
    competitors_base."競争相手最高騎手期待単勝率",
    competitors_base."競争相手最低騎手期待単勝率",
    competitors_base."競争相手平均騎手期待単勝率",
    competitors_base."競争相手騎手期待単勝率標準偏差",
    competitors_base."競争相手最高騎手期待３着内率",
    competitors_base."競争相手最低騎手期待３着内率",
    competitors_base."競争相手平均騎手期待３着内率",
    competitors_base."競争相手騎手期待３着内率標準偏差",
    competitors_base."競争相手最高展開参考データ_馬スタート指数",
    competitors_base."競争相手最低展開参考データ_馬スタート指数",
    competitors_base."競争相手平均展開参考データ_馬スタート指数",
    competitors_base."競争相手展開参考データ_馬スタート指数標準偏差",
    competitors_base."競争相手最高展開参考データ_馬出遅率",
    competitors_base."競争相手最低展開参考データ_馬出遅率",
    competitors_base."競争相手平均展開参考データ_馬出遅率",
    competitors_base."競争相手展開参考データ_馬出遅率標準偏差",
    competitors_base."競争相手最高万券指数",
    competitors_base."競争相手最低万券指数",
    competitors_base."競争相手平均万券指数",
    competitors_base."競争相手万券指数標準偏差",
    competitors_base."競争相手万券印1割合",
    competitors_base."競争相手万券印2割合",
    competitors_base."競争相手万券印3割合",
    competitors_base."競争相手万券印4割合",

    competitors_horse."競争相手性別牡割合",
    competitors_horse."競争相手性別牝割合",
    competitors_horse."競争相手性別セ割合",
    competitors_horse."競争相手最高一走前着順",
    competitors_horse."競争相手最低一走前着順",
    competitors_horse."競争相手平均一走前着順",
    competitors_horse."競争相手一走前着順標準偏差",
    competitors_horse."競争相手最高二走前着順",
    competitors_horse."競争相手最低二走前着順",
    competitors_horse."競争相手平均二走前着順",
    competitors_horse."競争相手二走前着順標準偏差",
    competitors_horse."競争相手最高三走前着順",
    competitors_horse."競争相手最低三走前着順",
    competitors_horse."競争相手平均三走前着順",
    competitors_horse."競争相手三走前着順標準偏差",
    competitors_horse."競争相手最高四走前着順",
    competitors_horse."競争相手最低四走前着順",
    competitors_horse."競争相手平均四走前着順",
    competitors_horse."競争相手四走前着順標準偏差",
    competitors_horse."競争相手最高五走前着順",
    competitors_horse."競争相手最低五走前着順",
    competitors_horse."競争相手平均五走前着順",
    competitors_horse."競争相手五走前着順標準偏差",
    competitors_horse."競争相手最高六走前着順",
    competitors_horse."競争相手最低六走前着順",
    competitors_horse."競争相手平均六走前着順",
    competitors_horse."競争相手六走前着順標準偏差",
    competitors_horse."競争相手前走トップ3割合",
    competitors_horse."競争相手最高入厩何日前",
    competitors_horse."競争相手最低入厩何日前",
    competitors_horse."競争相手平均入厩何日前",
    competitors_horse."競争相手入厩何日前標準偏差",
    competitors_horse."競争相手最高馬体重",
    competitors_horse."競争相手最低馬体重",
    competitors_horse."競争相手平均馬体重",
    competitors_horse."競争相手馬体重標準偏差",
    competitors_horse."競争相手最高馬体重増減",
    competitors_horse."競争相手最低馬体重増減",
    competitors_horse."競争相手平均馬体重増減",
    competitors_horse."競争相手馬体重増減標準偏差",
    competitors_horse."競争相手最高年齢",
    competitors_horse."競争相手最低年齢",
    competitors_horse."競争相手平均年齢",
    competitors_horse."競争相手年齢標準偏差",
    competitors_horse."競争相手最高レース数",
    competitors_horse."競争相手最低レース数",
    competitors_horse."競争相手平均レース数",
    competitors_horse."競争相手レース数標準偏差",
    competitors_horse."競争相手最高1位完走",
    competitors_horse."競争相手最低1位完走",
    competitors_horse."競争相手平均1位完走",
    competitors_horse."競争相手1位完走標準偏差",
    competitors_horse."競争相手最高トップ3完走",
    competitors_horse."競争相手最低トップ3完走",
    competitors_horse."競争相手平均トップ3完走",
    competitors_horse."競争相手トップ3完走標準偏差",
    competitors_horse."競争相手最高1位完走率",
    competitors_horse."競争相手最低1位完走率",
    competitors_horse."競争相手平均1位完走率",
    competitors_horse."競争相手1位完走率標準偏差",
    competitors_horse."競争相手最高トップ3完走率",
    competitors_horse."競争相手最低トップ3完走率",
    competitors_horse."競争相手平均トップ3完走率",
    competitors_horse."競争相手トップ3完走率標準偏差",
    competitors_horse."競争相手最高過去5走勝率",
    competitors_horse."競争相手最低過去5走勝率",
    competitors_horse."競争相手平均過去5走勝率",
    competitors_horse."競争相手過去5走勝率標準偏差",
    competitors_horse."競争相手最高過去5走トップ3完走率",
    competitors_horse."競争相手最低過去5走トップ3完走率",
    competitors_horse."競争相手平均過去5走トップ3完走率",
    competitors_horse."競争相手過去5走トップ3完走率標準偏差",
    competitors_horse."競争相手最高場所レース数",
    competitors_horse."競争相手最低場所レース数",
    competitors_horse."競争相手平均場所レース数",
    competitors_horse."競争相手場所レース数標準偏差",
    competitors_horse."競争相手最高場所1位完走",
    competitors_horse."競争相手最低場所1位完走",
    competitors_horse."競争相手平均場所1位完走",
    competitors_horse."競争相手場所1位完走標準偏差",
    competitors_horse."競争相手最高場所トップ3完走",
    competitors_horse."競争相手最低場所トップ3完走",
    competitors_horse."競争相手平均場所トップ3完走",
    competitors_horse."競争相手場所トップ3完走標準偏差",
    competitors_horse."競争相手最高場所1位完走率",
    competitors_horse."競争相手最低場所1位完走率",
    competitors_horse."競争相手平均場所1位完走率",
    competitors_horse."競争相手場所1位完走率標準偏差",
    competitors_horse."競争相手最高場所トップ3完走率",
    competitors_horse."競争相手最低場所トップ3完走率",
    competitors_horse."競争相手平均場所トップ3完走率",
    competitors_horse."競争相手場所トップ3完走率標準偏差",
    competitors_horse."競争相手最高トラック種別レース数",
    competitors_horse."競争相手最低トラック種別レース数",
    competitors_horse."競争相手平均トラック種別レース数",
    competitors_horse."競争相手トラック種別レース数標準偏差",
    competitors_horse."競争相手最高トラック種別1位完走",
    competitors_horse."競争相手最低トラック種別1位完走",
    competitors_horse."競争相手平均トラック種別1位完走",
    competitors_horse."競争相手トラック種別1位完走標準偏差",
    competitors_horse."競争相手最高トラック種別トップ3完走",
    competitors_horse."競争相手最低トラック種別トップ3完走",
    competitors_horse."競争相手平均トラック種別トップ3完走",
    competitors_horse."競争相手トラック種別トップ3完走標準偏差",
    competitors_horse."競争相手最高馬場状態レース数",
    competitors_horse."競争相手最低馬場状態レース数",
    competitors_horse."競争相手平均馬場状態レース数",
    competitors_horse."競争相手馬場状態レース数標準偏差",
    competitors_horse."競争相手最高馬場状態1位完走",
    competitors_horse."競争相手最低馬場状態1位完走",
    competitors_horse."競争相手平均馬場状態1位完走",
    competitors_horse."競争相手馬場状態1位完走標準偏差",
    competitors_horse."競争相手最高馬場状態トップ3完走",
    competitors_horse."競争相手最低馬場状態トップ3完走",
    competitors_horse."競争相手平均馬場状態トップ3完走",
    competitors_horse."競争相手馬場状態トップ3完走標準偏差",
    competitors_horse."競争相手最高距離レース数",
    competitors_horse."競争相手最低距離レース数",
    competitors_horse."競争相手平均距離レース数",
    competitors_horse."競争相手距離レース数標準偏差",
    competitors_horse."競争相手最高距離1位完走",
    competitors_horse."競争相手最低距離1位完走",
    competitors_horse."競争相手平均距離1位完走",
    competitors_horse."競争相手距離1位完走標準偏差",
    competitors_horse."競争相手最高距離トップ3完走",
    competitors_horse."競争相手最低距離トップ3完走",
    competitors_horse."競争相手平均距離トップ3完走",
    competitors_horse."競争相手距離トップ3完走標準偏差",
    competitors_horse."競争相手最高四半期レース数",
    competitors_horse."競争相手最低四半期レース数",
    competitors_horse."競争相手平均四半期レース数",
    competitors_horse."競争相手四半期レース数標準偏差",
    competitors_horse."競争相手最高四半期1位完走",
    competitors_horse."競争相手最低四半期1位完走",
    competitors_horse."競争相手平均四半期1位完走",
    competitors_horse."競争相手四半期1位完走標準偏差",
    competitors_horse."競争相手最高四半期トップ3完走",
    competitors_horse."競争相手最低四半期トップ3完走",
    competitors_horse."競争相手平均四半期トップ3完走",
    competitors_horse."競争相手四半期トップ3完走標準偏差",
    competitors_horse."競争相手最高過去3走順位平方和",
    competitors_horse."競争相手最低過去3走順位平方和",
    competitors_horse."競争相手平均過去3走順位平方和",
    competitors_horse."競争相手過去3走順位平方和標準偏差",
    competitors_horse."競争相手最高本賞金累計",
    competitors_horse."競争相手最低本賞金累計",
    competitors_horse."競争相手平均本賞金累計",
    competitors_horse."競争相手本賞金累計標準偏差",
    competitors_horse."競争相手最高1位完走平均賞金",
    competitors_horse."競争相手最低1位完走平均賞金",
    competitors_horse."競争相手平均1位完走平均賞金",
    competitors_horse."競争相手1位完走平均賞金標準偏差",
    competitors_horse."競争相手最高レース数平均賞金",
    competitors_horse."競争相手最低レース数平均賞金",
    competitors_horse."競争相手平均レース数平均賞金",
    competitors_horse."競争相手レース数平均賞金標準偏差",
    competitors_horse."競争相手瞬発戦好走馬_芝割合",
    competitors_horse."競争相手消耗戦好走馬_芝割合",
    competitors_horse."競争相手瞬発戦好走馬_ダート割合",
    competitors_horse."競争相手消耗戦好走馬_ダート割合",
    competitors_horse."競争相手瞬発戦好走馬_総合割合",
    competitors_horse."競争相手消耗戦好走馬_総合割合",
    competitors_horse."競争相手最高連続1着",
    competitors_horse."競争相手最低連続1着",
    competitors_horse."競争相手平均連続1着",
    competitors_horse."競争相手連続1着標準偏差",
    competitors_horse."競争相手最高連続3着以内",
    competitors_horse."競争相手最低連続3着以内",
    competitors_horse."競争相手平均連続3着以内",
    competitors_horse."競争相手連続3着以内標準偏差"
  from
    competitors_base
  inner join
    competitors_horse
  on
    competitors_base.レースキー = competitors_horse.レースキー
    and competitors_base.馬番 = competitors_horse.馬番
  ),

  final as (
  select
    base."レースキー",
    base."馬番",
    base."枠番",
    base."血統登録番号",
    base."場コード",
    base."騎手コード",
    base."調教師コード",
    base."年月日",
    base."頭数",
    base."単勝的中",
    base."単勝払戻金",
    base."複勝的中",
    base."複勝払戻金",
    base."四半期",
    base."馬場差",
    base."芝馬場状態内",
    base."芝馬場状態中",
    base."芝馬場状態外",
    base."直線馬場差最内",
    base."直線馬場差内",
    base."直線馬場差中",
    base."直線馬場差外",
    base."直線馬場差大外",
    base."ダ馬場状態内",
    base."ダ馬場状態中",
    base."ダ馬場状態外",
    base."芝種類",
    base."草丈",
    base."転圧",
    base."凍結防止剤",
    base."中間降水量",
    base."馬場状態コード",
    base."レース条件_トラック情報_右左",
    base."レース条件_トラック情報_内外",
    base."レース条件_種別",
    base."レース条件_条件",
    base."レース条件_記号",
    base."レース条件_重量",
    base."レース条件_グレード",
    base."トラック種別",
    base."ＩＤＭ",
    base."IDM標準偏差",
    -- Create a new feature that combines the IDM and IDM標準偏差 to express the horses IDM in comparison
    -- to the standard deviation.
    coalesce({{ dbt_utils.safe_divide('base."ＩＤＭ"', 'base."IDM標準偏差"') }}, 0) as "IDM_標準偏差比",
    base."脚質",
    base."単勝オッズ",
    base."複勝オッズ",
    base."騎手指数",
    base."情報指数",
    base."オッズ指数",
    base."パドック指数",
    base."総合指数",
    base."馬具変更情報",
    base."脚元情報",
    base."負担重量",
    base."見習い区分",
    base."オッズ印",
    base."パドック印",
    base."直前総合印",
    base."馬体",
    base."気配コード",
    base."距離適性",
    base."上昇度",
    base."ローテーション",
    base."基準オッズ",
    base."基準人気順位",
    base."基準複勝オッズ",
    base."基準複勝人気順位",
    base."特定情報◎",
    base."特定情報○",
    base."特定情報▲",
    base."特定情報△",
    base."特定情報×",
    base."総合情報◎",
    base."総合情報○",
    base."総合情報▲",
    base."総合情報△",
    base."総合情報×",
    base."人気指数",
    base."調教指数",
    base."厩舎指数",
    base."調教矢印コード",
    base."厩舎評価コード",
    base."騎手期待連対率",
    base."激走指数",
    base."蹄コード",
    base."重適性コード",
    base."クラスコード",
    base."ブリンカー",
    base."印コード_総合印",
    base."印コード_ＩＤＭ印",
    base."印コード_情報印",
    base."印コード_騎手印",
    base."印コード_厩舎印",
    base."印コード_調教印",
    base."印コード_激走印",
    base."展開予想データ_テン指数",
    base."展開予想データ_ペース指数",
    base."展開予想データ_上がり指数",
    base."展開予想データ_位置指数",
    base."展開予想データ_ペース予想",
    base."展開予想データ_道中順位",
    base."展開予想データ_道中差",
    base."展開予想データ_道中内外",
    base."展開予想データ_後３Ｆ順位",
    base."展開予想データ_後３Ｆ差",
    base."展開予想データ_後３Ｆ内外",
    base."展開予想データ_ゴール順位",
    base."展開予想データ_ゴール差",
    base."展開予想データ_ゴール内外",
    base."展開予想データ_展開記号",
    base."激走順位",
    base."LS指数順位",
    base."テン指数順位",
    base."ペース指数順位",
    base."上がり指数順位",
    base."位置指数順位",
    base."騎手期待単勝率",
    base."騎手期待３着内率",
    base."輸送区分",
    base."体型_全体",
    base."体型_背中",
    base."体型_胴",
    base."体型_尻",
    base."体型_トモ",
    base."体型_腹袋",
    base."体型_頭",
    base."体型_首",
    base."体型_胸",
    base."体型_肩",
    base."体型_前長",
    base."体型_後長",
    base."体型_前幅",
    base."体型_後幅",
    base."体型_前繋",
    base."体型_後繋",
    base."体型総合１",
    base."体型総合２",
    base."体型総合３",
    base."馬特記１",
    base."馬特記２",
    base."馬特記３",
    base."展開参考データ_馬スタート指数",
    base."展開参考データ_馬出遅率",
    base."万券指数",
    base."万券印",
    base."激走タイプ",
    base."休養理由分類コード",
    base."芝ダ障害フラグ",
    base."距離フラグ",
    base."クラスフラグ",
    base."転厩フラグ",
    base."去勢フラグ",
    base."乗替フラグ",
    base."放牧先ランク",
    base."厩舎ランク",
    base."天候コード",

    horse_features."性別",
    horse_features."一走前着順",
    horse_features."二走前着順",
    horse_features."三走前着順",
    horse_features."四走前着順",
    horse_features."五走前着順",
    horse_features."六走前着順",
    horse_features."前走トップ3",
    horse_features."前走枠番",
    horse_features."入厩何日前", -- horse_rest_time
    horse_features."入厩15日未満", -- horse_rest_lest14
    horse_features."入厩35日以上", -- horse_rest_over35
    horse_features."馬体重", -- declared_weight
    horse_features."馬体重増減", -- diff_declared_weight (todo: check if this matches lag)
    horse_features."距離", -- distance
    horse_features."前走距離差", -- diff_distance
    horse_features."年齢", -- horse_age (years)
    horse_features."4歳以下",
    horse_features."4歳以下頭数",
    horse_features."4歳以下割合",
    horse_features."レース数", -- horse_runs
    horse_features."1位完走", -- horse_wins
    horse_features."トップ3完走", -- horse_places
    horse_features."1位完走率",
    horse_features."トップ3完走率",
    horse_features."過去5走勝率",
    horse_features."過去5走トップ3完走率",
    horse_features."場所レース数", -- horse_venue_runs
    horse_features."場所1位完走", -- horse_venue_wins
    horse_features."場所トップ3完走", -- horse_venue_places
    horse_features."場所1位完走率", -- ratio_win_horse_venue
    horse_features."場所トップ3完走率", -- ratio_place_horse_venue
    horse_features."トラック種別レース数", -- horse_surface_runs
    horse_features."トラック種別1位完走", -- horse_surface_wins
    horse_features."トラック種別トップ3完走", -- horse_surface_places
    horse_features."トラック種別1位完走率", -- ratio_win_horse_surface
    horse_features."トラック種別トップ3完走率", -- ratio_place_horse_surface
    horse_features."馬場状態レース数", -- horse_going_runs
    horse_features."馬場状態1位完走", -- horse_going_wins
    horse_features."馬場状態トップ3完走", -- horse_going_places
    horse_features."馬場状態1位完走率", -- ratio_win_horse_going
    horse_features."馬場状態トップ3完走率", -- ratio_place_horse_going
    horse_features."距離レース数", -- horse_distance_runs
    horse_features."距離1位完走", -- horse_distance_wins
    horse_features."距離トップ3完走", -- horse_distance_places
    horse_features."距離1位完走率", -- ratio_win_horse_distance
    horse_features."距離トップ3完走率", -- ratio_place_horse_distance
    horse_features."四半期レース数", -- horse_quarter_runs
    horse_features."四半期1位完走", -- horse_quarter_wins
    horse_features."四半期トップ3完走", -- horse_quarter_places
    horse_features."四半期1位完走率", -- ratio_win_horse_quarter
    horse_features."四半期トップ3完走率", -- ratio_place_horse_quarter
    horse_features."過去3走順位平方和", -- horse_std_rank
    horse_features."本賞金累計", -- prize_horse_cumulative
    horse_features."1位完走平均賞金", -- avg_prize_wins_horse
    horse_features."レース数平均賞金", -- avg_prize_runs_horse
    horse_features."瞬発戦好走馬_芝",
    horse_features."消耗戦好走馬_芝",
    horse_features."瞬発戦好走馬_ダート",
    horse_features."消耗戦好走馬_ダート",
    horse_features."瞬発戦好走馬_総合",
    horse_features."消耗戦好走馬_総合",
    horse_features."連続1着",
    horse_features."連続3着以内",

    jockey_features."騎手レース数", -- jockey_runs
    jockey_features."騎手1位完走", -- jockey_wins
    jockey_features."騎手トップ3完走", -- jockey_places
    jockey_features."騎手1位完走率", -- ratio_win_jockey
    jockey_features."騎手トップ3完走率", -- ratio_place_jockey
    jockey_features."騎手過去5走勝率",
    jockey_features."騎手過去5走トップ3完走率",
    jockey_features."騎手場所レース数", -- jockey_venue_runs
    jockey_features."騎手場所1位完走", -- jockey_venue_wins
    jockey_features."騎手場所トップ3完走", -- jockey_venue_places
    jockey_features."騎手場所1位完走率", -- ratio_win_jockey_venue
    jockey_features."騎手場所トップ3完走率", -- ratio_place_jockey_venue
    jockey_features."騎手距離レース数", -- jockey_distance_runs
    jockey_features."騎手距離1位完走", -- jockey_distance_wins
    jockey_features."騎手距離トップ3完走", -- jockey_distance_places
    jockey_features."騎手距離1位完走率", -- ratio_win_jockey_distance
    jockey_features."騎手距離トップ3完走率", -- ratio_place_jockey_distance
    jockey_features."騎手本賞金累計", -- prize_jockey_cumulative
    jockey_features."騎手1位完走平均賞金", -- avg_prize_wins_jockey
    jockey_features."騎手レース数平均賞金", -- avg_prize_runs_jockey

    trainer_features."調教師レース数", -- trainer_runs
    trainer_features."調教師1位完走", -- trainer_wins
    trainer_features."調教師トップ3完走", -- trainer_places
    trainer_features."調教師1位完走率", -- ratio_win_trainer
    trainer_features."調教師トップ3完走率", -- ratio_place_trainer
    trainer_features."調教師場所レース数", -- trainer_venue_runs
    trainer_features."調教師場所1位完走", -- trainer_venue_wins
    trainer_features."調教師場所トップ3完走", -- trainer_venue_places
    trainer_features."調教師場所1位完走率", -- ratio_win_trainer_venue
    trainer_features."調教師場所トップ3完走率", -- ratio_place_trainer_venue
    trainer_features."調教師本賞金累計", -- prize_trainer_cumulative
    trainer_features."調教師1位完走平均賞金", -- avg_prize_wins_trainer
    trainer_features."調教師レース数平均賞金", -- avg_prize_runs_trainer

    combined_features."馬騎手レース数", -- runs_horse_jockey
    combined_features."馬騎手1位完走", -- wins_horse_jockey
    combined_features."馬騎手1位完走率", -- ratio_win_horse_jockey
    combined_features."馬騎手トップ3完走", -- places_horse_jockey
    combined_features."馬騎手トップ3完走率", -- ratio_place_horse_jockey
    combined_features."馬騎手初二走", -- first_second_jockey
    combined_features."馬騎手同騎手", -- same_last_jockey
    combined_features."馬騎手場所レース数", -- runs_horse_jockey_venue
    combined_features."馬騎手場所1位完走", -- wins_horse_jockey_venue
    combined_features."馬騎手場所1位完走率", -- ratio_win_horse_jockey_venue
    combined_features."馬騎手場所トップ3完走", -- places_horse_jockey_venue
    combined_features."馬騎手場所トップ3完走率", -- ratio_place_horse_jockey_venue
    combined_features."馬調教師レース数", -- runs_horse_trainer
    combined_features."馬調教師1位完走", -- wins_horse_trainer
    combined_features."馬調教師1位完走率", -- ratio_win_horse_trainer
    combined_features."馬調教師トップ3完走", -- places_horse_trainer
    combined_features."馬調教師トップ3完走率", -- ratio_place_horse_trainer
    combined_features."馬調教師初二走", -- first_second_trainer
    combined_features."馬調教師同調教師", -- same_last_trainer
    combined_features."馬調教師場所レース数", -- runs_horse_trainer_venue
    combined_features."馬調教師場所1位完走", -- wins_horse_trainer_venue
    combined_features."馬調教師場所1位完走率", -- ratio_win_horse_trainer_venue
    combined_features."馬調教師場所トップ3完走", -- places_horse_trainer_venue
    combined_features."馬調教師場所トップ3完走率", -- ratio_place_horse_trainer_venue

    competitors."競争相手最高IDM",
    competitors."競争相手最低IDM",
    competitors."競争相手平均IDM",
    competitors."競争相手IDM標準偏差",
    competitors."競争相手脚質逃げ割合",
    competitors."競争相手脚質先行割合",
    competitors."競争相手脚質追込割合",
    competitors."競争相手脚質自在割合",
    competitors."競争相手最高単勝オッズ",
    competitors."競争相手最低単勝オッズ",
    competitors."競争相手平均単勝オッズ",
    competitors."競争相手単勝オッズ標準偏差",
    competitors."競争相手最高複勝オッズ",
    competitors."競争相手最低複勝オッズ",
    competitors."競争相手平均複勝オッズ",
    competitors."競争相手複勝オッズ標準偏差",
    competitors."競争相手最高騎手指数",
    competitors."競争相手最低騎手指数",
    competitors."競争相手平均騎手指数",
    competitors."競争相手騎手指数標準偏差",
    competitors."競争相手最高情報指数",
    competitors."競争相手最低情報指数",
    competitors."競争相手平均情報指数",
    competitors."競争相手情報指数標準偏差",
    competitors."競争相手最高オッズ指数",
    competitors."競争相手最低オッズ指数",
    competitors."競争相手平均オッズ指数",
    competitors."競争相手オッズ指数標準偏差",
    competitors."競争相手最高パドック指数",
    competitors."競争相手最低パドック指数",
    competitors."競争相手平均パドック指数",
    competitors."競争相手パドック指数標準偏差",
    competitors."競争相手最高総合指数",
    competitors."競争相手最低総合指数",
    competitors."競争相手平均総合指数",
    competitors."競争相手総合指数標準偏差",
    competitors."競争相手馬具変更情報0割合",
    competitors."競争相手馬具変更情報1割合",
    competitors."競争相手馬具変更情報2割合",
    competitors."競争相手最高負担重量",
    competitors."競争相手最低負担重量",
    competitors."競争相手平均負担重量",
    competitors."競争相手負担重量標準偏差",
    competitors."競争相手最高見習い区分",
    competitors."競争相手最低見習い区分",
    competitors."競争相手平均見習い区分",
    competitors."競争相手見習い区分標準偏差",
    competitors."競争相手オッズ印1割合",
    competitors."競争相手オッズ印2割合",
    competitors."競争相手オッズ印3割合",
    competitors."競争相手オッズ印4割合",
    competitors."競争相手パドック印1割合",
    competitors."競争相手パドック印2割合",
    competitors."競争相手パドック印3割合",
    competitors."競争相手パドック印4割合",
    competitors."競争相手直前総合印1割合",
    competitors."競争相手直前総合印2割合",
    competitors."競争相手直前総合印3割合",
    competitors."競争相手直前総合印4割合",
    competitors."競争相手距離適性1割合",
    competitors."競争相手距離適性2割合",
    competitors."競争相手距離適性3割合",
    competitors."競争相手距離適性5割合",
    competitors."競争相手距離適性6割合",
    competitors."競争相手最高上昇度",
    competitors."競争相手最低上昇度",
    competitors."競争相手平均上昇度",
    competitors."競争相手上昇度標準偏差",
    competitors."競争相手最高ローテーション",
    competitors."競争相手最低ローテーション",
    competitors."競争相手平均ローテーション",
    competitors."競争相手ローテーション標準偏差",
    competitors."競争相手最高基準オッズ",
    competitors."競争相手最低基準オッズ",
    competitors."競争相手平均基準オッズ",
    competitors."競争相手基準オッズ標準偏差",
    competitors."競争相手最高基準複勝オッズ",
    competitors."競争相手最低基準複勝オッズ",
    competitors."競争相手平均基準複勝オッズ",
    competitors."競争相手基準複勝オッズ標準偏差",
    competitors."競争相手最高人気指数",
    competitors."競争相手最低人気指数",
    competitors."競争相手平均人気指数",
    competitors."競争相手人気指数標準偏差",
    competitors."競争相手最高調教指数",
    competitors."競争相手最低調教指数",
    competitors."競争相手平均調教指数",
    competitors."競争相手調教指数標準偏差",
    competitors."競争相手最高厩舎指数",
    competitors."競争相手最低厩舎指数",
    competitors."競争相手平均厩舎指数",
    competitors."競争相手厩舎指数標準偏差",
    competitors."競争相手最高激走指数",
    competitors."競争相手最低激走指数",
    competitors."競争相手平均激走指数",
    competitors."競争相手激走指数標準偏差",
    competitors."競争相手ブリンカー1割合",
    competitors."競争相手ブリンカー2割合",
    competitors."競争相手ブリンカー3割合",
    competitors."競争相手最高展開予想データ_テン指数",
    competitors."競争相手最低展開予想データ_テン指数",
    competitors."競争相手平均展開予想データ_テン指数",
    competitors."競争相手展開予想データ_テン指数標準偏差",
    competitors."競争相手最高展開予想データ_ペース指数",
    competitors."競争相手最低展開予想データ_ペース指数",
    competitors."競争相手平均展開予想データ_ペース指数",
    competitors."競争相手展開予想データ_ペース指数標準偏差",
    competitors."競争相手最高展開予想データ_上がり指数",
    competitors."競争相手最低展開予想データ_上がり指数",
    competitors."競争相手平均展開予想データ_上がり指数",
    competitors."競争相手展開予想データ_上がり指数標準偏差",
    competitors."競争相手最高展開予想データ_位置指数",
    competitors."競争相手最低展開予想データ_位置指数",
    competitors."競争相手平均展開予想データ_位置指数",
    competitors."競争相手展開予想データ_位置指数標準偏差",
    competitors."競争相手展開予想データ_ペース予想H割合",
    competitors."競争相手展開予想データ_ペース予想M割合",
    competitors."競争相手展開予想データ_ペース予想S割合",
    competitors."競争相手最高騎手期待単勝率",
    competitors."競争相手最低騎手期待単勝率",
    competitors."競争相手平均騎手期待単勝率",
    competitors."競争相手騎手期待単勝率標準偏差",
    competitors."競争相手最高騎手期待３着内率",
    competitors."競争相手最低騎手期待３着内率",
    competitors."競争相手平均騎手期待３着内率",
    competitors."競争相手騎手期待３着内率標準偏差",
    competitors."競争相手最高展開参考データ_馬スタート指数",
    competitors."競争相手最低展開参考データ_馬スタート指数",
    competitors."競争相手平均展開参考データ_馬スタート指数",
    competitors."競争相手展開参考データ_馬スタート指数標準偏差",
    competitors."競争相手最高展開参考データ_馬出遅率",
    competitors."競争相手最低展開参考データ_馬出遅率",
    competitors."競争相手平均展開参考データ_馬出遅率",
    competitors."競争相手展開参考データ_馬出遅率標準偏差",
    competitors."競争相手最高万券指数",
    competitors."競争相手最低万券指数",
    competitors."競争相手平均万券指数",
    competitors."競争相手万券指数標準偏差",
    competitors."競争相手万券印1割合",
    competitors."競争相手万券印2割合",
    competitors."競争相手万券印3割合",
    competitors."競争相手万券印4割合",
    competitors."競争相手性別牡割合",
    competitors."競争相手性別牝割合",
    competitors."競争相手性別セ割合",
    competitors."競争相手最高一走前着順",
    competitors."競争相手最低一走前着順",
    competitors."競争相手平均一走前着順",
    competitors."競争相手一走前着順標準偏差",
    competitors."競争相手最高二走前着順",
    competitors."競争相手最低二走前着順",
    competitors."競争相手平均二走前着順",
    competitors."競争相手二走前着順標準偏差",
    competitors."競争相手最高三走前着順",
    competitors."競争相手最低三走前着順",
    competitors."競争相手平均三走前着順",
    competitors."競争相手三走前着順標準偏差",
    competitors."競争相手最高四走前着順",
    competitors."競争相手最低四走前着順",
    competitors."競争相手平均四走前着順",
    competitors."競争相手四走前着順標準偏差",
    competitors."競争相手最高五走前着順",
    competitors."競争相手最低五走前着順",
    competitors."競争相手平均五走前着順",
    competitors."競争相手五走前着順標準偏差",
    competitors."競争相手最高六走前着順",
    competitors."競争相手最低六走前着順",
    competitors."競争相手平均六走前着順",
    competitors."競争相手六走前着順標準偏差",
    competitors."競争相手前走トップ3割合",
    competitors."競争相手最高入厩何日前",
    competitors."競争相手最低入厩何日前",
    competitors."競争相手平均入厩何日前",
    competitors."競争相手入厩何日前標準偏差",
    competitors."競争相手最高馬体重",
    competitors."競争相手最低馬体重",
    competitors."競争相手平均馬体重",
    competitors."競争相手馬体重標準偏差",
    competitors."競争相手最高馬体重増減",
    competitors."競争相手最低馬体重増減",
    competitors."競争相手平均馬体重増減",
    competitors."競争相手馬体重増減標準偏差",
    competitors."競争相手最高年齢",
    competitors."競争相手最低年齢",
    competitors."競争相手平均年齢",
    competitors."競争相手年齢標準偏差",
    competitors."競争相手最高レース数",
    competitors."競争相手最低レース数",
    competitors."競争相手平均レース数",
    competitors."競争相手レース数標準偏差",
    competitors."競争相手最高1位完走",
    competitors."競争相手最低1位完走",
    competitors."競争相手平均1位完走",
    competitors."競争相手1位完走標準偏差",
    competitors."競争相手最高トップ3完走",
    competitors."競争相手最低トップ3完走",
    competitors."競争相手平均トップ3完走",
    competitors."競争相手トップ3完走標準偏差",
    competitors."競争相手最高1位完走率",
    competitors."競争相手最低1位完走率",
    competitors."競争相手平均1位完走率",
    competitors."競争相手1位完走率標準偏差",
    competitors."競争相手最高トップ3完走率",
    competitors."競争相手最低トップ3完走率",
    competitors."競争相手平均トップ3完走率",
    competitors."競争相手トップ3完走率標準偏差",
    competitors."競争相手最高過去5走勝率",
    competitors."競争相手最低過去5走勝率",
    competitors."競争相手平均過去5走勝率",
    competitors."競争相手過去5走勝率標準偏差",
    competitors."競争相手最高過去5走トップ3完走率",
    competitors."競争相手最低過去5走トップ3完走率",
    competitors."競争相手平均過去5走トップ3完走率",
    competitors."競争相手過去5走トップ3完走率標準偏差",
    competitors."競争相手最高場所レース数",
    competitors."競争相手最低場所レース数",
    competitors."競争相手平均場所レース数",
    competitors."競争相手場所レース数標準偏差",
    competitors."競争相手最高場所1位完走",
    competitors."競争相手最低場所1位完走",
    competitors."競争相手平均場所1位完走",
    competitors."競争相手場所1位完走標準偏差",
    competitors."競争相手最高場所トップ3完走",
    competitors."競争相手最低場所トップ3完走",
    competitors."競争相手平均場所トップ3完走",
    competitors."競争相手場所トップ3完走標準偏差",
    competitors."競争相手最高場所1位完走率",
    competitors."競争相手最低場所1位完走率",
    competitors."競争相手平均場所1位完走率",
    competitors."競争相手場所1位完走率標準偏差",
    competitors."競争相手最高場所トップ3完走率",
    competitors."競争相手最低場所トップ3完走率",
    competitors."競争相手平均場所トップ3完走率",
    competitors."競争相手場所トップ3完走率標準偏差",
    competitors."競争相手最高トラック種別レース数",
    competitors."競争相手最低トラック種別レース数",
    competitors."競争相手平均トラック種別レース数",
    competitors."競争相手トラック種別レース数標準偏差",
    competitors."競争相手最高トラック種別1位完走",
    competitors."競争相手最低トラック種別1位完走",
    competitors."競争相手平均トラック種別1位完走",
    competitors."競争相手トラック種別1位完走標準偏差",
    competitors."競争相手最高トラック種別トップ3完走",
    competitors."競争相手最低トラック種別トップ3完走",
    competitors."競争相手平均トラック種別トップ3完走",
    competitors."競争相手トラック種別トップ3完走標準偏差",
    competitors."競争相手最高馬場状態レース数",
    competitors."競争相手最低馬場状態レース数",
    competitors."競争相手平均馬場状態レース数",
    competitors."競争相手馬場状態レース数標準偏差",
    competitors."競争相手最高馬場状態1位完走",
    competitors."競争相手最低馬場状態1位完走",
    competitors."競争相手平均馬場状態1位完走",
    competitors."競争相手馬場状態1位完走標準偏差",
    competitors."競争相手最高馬場状態トップ3完走",
    competitors."競争相手最低馬場状態トップ3完走",
    competitors."競争相手平均馬場状態トップ3完走",
    competitors."競争相手馬場状態トップ3完走標準偏差",
    competitors."競争相手最高距離レース数",
    competitors."競争相手最低距離レース数",
    competitors."競争相手平均距離レース数",
    competitors."競争相手距離レース数標準偏差",
    competitors."競争相手最高距離1位完走",
    competitors."競争相手最低距離1位完走",
    competitors."競争相手平均距離1位完走",
    competitors."競争相手距離1位完走標準偏差",
    competitors."競争相手最高距離トップ3完走",
    competitors."競争相手最低距離トップ3完走",
    competitors."競争相手平均距離トップ3完走",
    competitors."競争相手距離トップ3完走標準偏差",
    competitors."競争相手最高四半期レース数",
    competitors."競争相手最低四半期レース数",
    competitors."競争相手平均四半期レース数",
    competitors."競争相手四半期レース数標準偏差",
    competitors."競争相手最高四半期1位完走",
    competitors."競争相手最低四半期1位完走",
    competitors."競争相手平均四半期1位完走",
    competitors."競争相手四半期1位完走標準偏差",
    competitors."競争相手最高四半期トップ3完走",
    competitors."競争相手最低四半期トップ3完走",
    competitors."競争相手平均四半期トップ3完走",
    competitors."競争相手四半期トップ3完走標準偏差",
    competitors."競争相手最高過去3走順位平方和",
    competitors."競争相手最低過去3走順位平方和",
    competitors."競争相手平均過去3走順位平方和",
    competitors."競争相手過去3走順位平方和標準偏差",
    competitors."競争相手最高本賞金累計",
    competitors."競争相手最低本賞金累計",
    competitors."競争相手平均本賞金累計",
    competitors."競争相手本賞金累計標準偏差",
    competitors."競争相手最高1位完走平均賞金",
    competitors."競争相手最低1位完走平均賞金",
    competitors."競争相手平均1位完走平均賞金",
    competitors."競争相手1位完走平均賞金標準偏差",
    competitors."競争相手最高レース数平均賞金",
    competitors."競争相手最低レース数平均賞金",
    competitors."競争相手平均レース数平均賞金",
    competitors."競争相手レース数平均賞金標準偏差",
    competitors."競争相手瞬発戦好走馬_芝割合",
    competitors."競争相手消耗戦好走馬_芝割合",
    competitors."競争相手瞬発戦好走馬_ダート割合",
    competitors."競争相手消耗戦好走馬_ダート割合",
    competitors."競争相手瞬発戦好走馬_総合割合",
    competitors."競争相手消耗戦好走馬_総合割合",
    competitors."競争相手最高連続1着",
    competitors."競争相手最低連続1着",
    competitors."競争相手平均連続1着",
    competitors."競争相手連続1着標準偏差",
    competitors."競争相手最高連続3着以内",
    competitors."競争相手最低連続3着以内",
    competitors."競争相手平均連続3着以内",
    competitors."競争相手連続3着以内標準偏差",

    race_weather.temperature,
    race_weather.precipitation,
    race_weather.snowfall,
    race_weather.snow_depth,
    race_weather.wind_speed,
    race_weather.wind_direction,
    race_weather.solar_radiation,
    race_weather.local_air_pressure,
    race_weather.sea_level_air_pressure,
    race_weather.relative_humidity,
    race_weather.vapor_pressure,
    race_weather.dew_point_temperature,
    race_weather.weather,
    race_weather.visibility

  from
    base
  inner join
    {{ ref("int_race_horses") }} horse_features
  on
    base."レースキー" = horse_features."レースキー"
    and base."馬番" = horse_features."馬番"
  inner join
    {{ ref("int_race_jockeys") }} jockey_features
  on
    base."レースキー" = jockey_features."レースキー"
    and base."馬番" = jockey_features."馬番"
  inner join
    {{ ref("int_race_trainers") }} trainer_features
  on
    base."レースキー" = trainer_features."レースキー"
    and base."馬番" = trainer_features."馬番"
  inner join
    combined_features
  on
    base."レースキー" = combined_features."レースキー"
    and base."馬番" = combined_features."馬番"
  inner join
    race_weather
  on
    base."レースキー" = race_weather."レースキー"
  inner join
    competitors
  on
    base."レースキー" = competitors."レースキー"
    and base."馬番" = competitors."馬番"
  )

select
  *
from
  final
