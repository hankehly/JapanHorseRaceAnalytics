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
    kyi."脚質",
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
    tyb."馬体コード",
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

    -- horse/jockey/venue

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

    -- horse/trainer/venue

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
    base."馬体コード",
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

    -- Relative features --

    -- Average IDM of Competitors: Calculate the average speed rating of all horses in a race and compare each horse's speed rating to this average.
    dbt_utils.safe_divide(
      'sum(base."ＩＤＭ") over (partition by base."レースキー") - base."ＩＤＭ"',
      'cast(base."頭数" - 1 as numeric)'
    ) as "競争相手平均IDM",

    -- Calculate the difference between each horse's IDM and the average IDM of its competitors.
    base."ＩＤＭ" - dbt_utils.safe_divide(
      'sum(base."ＩＤＭ") over (partition by base."レースキー") - base."ＩＤＭ"',
      'cast(base."頭数" - 1 as numeric)'
    ) AS "競争相手平均IDM差",

    -- This calculates the average streak of competitors for each horse, excluding its own streak from the average.
    dbt_utils.safe_divide(
      'sum(horse_features."連続1着") over (partition by base."レースキー") - horse_features."連続1着"',
      'cast(base."頭数" - 1 as numeric)'
    ) as "競争相手平均連続1着",

    -- Calculate the difference between each horse's streak and the average or maximum streak of its competitors.
    -- This could indicate how much stronger or weaker the horse is relative to the field.
    horse_features."連続1着" - dbt_utils.safe_divide(
      'sum(horse_features."連続1着") over (partition by base."レースキー") - horse_features."連続1着"',
      'cast(base."頭数" - 1 as numeric)'
    ) AS "競争相手平均連続1着差",

    -- Todo

    -- X:
    -- competitor mean
    -- competitor max
    -- competitor min
    -- competitor std
    -- difference from competitor mean

    -- Y:
    -- do for all horse, jockey, trainer, combined features, select the ones with highest feature importance
    -- and eliminate ones with high correlation

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
  )

select
  *
from
  final
