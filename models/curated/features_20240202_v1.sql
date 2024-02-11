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
    -- Metadata (do not use for training)
    kyi.`レースキー`,
    kyi.`馬番`,
    kyi.`血統登録番号`,
    coalesce(tyb.`騎手コード`, kyi.`騎手コード`) as `騎手コード`,
    bac.`発走日時`,
    kyi.`レースキー_場コード` as `場コード`,
    kyi.`調教師コード`,
    sed.`馬成績_着順` as `着順`,
    sed.`本賞金`,
    coalesce(win_payouts.`払戻金`, 0) > 0 as `単勝的中`,
    coalesce(win_payouts.`払戻金`, 0) as `単勝払戻金`,
    coalesce(place_payouts.`払戻金`, 0) > 0 as `複勝的中`,
    coalesce(place_payouts.`払戻金`, 0) as `複勝払戻金`,

    -- Base features
    case
      when extract(month from bac.`発走日時`) <= 3 then 1
      when extract(month from bac.`発走日時`) <= 6 then 2
      when extract(month from bac.`発走日時`) <= 9 then 3
      when extract(month from bac.`発走日時`) <= 12 then 4
    end as `四半期`,
    bac.`レース条件_距離` as `距離`,
    coalesce(
      tyb.`馬場状態コード`,
      case
        when bac.`レース条件_トラック情報_芝ダ障害コード` = 'ダート' then kab.`ダ馬場状態コード`
        -- 障害コースの場合は芝馬場状態を使用する
        else kab.`芝馬場状態コード`
      end
    ) as `馬場状態コード`,
    bac.`レース条件_トラック情報_右左`,
    bac.`レース条件_トラック情報_内外`,
    bac.`レース条件_種別`,
    bac.`レース条件_条件`,
    bac.`レース条件_記号`,
    bac.`レース条件_重量`,
    bac.`レース条件_グレード`,
    bac.`頭数`,
    bac.`レース条件_トラック情報_芝ダ障害コード` as `トラック種別`,
    case
      when bac.`レース条件_トラック情報_芝ダ障害コード` = 'ダート' then kab.`ダ馬場差`
      -- 障害コースの場合は芝馬場差を使用する
      else kab.`芝馬場差`
    end `馬場差`,
    kab.`芝馬場状態内`,
    kab.`芝馬場状態中`,
    kab.`芝馬場状態外`,
    kab.`直線馬場差最内`,
    kab.`直線馬場差内`,
    kab.`直線馬場差中`,
    kab.`直線馬場差外`,
    kab.`直線馬場差大外`,
    kab.`ダ馬場状態内`,
    kab.`ダ馬場状態中`,
    kab.`ダ馬場状態外`,
    kab.`芝種類`,
    kab.`草丈`,
    kab.`転圧`,
    kab.`凍結防止剤`,
    kab.`中間降水量`,
    coalesce(tyb.`ＩＤＭ`, kyi.`ＩＤＭ`) as `ＩＤＭ`,
    -- https://note.com/jrdbn/n/n0b3d06e39768
    coalesce(stddev_pop(coalesce(tyb.`ＩＤＭ`, kyi.`ＩＤＭ`)) over (partition by kyi.`レースキー`), 0) as `IDM標準偏差`,
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
    kyi.`厩舎ランク`,
    coalesce(tyb.`天候コード`, kab.`天候コード`) as `天候コード`
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
    win_odds
  on
    kyi.`レースキー` = win_odds.`レースキー`
    and kyi.`馬番` = win_odds.`馬番`

  inner join
    place_odds
  on
    kyi.`レースキー` = place_odds.`レースキー`
    and kyi.`馬番` = place_odds.`馬番`

  left join
    win_payouts
  on
    kyi.`レースキー` = win_payouts.`レースキー`
    and kyi.`馬番` = win_payouts.`馬番`

  left join
    place_payouts
  on
    kyi.`レースキー` = place_payouts.`レースキー`
    and kyi.`馬番` = place_payouts.`馬番`

  left join
    {{ ref('jrdb__horse_form_codes') }} horse_form_codes
  on
    tyb.`馬体コード` = horse_form_codes.`code`

  left join
    {{ ref('jrdb__run_style_codes') }} run_style_codes
  on
    kyi.`脚質` = run_style_codes.`code`
  ),

  combined_features as (
  select
    base.`レースキー`,
    base.`馬番`,

    -- runs_horse_jockey
    coalesce(cast(count(*) over (partition by `血統登録番号`, `騎手コード` order by `発走日時`) - 1 as integer), 0) as `馬騎手レース数`,

    -- wins_horse_jockey
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬騎手1位完走`,

    -- ratio_win_horse_jockey
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `騎手コード` order by `発走日時`) - 1 as numeric)'
      )
    }}, 0) as `馬騎手1位完走率`,

    -- places_horse_jockey
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬騎手トップ3完走`,

    -- ratio_place_horse_jockey
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `騎手コード` order by `発走日時`) - 1 as numeric)'
      )
    }}, 0) as `馬騎手トップ3完走率`,

    -- first_second_jockey
    case when cast(count(*) over (partition by `血統登録番号`, `騎手コード` order by `発走日時`) - 1 as integer) < 2 then true else false end as `馬騎手初二走`,

    -- same_last_jockey (horse jockey combination was same last race)
    case when lag(`騎手コード`) over (partition by `血統登録番号` order by `発走日時`) = `騎手コード` then true else false end as `馬騎手同騎手`,

    -- runs_horse_jockey_venue
    coalesce(cast(count(*) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時`) - 1 as integer), 0) as `馬騎手場所レース数`,

    -- wins_horse_jockey_venue
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬騎手場所1位完走`,

    -- ratio_win_horse_jockey_venue
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時`) - 1 as numeric)'
      )
    }}, 0) as `馬騎手場所1位完走率`,

    -- places_horse_jockey_venue
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬騎手場所トップ3完走`,

    -- ratio_place_horse_jockey_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時`) - 1 as numeric)'
      )
    }}, 0) as `馬騎手場所トップ3完走率`,

    -- runs_horse_trainer
    coalesce(cast(count(*) over (partition by `血統登録番号`, `調教師コード` order by `発走日時`) - 1 as integer), 0) as `馬調教師レース数`,

    -- wins_horse_trainer
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬調教師1位完走`,

    -- ratio_win_horse_trainer
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `調教師コード` order by `発走日時`) - 1 as numeric)'
      )
    }}, 0) as `馬調教師1位完走率`,

    -- places_horse_trainer
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬調教師トップ3完走`,

    -- ratio_place_horse_trainer
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `調教師コード` order by `発走日時`) - 1 as numeric)'
      )
    }}, 0) as `馬調教師トップ3完走率`,

    -- first_second_trainer
    case when cast(count(*) over (partition by `血統登録番号`, `調教師コード` order by `発走日時`) - 1 as integer) < 2 then true else false end as `馬調教師初二走`,

    -- same_last_trainer
    case when lag(`調教師コード`) over (partition by `血統登録番号` order by `発走日時`) = `調教師コード` then true else false end as `馬調教師同調教師`,

    -- runs_horse_trainer_venue
    coalesce(cast(count(*) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時`) - 1 as integer), 0) as `馬調教師場所レース数`,

    -- wins_horse_trainer_venue
    coalesce(cast(sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬調教師場所1位完走`,

    -- ratio_win_horse_trainer_venue
    coalesce({{
      dbt_utils.safe_divide(
        'sum(case when `着順` = 1 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時`) - 1 as numeric)'
      )
    }}, 0) as `馬調教師場所1位完走率`,

    -- places_horse_trainer_venue
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬調教師場所トップ3完走`,

    -- ratio_place_horse_trainer_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時`) - 1 as numeric)'
      )
    }}, 0) as `馬調教師場所トップ3完走率`
  from
    base
  ),

  final as (
  select
    -- Metadata (do not use for training)
    base.`レースキー`,
    base.`馬番`,
    base.`血統登録番号`,
    base.`騎手コード`,
    base.`発走日時`,
    base.`場コード`,
    base.`調教師コード`,
    base.`着順` as `先読み注意_着順`,
    base.`本賞金` as `先読み注意_本賞金`,
    base.`単勝的中` as `先読み注意_単勝的中`,
    base.`単勝払戻金` as `先読み注意_単勝払戻金`,
    base.`複勝的中` as `先読み注意_複勝的中`,
    base.`複勝払戻金` as `先読み注意_複勝払戻金`,

    -- Base features
    base.`四半期`,
    base.`距離`,
    base.`馬場状態コード`,
    base.`レース条件_トラック情報_右左`,
    base.`レース条件_トラック情報_内外`,
    base.`レース条件_種別`,
    base.`レース条件_条件`,
    base.`レース条件_記号`,
    base.`レース条件_重量`,
    base.`レース条件_グレード`,
    base.`頭数`,
    base.`トラック種別`,
    base.`馬場差`,
    base.`芝馬場状態内`,
    base.`芝馬場状態中`,
    base.`芝馬場状態外`,
    base.`直線馬場差最内`,
    base.`直線馬場差内`,
    base.`直線馬場差中`,
    base.`直線馬場差外`,
    base.`直線馬場差大外`,
    base.`ダ馬場状態内`,
    base.`ダ馬場状態中`,
    base.`ダ馬場状態外`,
    base.`芝種類`,
    base.`草丈`,
    base.`転圧`,
    base.`凍結防止剤`,
    base.`中間降水量`,
    base.`ＩＤＭ`,
    base.`IDM標準偏差`,
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
    base.`天候コード`,

    -- Combination of IDM and IDM標準偏差 to express horse IDM in comparison to standard deviation.
    -- coalesce( dbt_utils.safe_divide('base.`ＩＤＭ`', 'base.`IDM標準偏差`') , 0) as `IDM_標準偏差比`,

   -- Horse features
    race_horses.`一走前着順`,
    race_horses.`二走前着順`,
    race_horses.`三走前着順`,
    race_horses.`四走前着順`,
    race_horses.`五走前着順`,
    race_horses.`六走前着順`,
    race_horses.`前走トップ3`,
    race_horses.`枠番`,
    race_horses.`前走枠番`,
    race_horses.`入厩何日前`,
    race_horses.`入厩15日未満`,
    race_horses.`入厩35日以上`,
    race_horses.`馬体重`,
    race_horses.`馬体重増減`,
    race_horses.`前走距離差`,
    race_horses.`年齢`,
    race_horses.`4歳以下`,
    race_horses.`4歳以下頭数`,
    race_horses.`4歳以下割合`,
    race_horses.`レース数`,
    race_horses.`1位完走`,
    race_horses.`トップ3完走`,
    race_horses.`1位完走率`,
    race_horses.`トップ3完走率`,
    race_horses.`過去5走勝率`,
    race_horses.`過去5走トップ3完走率`,
    race_horses.`場所レース数`,
    race_horses.`場所1位完走`,
    race_horses.`場所トップ3完走`,
    race_horses.`場所1位完走率`,
    race_horses.`場所トップ3完走率`,
    race_horses.`トラック種別レース数`,
    race_horses.`トラック種別1位完走`,
    race_horses.`トラック種別トップ3完走`,
    race_horses.`トラック種別1位完走率`,
    race_horses.`トラック種別トップ3完走率`,
    race_horses.`馬場状態レース数`,
    race_horses.`馬場状態1位完走`,
    race_horses.`馬場状態トップ3完走`,
    race_horses.`馬場状態1位完走率`,
    race_horses.`馬場状態トップ3完走率`,
    race_horses.`距離レース数`,
    race_horses.`距離1位完走`,
    race_horses.`距離トップ3完走`,
    race_horses.`距離1位完走率`,
    race_horses.`距離トップ3完走率`,
    race_horses.`四半期レース数`,
    race_horses.`四半期1位完走`,
    race_horses.`四半期トップ3完走`,
    race_horses.`四半期1位完走率`,
    race_horses.`四半期トップ3完走率`,
    race_horses.`過去3走順位平方和`,
    race_horses.`本賞金累計`,
    race_horses.`1位完走平均賞金`,
    race_horses.`レース数平均賞金`,
    race_horses.`連続1着`,
    race_horses.`連続3着内`,
    race_horses.`性別`,
    race_horses.`瞬発戦好走馬_芝`,
    race_horses.`消耗戦好走馬_芝`,
    race_horses.`瞬発戦好走馬_ダート`,
    race_horses.`消耗戦好走馬_ダート`,
    race_horses.`瞬発戦好走馬_総合`,
    race_horses.`消耗戦好走馬_総合`,
    race_horses.`競争相手性別牡割合`,
    race_horses.`競争相手性別牝割合`,
    race_horses.`競争相手性別セ割合`,
    race_horses.`競争相手最高一走前着順`,
    race_horses.`競争相手最低一走前着順`,
    race_horses.`競争相手平均一走前着順`,
    race_horses.`競争相手一走前着順標準偏差`,
    race_horses.`競争相手最高二走前着順`,
    race_horses.`競争相手最低二走前着順`,
    race_horses.`競争相手平均二走前着順`,
    race_horses.`競争相手二走前着順標準偏差`,
    race_horses.`競争相手最高三走前着順`,
    race_horses.`競争相手最低三走前着順`,
    race_horses.`競争相手平均三走前着順`,
    race_horses.`競争相手三走前着順標準偏差`,
    race_horses.`競争相手最高四走前着順`,
    race_horses.`競争相手最低四走前着順`,
    race_horses.`競争相手平均四走前着順`,
    race_horses.`競争相手四走前着順標準偏差`,
    race_horses.`競争相手最高五走前着順`,
    race_horses.`競争相手最低五走前着順`,
    race_horses.`競争相手平均五走前着順`,
    race_horses.`競争相手五走前着順標準偏差`,
    race_horses.`競争相手最高六走前着順`,
    race_horses.`競争相手最低六走前着順`,
    race_horses.`競争相手平均六走前着順`,
    race_horses.`競争相手六走前着順標準偏差`,
    race_horses.`競争相手前走トップ3割合`,
    race_horses.`競争相手最高入厩何日前`,
    race_horses.`競争相手最低入厩何日前`,
    race_horses.`競争相手平均入厩何日前`,
    race_horses.`競争相手入厩何日前標準偏差`,
    race_horses.`競争相手最高馬体重`,
    race_horses.`競争相手最低馬体重`,
    race_horses.`競争相手平均馬体重`,
    race_horses.`競争相手馬体重標準偏差`,
    race_horses.`競争相手最高馬体重増減`,
    race_horses.`競争相手最低馬体重増減`,
    race_horses.`競争相手平均馬体重増減`,
    race_horses.`競争相手馬体重増減標準偏差`,
    race_horses.`競争相手最高年齢`,
    race_horses.`競争相手最低年齢`,
    race_horses.`競争相手平均年齢`,
    race_horses.`競争相手年齢標準偏差`,
    race_horses.`競争相手最高レース数`,
    race_horses.`競争相手最低レース数`,
    race_horses.`競争相手平均レース数`,
    race_horses.`競争相手レース数標準偏差`,
    race_horses.`競争相手最高1位完走`,
    race_horses.`競争相手最低1位完走`,
    race_horses.`競争相手平均1位完走`,
    race_horses.`競争相手1位完走標準偏差`,
    race_horses.`競争相手最高トップ3完走`,
    race_horses.`競争相手最低トップ3完走`,
    race_horses.`競争相手平均トップ3完走`,
    race_horses.`競争相手トップ3完走標準偏差`,
    race_horses.`競争相手最高1位完走率`,
    race_horses.`競争相手最低1位完走率`,
    race_horses.`競争相手平均1位完走率`,
    race_horses.`競争相手1位完走率標準偏差`,
    race_horses.`競争相手最高トップ3完走率`,
    race_horses.`競争相手最低トップ3完走率`,
    race_horses.`競争相手平均トップ3完走率`,
    race_horses.`競争相手トップ3完走率標準偏差`,
    race_horses.`競争相手最高過去5走勝率`,
    race_horses.`競争相手最低過去5走勝率`,
    race_horses.`競争相手平均過去5走勝率`,
    race_horses.`競争相手過去5走勝率標準偏差`,
    race_horses.`競争相手最高過去5走トップ3完走率`,
    race_horses.`競争相手最低過去5走トップ3完走率`,
    race_horses.`競争相手平均過去5走トップ3完走率`,
    race_horses.`競争相手過去5走トップ3完走率標準偏差`,
    race_horses.`競争相手最高場所レース数`,
    race_horses.`競争相手最低場所レース数`,
    race_horses.`競争相手平均場所レース数`,
    race_horses.`競争相手場所レース数標準偏差`,
    race_horses.`競争相手最高場所1位完走`,
    race_horses.`競争相手最低場所1位完走`,
    race_horses.`競争相手平均場所1位完走`,
    race_horses.`競争相手場所1位完走標準偏差`,
    race_horses.`競争相手最高場所トップ3完走`,
    race_horses.`競争相手最低場所トップ3完走`,
    race_horses.`競争相手平均場所トップ3完走`,
    race_horses.`競争相手場所トップ3完走標準偏差`,
    race_horses.`競争相手最高場所1位完走率`,
    race_horses.`競争相手最低場所1位完走率`,
    race_horses.`競争相手平均場所1位完走率`,
    race_horses.`競争相手場所1位完走率標準偏差`,
    race_horses.`競争相手最高場所トップ3完走率`,
    race_horses.`競争相手最低場所トップ3完走率`,
    race_horses.`競争相手平均場所トップ3完走率`,
    race_horses.`競争相手場所トップ3完走率標準偏差`,
    race_horses.`競争相手最高トラック種別レース数`,
    race_horses.`競争相手最低トラック種別レース数`,
    race_horses.`競争相手平均トラック種別レース数`,
    race_horses.`競争相手トラック種別レース数標準偏差`,
    race_horses.`競争相手最高トラック種別1位完走`,
    race_horses.`競争相手最低トラック種別1位完走`,
    race_horses.`競争相手平均トラック種別1位完走`,
    race_horses.`競争相手トラック種別1位完走標準偏差`,
    race_horses.`競争相手最高トラック種別トップ3完走`,
    race_horses.`競争相手最低トラック種別トップ3完走`,
    race_horses.`競争相手平均トラック種別トップ3完走`,
    race_horses.`競争相手トラック種別トップ3完走標準偏差`,
    race_horses.`競争相手最高馬場状態レース数`,
    race_horses.`競争相手最低馬場状態レース数`,
    race_horses.`競争相手平均馬場状態レース数`,
    race_horses.`競争相手馬場状態レース数標準偏差`,
    race_horses.`競争相手最高馬場状態1位完走`,
    race_horses.`競争相手最低馬場状態1位完走`,
    race_horses.`競争相手平均馬場状態1位完走`,
    race_horses.`競争相手馬場状態1位完走標準偏差`,
    race_horses.`競争相手最高馬場状態トップ3完走`,
    race_horses.`競争相手最低馬場状態トップ3完走`,
    race_horses.`競争相手平均馬場状態トップ3完走`,
    race_horses.`競争相手馬場状態トップ3完走標準偏差`,
    race_horses.`競争相手最高距離レース数`,
    race_horses.`競争相手最低距離レース数`,
    race_horses.`競争相手平均距離レース数`,
    race_horses.`競争相手距離レース数標準偏差`,
    race_horses.`競争相手最高距離1位完走`,
    race_horses.`競争相手最低距離1位完走`,
    race_horses.`競争相手平均距離1位完走`,
    race_horses.`競争相手距離1位完走標準偏差`,
    race_horses.`競争相手最高距離トップ3完走`,
    race_horses.`競争相手最低距離トップ3完走`,
    race_horses.`競争相手平均距離トップ3完走`,
    race_horses.`競争相手距離トップ3完走標準偏差`,
    race_horses.`競争相手最高四半期レース数`,
    race_horses.`競争相手最低四半期レース数`,
    race_horses.`競争相手平均四半期レース数`,
    race_horses.`競争相手四半期レース数標準偏差`,
    race_horses.`競争相手最高四半期1位完走`,
    race_horses.`競争相手最低四半期1位完走`,
    race_horses.`競争相手平均四半期1位完走`,
    race_horses.`競争相手四半期1位完走標準偏差`,
    race_horses.`競争相手最高四半期トップ3完走`,
    race_horses.`競争相手最低四半期トップ3完走`,
    race_horses.`競争相手平均四半期トップ3完走`,
    race_horses.`競争相手四半期トップ3完走標準偏差`,
    race_horses.`競争相手最高過去3走順位平方和`,
    race_horses.`競争相手最低過去3走順位平方和`,
    race_horses.`競争相手平均過去3走順位平方和`,
    race_horses.`競争相手過去3走順位平方和標準偏差`,
    race_horses.`競争相手最高本賞金累計`,
    race_horses.`競争相手最低本賞金累計`,
    race_horses.`競争相手平均本賞金累計`,
    race_horses.`競争相手本賞金累計標準偏差`,
    race_horses.`競争相手最高1位完走平均賞金`,
    race_horses.`競争相手最低1位完走平均賞金`,
    race_horses.`競争相手平均1位完走平均賞金`,
    race_horses.`競争相手1位完走平均賞金標準偏差`,
    race_horses.`競争相手最高レース数平均賞金`,
    race_horses.`競争相手最低レース数平均賞金`,
    race_horses.`競争相手平均レース数平均賞金`,
    race_horses.`競争相手レース数平均賞金標準偏差`,
    race_horses.`競争相手瞬発戦好走馬_芝割合`,
    race_horses.`競争相手消耗戦好走馬_芝割合`,
    race_horses.`競争相手瞬発戦好走馬_ダート割合`,
    race_horses.`競争相手消耗戦好走馬_ダート割合`,
    race_horses.`競争相手瞬発戦好走馬_総合割合`,
    race_horses.`競争相手消耗戦好走馬_総合割合`,
    race_horses.`競争相手最高連続1着`,
    race_horses.`競争相手最低連続1着`,
    race_horses.`競争相手平均連続1着`,
    race_horses.`競争相手連続1着標準偏差`,
    race_horses.`競争相手最高連続3着内`,
    race_horses.`競争相手最低連続3着内`,
    race_horses.`競争相手平均連続3着内`,
    race_horses.`競争相手連続3着内標準偏差`,
    race_horses.`競争相手平均一走前着順差`,
    race_horses.`競争相手平均二走前着順差`,
    race_horses.`競争相手平均三走前着順差`,
    race_horses.`競争相手平均四走前着順差`,
    race_horses.`競争相手平均五走前着順差`,
    race_horses.`競争相手平均六走前着順差`,
    race_horses.`競争相手平均入厩何日前差`,
    race_horses.`競争相手平均馬体重差`,
    race_horses.`競争相手平均馬体重増減差`,
    race_horses.`競争相手平均前走距離差差`,
    race_horses.`競争相手平均年齢差`,
    race_horses.`競争相手平均レース数差`,
    race_horses.`競争相手平均1位完走差`,
    race_horses.`競争相手平均トップ3完走差`,
    race_horses.`競争相手平均1位完走率差`,
    race_horses.`競争相手平均トップ3完走率差`,
    race_horses.`競争相手平均過去5走勝率差`,
    race_horses.`競争相手平均過去5走トップ3完走率差`,
    race_horses.`競争相手平均場所レース数差`,
    race_horses.`競争相手平均場所1位完走差`,
    race_horses.`競争相手平均場所トップ3完走差`,
    race_horses.`競争相手平均場所1位完走率差`,
    race_horses.`競争相手平均場所トップ3完走率差`,
    race_horses.`競争相手平均トラック種別レース数差`,
    race_horses.`競争相手平均トラック種別1位完走差`,
    race_horses.`競争相手平均トラック種別トップ3完走差`,
    race_horses.`競争相手平均馬場状態レース数差`,
    race_horses.`競争相手平均馬場状態1位完走差`,
    race_horses.`競争相手平均馬場状態トップ3完走差`,
    race_horses.`競争相手平均距離レース数差`,
    race_horses.`競争相手平均距離1位完走差`,
    race_horses.`競争相手平均距離トップ3完走差`,
    race_horses.`競争相手平均四半期レース数差`,
    race_horses.`競争相手平均四半期1位完走差`,
    race_horses.`競争相手平均四半期トップ3完走差`,
    race_horses.`競争相手平均過去3走順位平方和差`,
    race_horses.`競争相手平均本賞金累計差`,
    race_horses.`競争相手平均1位完走平均賞金差`,
    race_horses.`競争相手平均レース数平均賞金差`,
    race_horses.`競争相手平均連続1着差`,
    race_horses.`競争相手平均連続3着内差`,

    -- -- Jockey features
    race_jockeys.`騎手レース数`,
    race_jockeys.`騎手1位完走`,
    race_jockeys.`騎手トップ3完走`,
    race_jockeys.`騎手1位完走率`,
    race_jockeys.`騎手トップ3完走率`,
    race_jockeys.`騎手過去5走勝率`,
    race_jockeys.`騎手過去5走トップ3完走率`,
    race_jockeys.`騎手場所レース数`,
    race_jockeys.`騎手場所1位完走`,
    race_jockeys.`騎手場所トップ3完走`,
    race_jockeys.`騎手場所1位完走率`,
    race_jockeys.`騎手場所トップ3完走率`,
    race_jockeys.`騎手距離レース数`,
    race_jockeys.`騎手距離1位完走`,
    race_jockeys.`騎手距離トップ3完走`,
    race_jockeys.`騎手距離1位完走率`,
    race_jockeys.`騎手距離トップ3完走率`,
    race_jockeys.`騎手本賞金累計`,
    race_jockeys.`騎手1位完走平均賞金`,
    race_jockeys.`騎手レース数平均賞金`,
    race_jockeys.`騎手連続1着`,
    race_jockeys.`騎手連続3着内`,
    race_jockeys.`競争相手最大騎手レース数`,
    race_jockeys.`競争相手最小騎手レース数`,
    race_jockeys.`競争相手平均騎手レース数`,
    race_jockeys.`競争相手騎手レース数標準偏差`,
    race_jockeys.`競争相手最大騎手1位完走`,
    race_jockeys.`競争相手最小騎手1位完走`,
    race_jockeys.`競争相手平均騎手1位完走`,
    race_jockeys.`競争相手騎手1位完走標準偏差`,
    race_jockeys.`競争相手最大騎手トップ3完走`,
    race_jockeys.`競争相手最小騎手トップ3完走`,
    race_jockeys.`競争相手平均騎手トップ3完走`,
    race_jockeys.`競争相手騎手トップ3完走標準偏差`,
    race_jockeys.`競争相手最大騎手1位完走率`,
    race_jockeys.`競争相手最小騎手1位完走率`,
    race_jockeys.`競争相手平均騎手1位完走率`,
    race_jockeys.`競争相手騎手1位完走率標準偏差`,
    race_jockeys.`競争相手最大騎手トップ3完走率`,
    race_jockeys.`競争相手最小騎手トップ3完走率`,
    race_jockeys.`競争相手平均騎手トップ3完走率`,
    race_jockeys.`競争相手騎手トップ3完走率標準偏差`,
    race_jockeys.`競争相手最大騎手過去5走勝率`,
    race_jockeys.`競争相手最小騎手過去5走勝率`,
    race_jockeys.`競争相手平均騎手過去5走勝率`,
    race_jockeys.`競争相手騎手過去5走勝率標準偏差`,
    race_jockeys.`競争相手最大騎手過去5走トップ3完走率`,
    race_jockeys.`競争相手最小騎手過去5走トップ3完走率`,
    race_jockeys.`競争相手平均騎手過去5走トップ3完走率`,
    race_jockeys.`競争相手騎手過去5走トップ3完走率標準偏差`,
    race_jockeys.`競争相手最大騎手場所レース数`,
    race_jockeys.`競争相手最小騎手場所レース数`,
    race_jockeys.`競争相手平均騎手場所レース数`,
    race_jockeys.`競争相手騎手場所レース数標準偏差`,
    race_jockeys.`競争相手最大騎手場所1位完走`,
    race_jockeys.`競争相手最小騎手場所1位完走`,
    race_jockeys.`競争相手平均騎手場所1位完走`,
    race_jockeys.`競争相手騎手場所1位完走標準偏差`,
    race_jockeys.`競争相手最大騎手場所トップ3完走`,
    race_jockeys.`競争相手最小騎手場所トップ3完走`,
    race_jockeys.`競争相手平均騎手場所トップ3完走`,
    race_jockeys.`競争相手騎手場所トップ3完走標準偏差`,
    race_jockeys.`競争相手最大騎手場所1位完走率`,
    race_jockeys.`競争相手最小騎手場所1位完走率`,
    race_jockeys.`競争相手平均騎手場所1位完走率`,
    race_jockeys.`競争相手騎手場所1位完走率標準偏差`,
    race_jockeys.`競争相手最大騎手場所トップ3完走率`,
    race_jockeys.`競争相手最小騎手場所トップ3完走率`,
    race_jockeys.`競争相手平均騎手場所トップ3完走率`,
    race_jockeys.`競争相手騎手場所トップ3完走率標準偏差`,
    race_jockeys.`競争相手最大騎手距離レース数`,
    race_jockeys.`競争相手最小騎手距離レース数`,
    race_jockeys.`競争相手平均騎手距離レース数`,
    race_jockeys.`競争相手騎手距離レース数標準偏差`,
    race_jockeys.`競争相手最大騎手距離1位完走`,
    race_jockeys.`競争相手最小騎手距離1位完走`,
    race_jockeys.`競争相手平均騎手距離1位完走`,
    race_jockeys.`競争相手騎手距離1位完走標準偏差`,
    race_jockeys.`競争相手最大騎手距離トップ3完走`,
    race_jockeys.`競争相手最小騎手距離トップ3完走`,
    race_jockeys.`競争相手平均騎手距離トップ3完走`,
    race_jockeys.`競争相手騎手距離トップ3完走標準偏差`,
    race_jockeys.`競争相手最大騎手距離1位完走率`,
    race_jockeys.`競争相手最小騎手距離1位完走率`,
    race_jockeys.`競争相手平均騎手距離1位完走率`,
    race_jockeys.`競争相手騎手距離1位完走率標準偏差`,
    race_jockeys.`競争相手最大騎手距離トップ3完走率`,
    race_jockeys.`競争相手最小騎手距離トップ3完走率`,
    race_jockeys.`競争相手平均騎手距離トップ3完走率`,
    race_jockeys.`競争相手騎手距離トップ3完走率標準偏差`,
    race_jockeys.`競争相手最大騎手本賞金累計`,
    race_jockeys.`競争相手最小騎手本賞金累計`,
    race_jockeys.`競争相手平均騎手本賞金累計`,
    race_jockeys.`競争相手騎手本賞金累計標準偏差`,
    race_jockeys.`競争相手最大騎手1位完走平均賞金`,
    race_jockeys.`競争相手最小騎手1位完走平均賞金`,
    race_jockeys.`競争相手平均騎手1位完走平均賞金`,
    race_jockeys.`競争相手騎手1位完走平均賞金標準偏差`,
    race_jockeys.`競争相手最大騎手レース数平均賞金`,
    race_jockeys.`競争相手最小騎手レース数平均賞金`,
    race_jockeys.`競争相手平均騎手レース数平均賞金`,
    race_jockeys.`競争相手騎手レース数平均賞金標準偏差`,
    race_jockeys.`競争相手最高騎手連続1着`,
    race_jockeys.`競争相手最低騎手連続1着`,
    race_jockeys.`競争相手平均騎手連続1着`,
    race_jockeys.`競争相手騎手連続1着標準偏差`,
    race_jockeys.`競争相手最高騎手連続3着内`,
    race_jockeys.`競争相手最低騎手連続3着内`,
    race_jockeys.`競争相手平均騎手連続3着内`,
    race_jockeys.`競争相手騎手連続3着内標準偏差`,
    race_jockeys.`競争相手平均騎手レース数差`,
    race_jockeys.`競争相手平均騎手1位完走差`,
    race_jockeys.`競争相手平均騎手トップ3完走差`,
    race_jockeys.`競争相手平均騎手1位完走率差`,
    race_jockeys.`競争相手平均騎手トップ3完走率差`,
    race_jockeys.`競争相手平均騎手過去5走勝率差`,
    race_jockeys.`競争相手平均騎手過去5走トップ3完走率差`,
    race_jockeys.`競争相手平均騎手場所レース数差`,
    race_jockeys.`競争相手平均騎手場所1位完走差`,
    race_jockeys.`競争相手平均騎手場所トップ3完走差`,
    race_jockeys.`競争相手平均騎手場所1位完走率差`,
    race_jockeys.`競争相手平均騎手場所トップ3完走率差`,
    race_jockeys.`競争相手平均騎手距離レース数差`,
    race_jockeys.`競争相手平均騎手距離1位完走差`,
    race_jockeys.`競争相手平均騎手距離トップ3完走差`,
    race_jockeys.`競争相手平均騎手距離1位完走率差`,
    race_jockeys.`競争相手平均騎手距離トップ3完走率差`,
    race_jockeys.`競争相手平均騎手本賞金累計差`,
    race_jockeys.`競争相手平均騎手1位完走平均賞金差`,
    race_jockeys.`競争相手平均騎手レース数平均賞金差`,
    race_jockeys.`競争相手平均騎手連続1着差`,
    race_jockeys.`競争相手平均騎手連続3着内差`,

    -- -- Trainer features
    race_trainers.`調教師レース数`,
    race_trainers.`調教師1位完走`,
    race_trainers.`調教師トップ3完走`,
    race_trainers.`調教師1位完走率`,
    race_trainers.`調教師トップ3完走率`,
    race_trainers.`調教師場所レース数`,
    race_trainers.`調教師場所1位完走`,
    race_trainers.`調教師場所トップ3完走`,
    race_trainers.`調教師場所1位完走率`,
    race_trainers.`調教師場所トップ3完走率`,
    race_trainers.`調教師本賞金累計`,
    race_trainers.`調教師1位完走平均賞金`,
    race_trainers.`調教師レース数平均賞金`,
    race_trainers.`競争相手最高調教師レース数`,
    race_trainers.`競争相手最低調教師レース数`,
    race_trainers.`競争相手平均調教師レース数`,
    race_trainers.`競争相手調教師レース数標準偏差`,
    race_trainers.`競争相手最高調教師1位完走`,
    race_trainers.`競争相手最低調教師1位完走`,
    race_trainers.`競争相手平均調教師1位完走`,
    race_trainers.`競争相手調教師1位完走標準偏差`,
    race_trainers.`競争相手最高調教師トップ3完走`,
    race_trainers.`競争相手最低調教師トップ3完走`,
    race_trainers.`競争相手平均調教師トップ3完走`,
    race_trainers.`競争相手調教師トップ3完走標準偏差`,
    race_trainers.`競争相手最高調教師1位完走率`,
    race_trainers.`競争相手最低調教師1位完走率`,
    race_trainers.`競争相手平均調教師1位完走率`,
    race_trainers.`競争相手調教師1位完走率標準偏差`,
    race_trainers.`競争相手最高調教師トップ3完走率`,
    race_trainers.`競争相手最低調教師トップ3完走率`,
    race_trainers.`競争相手平均調教師トップ3完走率`,
    race_trainers.`競争相手調教師トップ3完走率標準偏差`,
    race_trainers.`競争相手最高調教師場所レース数`,
    race_trainers.`競争相手最低調教師場所レース数`,
    race_trainers.`競争相手平均調教師場所レース数`,
    race_trainers.`競争相手調教師場所レース数標準偏差`,
    race_trainers.`競争相手最高調教師場所1位完走`,
    race_trainers.`競争相手最低調教師場所1位完走`,
    race_trainers.`競争相手平均調教師場所1位完走`,
    race_trainers.`競争相手調教師場所1位完走標準偏差`,
    race_trainers.`競争相手最高調教師場所トップ3完走`,
    race_trainers.`競争相手最低調教師場所トップ3完走`,
    race_trainers.`競争相手平均調教師場所トップ3完走`,
    race_trainers.`競争相手調教師場所トップ3完走標準偏差`,
    race_trainers.`競争相手最高調教師場所1位完走率`,
    race_trainers.`競争相手最低調教師場所1位完走率`,
    race_trainers.`競争相手平均調教師場所1位完走率`,
    race_trainers.`競争相手調教師場所1位完走率標準偏差`,
    race_trainers.`競争相手最高調教師場所トップ3完走率`,
    race_trainers.`競争相手最低調教師場所トップ3完走率`,
    race_trainers.`競争相手平均調教師場所トップ3完走率`,
    race_trainers.`競争相手調教師場所トップ3完走率標準偏差`,
    race_trainers.`競争相手最高調教師本賞金累計`,
    race_trainers.`競争相手最低調教師本賞金累計`,
    race_trainers.`競争相手平均調教師本賞金累計`,
    race_trainers.`競争相手調教師本賞金累計標準偏差`,
    race_trainers.`競争相手最高調教師1位完走平均賞金`,
    race_trainers.`競争相手最低調教師1位完走平均賞金`,
    race_trainers.`競争相手平均調教師1位完走平均賞金`,
    race_trainers.`競争相手調教師1位完走平均賞金標準偏差`,
    race_trainers.`競争相手最高調教師レース数平均賞金`,
    race_trainers.`競争相手最低調教師レース数平均賞金`,
    race_trainers.`競争相手平均調教師レース数平均賞金`,
    race_trainers.`競争相手調教師レース数平均賞金標準偏差`,
    race_trainers.`競争相手平均調教師レース数差`,
    race_trainers.`競争相手平均調教師1位完走差`,
    race_trainers.`競争相手平均調教師トップ3完走差`,
    race_trainers.`競争相手平均調教師1位完走率差`,
    race_trainers.`競争相手平均調教師トップ3完走率差`,
    race_trainers.`競争相手平均調教師場所レース数差`,
    race_trainers.`競争相手平均調教師場所1位完走差`,
    race_trainers.`競争相手平均調教師場所トップ3完走差`,
    race_trainers.`競争相手平均調教師場所1位完走率差`,
    race_trainers.`競争相手平均調教師場所トップ3完走率差`,
    race_trainers.`競争相手平均調教師本賞金累計差`,
    race_trainers.`競争相手平均調教師1位完走平均賞金差`,
    race_trainers.`競争相手平均調教師レース数平均賞金差`,

    -- Horse/Jockey/Trainer combined features
    combined_features.`馬騎手レース数`,
    combined_features.`馬騎手1位完走`,
    combined_features.`馬騎手1位完走率`,
    combined_features.`馬騎手トップ3完走`,
    combined_features.`馬騎手トップ3完走率`,
    combined_features.`馬騎手初二走`,
    combined_features.`馬騎手同騎手`,
    combined_features.`馬騎手場所レース数`,
    combined_features.`馬騎手場所1位完走`,
    combined_features.`馬騎手場所1位完走率`,
    combined_features.`馬騎手場所トップ3完走`,
    combined_features.`馬騎手場所トップ3完走率`,
    combined_features.`馬調教師レース数`,
    combined_features.`馬調教師1位完走`,
    combined_features.`馬調教師1位完走率`,
    combined_features.`馬調教師トップ3完走`,
    combined_features.`馬調教師トップ3完走率`,
    combined_features.`馬調教師初二走`,
    combined_features.`馬調教師同調教師`,
    combined_features.`馬調教師場所レース数`,
    combined_features.`馬調教師場所1位完走`,
    combined_features.`馬調教師場所1位完走率`,
    combined_features.`馬調教師場所トップ3完走`,
    combined_features.`馬調教師場所トップ3完走率`,

    -- Weather features
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
    {{ ref('int_race_horses') }} race_horses
  on
    base.`レースキー` = race_horses.`レースキー`
    and base.`馬番` = race_horses.`馬番`
  inner join
    {{ ref('int_race_jockeys') }} race_jockeys
  on
    base.`レースキー` = race_jockeys.`レースキー`
    and base.`馬番` = race_jockeys.`馬番`
  inner join
    {{ ref('int_race_trainers') }} race_trainers
  on
    base.`レースキー` = race_trainers.`レースキー`
    and base.`馬番` = race_trainers.`馬番`
  inner join
    combined_features
  on
    base.`レースキー` = combined_features.`レースキー`
    and base.`馬番` = combined_features.`馬番`
  inner join
    {{ ref('int_race_weather') }} race_weather
  on
    base.`レースキー` = race_weather.`レースキー`
  )

select * from final
