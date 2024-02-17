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
    tyb.`騎手コード` as `騎手コード`,
    bac.`発走日時`,
    kyi.`調教師コード`,
    sed.`馬成績_着順` as `着順`,
    sed.`本賞金`,
    case when sed.`馬成績_着順` = 1 then TRUE else FALSE end as `単勝的中`,
    coalesce(win_payouts.`払戻金`, 0) as `単勝払戻金`,
    case when sed.`馬成績_着順` <= 3 then TRUE else FALSE end as `複勝的中`,
    coalesce(place_payouts.`払戻金`, 0) as `複勝払戻金`,

    -- Base features
    kyi.`レースキー_場コード` as `場コード`,
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
    case
      when bac.`レース条件_トラック情報_芝ダ障害コード` = 'ダート' then kab.`ダ馬場状態内`
      else kab.`芝馬場状態内`
    end `馬場状態内`,
    case
      when bac.`レース条件_トラック情報_芝ダ障害コード` = 'ダート' then kab.`ダ馬場状態中`
      else kab.`芝馬場状態中`
    end `馬場状態中`,
    case
      when bac.`レース条件_トラック情報_芝ダ障害コード` = 'ダート' then kab.`ダ馬場状態外`
      else kab.`芝馬場状態外`
    end `馬場状態外`,
    kab.`直線馬場差最内`,
    kab.`直線馬場差内`,
    kab.`直線馬場差中`,
    kab.`直線馬場差外`,
    kab.`直線馬場差大外`,
    case
      when bac.`レース条件_トラック情報_芝ダ障害コード` = '芝' then kab.`芝種類`
      else null -- 芝以外の場合は固定値を入れる
    end `芝種類`,
    kab.`草丈`,
    kab.`転圧`,
    kab.`凍結防止剤`,
    kab.`中間降水量`,
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
        'cast(count(*) over (partition by `血統登録番号`, `騎手コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬騎手1位完走率`,

    -- places_horse_jockey
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬騎手トップ3完走`,

    -- ratio_place_horse_jockey
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `騎手コード` order by `発走日時`) - 1 as double)'
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
        'cast(count(*) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬騎手場所1位完走率`,

    -- places_horse_jockey_venue
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬騎手場所トップ3完走`,

    -- ratio_place_horse_jockey_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `騎手コード`, `場コード` order by `発走日時`) - 1 as double)'
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
        'cast(count(*) over (partition by `血統登録番号`, `調教師コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬調教師1位完走率`,

    -- places_horse_trainer
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬調教師トップ3完走`,

    -- ratio_place_horse_trainer
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `調教師コード` order by `発走日時`) - 1 as double)'
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
        'cast(count(*) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時`) - 1 as double)'
      )
    }}, 0) as `馬調教師場所1位完走率`,

    -- places_horse_trainer_venue
    coalesce(cast(sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding) as integer), 0) as `馬調教師場所トップ3完走`,

    -- ratio_place_horse_trainer_venue
    coalesce({{ 
      dbt_utils.safe_divide(
        'sum(case when `着順` <= 3 then 1 else 0 end) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時` rows between unbounded preceding and 1 preceding)',
        'cast(count(*) over (partition by `血統登録番号`, `調教師コード`, `場コード` order by `発走日時`) - 1 as double)'
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
    base.`調教師コード`,
    base.`着順` as `先読み注意_着順`,
    base.`本賞金` as `先読み注意_本賞金`,
    base.`単勝的中` as `先読み注意_単勝的中`,
    base.`単勝払戻金` as `先読み注意_単勝払戻金`,
    base.`複勝的中` as `先読み注意_複勝的中`,
    base.`複勝払戻金` as `先読み注意_複勝払戻金`,

    -- Base features
    base.`場コード`,
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
    base.`馬場状態内`,
    base.`馬場状態中`,
    base.`馬場状態外`,
    base.`直線馬場差最内`,
    base.`直線馬場差内`,
    base.`直線馬場差中`,
    base.`直線馬場差外`,
    base.`直線馬場差大外`,
    base.`芝種類`,
    base.`草丈`,
    base.`転圧`,
    base.`凍結防止剤`,
    base.`中間降水量`,
    base.`天候コード`,

   -- Horse features
    race_horses.`性別`,
    race_horses.`トラック種別瞬発戦好走馬`,
    race_horses.`トラック種別消耗戦好走馬`,
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
    race_horses.`競争相手最高ＩＤＭ`,
    race_horses.`競争相手最低ＩＤＭ`,
    race_horses.`競争相手平均ＩＤＭ`,
    race_horses.`競争相手ＩＤＭ標準偏差`,
    race_horses.`競争相手最高単勝オッズ`,
    race_horses.`競争相手最低単勝オッズ`,
    race_horses.`競争相手平均単勝オッズ`,
    race_horses.`競争相手単勝オッズ標準偏差`,
    race_horses.`競争相手最高複勝オッズ`,
    race_horses.`競争相手最低複勝オッズ`,
    race_horses.`競争相手平均複勝オッズ`,
    race_horses.`競争相手複勝オッズ標準偏差`,
    race_horses.`競争相手最高騎手指数`,
    race_horses.`競争相手最低騎手指数`,
    race_horses.`競争相手平均騎手指数`,
    race_horses.`競争相手騎手指数標準偏差`,
    race_horses.`競争相手最高情報指数`,
    race_horses.`競争相手最低情報指数`,
    race_horses.`競争相手平均情報指数`,
    race_horses.`競争相手情報指数標準偏差`,
    race_horses.`競争相手最高オッズ指数`,
    race_horses.`競争相手最低オッズ指数`,
    race_horses.`競争相手平均オッズ指数`,
    race_horses.`競争相手オッズ指数標準偏差`,
    race_horses.`競争相手最高パドック指数`,
    race_horses.`競争相手最低パドック指数`,
    race_horses.`競争相手平均パドック指数`,
    race_horses.`競争相手パドック指数標準偏差`,
    race_horses.`競争相手最高総合指数`,
    race_horses.`競争相手最低総合指数`,
    race_horses.`競争相手平均総合指数`,
    race_horses.`競争相手総合指数標準偏差`,
    race_horses.`競争相手最高負担重量`,
    race_horses.`競争相手最低負担重量`,
    race_horses.`競争相手平均負担重量`,
    race_horses.`競争相手負担重量標準偏差`,
    race_horses.`競争相手最高ローテーション`,
    race_horses.`競争相手最低ローテーション`,
    race_horses.`競争相手平均ローテーション`,
    race_horses.`競争相手ローテーション標準偏差`,
    race_horses.`競争相手最高基準オッズ`,
    race_horses.`競争相手最低基準オッズ`,
    race_horses.`競争相手平均基準オッズ`,
    race_horses.`競争相手基準オッズ標準偏差`,
    race_horses.`競争相手最高基準複勝オッズ`,
    race_horses.`競争相手最低基準複勝オッズ`,
    race_horses.`競争相手平均基準複勝オッズ`,
    race_horses.`競争相手基準複勝オッズ標準偏差`,
    race_horses.`競争相手最高人気指数`,
    race_horses.`競争相手最低人気指数`,
    race_horses.`競争相手平均人気指数`,
    race_horses.`競争相手人気指数標準偏差`,
    race_horses.`競争相手最高調教指数`,
    race_horses.`競争相手最低調教指数`,
    race_horses.`競争相手平均調教指数`,
    race_horses.`競争相手調教指数標準偏差`,
    race_horses.`競争相手最高厩舎指数`,
    race_horses.`競争相手最低厩舎指数`,
    race_horses.`競争相手平均厩舎指数`,
    race_horses.`競争相手厩舎指数標準偏差`,
    race_horses.`競争相手最高騎手期待連対率`,
    race_horses.`競争相手最低騎手期待連対率`,
    race_horses.`競争相手平均騎手期待連対率`,
    race_horses.`競争相手騎手期待連対率標準偏差`,
    race_horses.`競争相手最高激走指数`,
    race_horses.`競争相手最低激走指数`,
    race_horses.`競争相手平均激走指数`,
    race_horses.`競争相手激走指数標準偏差`,
    race_horses.`競争相手最高展開予想データ_テン指数`,
    race_horses.`競争相手最低展開予想データ_テン指数`,
    race_horses.`競争相手平均展開予想データ_テン指数`,
    race_horses.`競争相手展開予想データ_テン指数標準偏差`,
    race_horses.`競争相手最高展開予想データ_ペース指数`,
    race_horses.`競争相手最低展開予想データ_ペース指数`,
    race_horses.`競争相手平均展開予想データ_ペース指数`,
    race_horses.`競争相手展開予想データ_ペース指数標準偏差`,
    race_horses.`競争相手最高展開予想データ_上がり指数`,
    race_horses.`競争相手最低展開予想データ_上がり指数`,
    race_horses.`競争相手平均展開予想データ_上がり指数`,
    race_horses.`競争相手展開予想データ_上がり指数標準偏差`,
    race_horses.`競争相手最高展開予想データ_位置指数`,
    race_horses.`競争相手最低展開予想データ_位置指数`,
    race_horses.`競争相手平均展開予想データ_位置指数`,
    race_horses.`競争相手展開予想データ_位置指数標準偏差`,
    race_horses.`競争相手最高展開予想データ_道中差`,
    race_horses.`競争相手最低展開予想データ_道中差`,
    race_horses.`競争相手平均展開予想データ_道中差`,
    race_horses.`競争相手展開予想データ_道中差標準偏差`,
    race_horses.`競争相手最高展開予想データ_後３Ｆ差`,
    race_horses.`競争相手最低展開予想データ_後３Ｆ差`,
    race_horses.`競争相手平均展開予想データ_後３Ｆ差`,
    race_horses.`競争相手展開予想データ_後３Ｆ差標準偏差`,
    race_horses.`競争相手最高展開予想データ_ゴール差`,
    race_horses.`競争相手最低展開予想データ_ゴール差`,
    race_horses.`競争相手平均展開予想データ_ゴール差`,
    race_horses.`競争相手展開予想データ_ゴール差標準偏差`,
    race_horses.`競争相手最高騎手期待単勝率`,
    race_horses.`競争相手最低騎手期待単勝率`,
    race_horses.`競争相手平均騎手期待単勝率`,
    race_horses.`競争相手騎手期待単勝率標準偏差`,
    race_horses.`競争相手最高騎手期待３着内率`,
    race_horses.`競争相手最低騎手期待３着内率`,
    race_horses.`競争相手平均騎手期待３着内率`,
    race_horses.`競争相手騎手期待３着内率標準偏差`,
    race_horses.`競争相手最高展開参考データ_馬スタート指数`,
    race_horses.`競争相手最低展開参考データ_馬スタート指数`,
    race_horses.`競争相手平均展開参考データ_馬スタート指数`,
    race_horses.`競争相手展開参考データ_馬スタート指数標準偏差`,
    race_horses.`競争相手最高展開参考データ_馬出遅率`,
    race_horses.`競争相手最低展開参考データ_馬出遅率`,
    race_horses.`競争相手平均展開参考データ_馬出遅率`,
    race_horses.`競争相手展開参考データ_馬出遅率標準偏差`,
    race_horses.`競争相手最高万券指数`,
    race_horses.`競争相手最低万券指数`,
    race_horses.`競争相手平均万券指数`,
    race_horses.`競争相手万券指数標準偏差`,
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
    race_horses.`競争相手トラック種別瞬発戦好走馬割合`,
    race_horses.`競争相手トラック種別消耗戦好走馬割合`,
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
    race_horses.`競争相手平均ＩＤＭ差`,
    race_horses.`競争相手平均単勝オッズ差`,
    race_horses.`競争相手平均複勝オッズ差`,
    race_horses.`競争相手平均騎手指数差`,
    race_horses.`競争相手平均情報指数差`,
    race_horses.`競争相手平均オッズ指数差`,
    race_horses.`競争相手平均パドック指数差`,
    race_horses.`競争相手平均総合指数差`,
    race_horses.`競争相手平均負担重量差`,
    race_horses.`競争相手平均ローテーション差`,
    race_horses.`競争相手平均基準オッズ差`,
    race_horses.`競争相手平均基準複勝オッズ差`,
    race_horses.`競争相手平均人気指数差`,
    race_horses.`競争相手平均調教指数差`,
    race_horses.`競争相手平均厩舎指数差`,
    race_horses.`競争相手平均騎手期待連対率差`,
    race_horses.`競争相手平均激走指数差`,
    race_horses.`競争相手平均展開予想データ_テン指数差`,
    race_horses.`競争相手平均展開予想データ_ペース指数差`,
    race_horses.`競争相手平均展開予想データ_上がり指数差`,
    race_horses.`競争相手平均展開予想データ_位置指数差`,
    race_horses.`競争相手平均展開予想データ_道中差差`,
    race_horses.`競争相手平均展開予想データ_後３Ｆ差差`,
    race_horses.`競争相手平均展開予想データ_ゴール差差`,
    race_horses.`競争相手平均騎手期待単勝率差`,
    race_horses.`競争相手平均騎手期待３着内率差`,
    race_horses.`競争相手平均展開参考データ_馬スタート指数差`,
    race_horses.`競争相手平均展開参考データ_馬出遅率差`,
    race_horses.`競争相手平均万券指数差`,
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

    -- Jockey features
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

    -- Trainer features
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
