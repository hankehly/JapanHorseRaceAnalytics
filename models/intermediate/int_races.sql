with
  base as (
  select
    -- Metadata (do not use for training)
    bac.`レースキー`,
    bac.`発走日時`,
    bac.`レースキー_場コード` as `場コード`,

    case
      when extract(month from bac.`発走日時`) <= 3 then 1
      when extract(month from bac.`発走日時`) <= 6 then 2
      when extract(month from bac.`発走日時`) <= 9 then 3
      when extract(month from bac.`発走日時`) <= 12 then 4
    end as `四半期`,

    -- Base features
    bac.`レース条件_距離` as `距離`,
    coalesce(
      tyb.`馬場状態コード`,
      case
        when bac.`レース条件_トラック情報_芝ダ障害コード` = 'ダート' then kab.`ダ馬場状態コード`
        -- 障害コースの場合は芝馬場状態を使用する
        else kab.`芝馬場状態コード`
      end
    ) as `事前_馬場状態コード`,

    sed.`レース条件_馬場状態` as `実績_馬場状態コード`,

    bac.`レース条件_トラック情報_右左` as `事前_レース条件_トラック情報_右左`,
    bac.`レース条件_トラック情報_内外` as `事前_レース条件_トラック情報_内外`,
    bac.`レース条件_種別` as `事前_レース条件_種別`,
    bac.`レース条件_条件` as `事前_レース条件_条件`,
    bac.`レース条件_記号` as `事前_レース条件_記号`,
    bac.`レース条件_重量` as `事前_レース条件_重量`,
    bac.`レース条件_グレード` as `事前_レース条件_グレード`,

    sed.`レース条件_トラック情報_右左` as `実績_レース条件_トラック情報_右左`,
    sed.`レース条件_トラック情報_内外` as `実績_レース条件_トラック情報_内外`,
    sed.`レース条件_種別` as `実績_レース条件_種別`,
    sed.`レース条件_条件` as `実績_レース条件_条件`,
    sed.`レース条件_記号` as `実績_レース条件_記号`,
    sed.`レース条件_重量` as `実績_レース条件_重量`,
    sed.`レース条件_グレード` as `実績_レース条件_グレード`,

    bac.`頭数`,
    bac.`レース条件_トラック情報_芝ダ障害コード` as `トラック種別`,

    case
      when bac.`レース条件_トラック情報_芝ダ障害コード` = 'ダート' then kab.`ダ馬場差`
      -- 障害コースの場合は芝馬場差を使用する
      else kab.`芝馬場差`
    end `事前_馬場差`,

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
    kab.`中間降水量`
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
  ),

  -- There are 3 or so cases where the track condition is different by 1
  -- between horses, which doesn't make sense. We'll just take the average.
  base_going_actual as (
  select
    `レースキー`,
    AVG(`ＪＲＤＢデータ_馬場差`) as `実績_馬場差`
  from
    {{ ref('stg_jrdb__sed') }} sed
  where
    `馬成績_異常区分` = '0'
    and `ＪＲＤＢデータ_馬場差` is not null
  group by
    `レースキー`
  ),

  final as (
  select distinct
    base.`レースキー` as `meta_int_races_レースキー`,
    base.`発走日時` as `meta_発走日時`,
    base.`場コード` as `meta_場コード`,
    base.`四半期` as `cat_四半期`,
    base.`距離` as `cat_距離`,

    -- Known before race
    base.`事前_馬場状態コード` as `cat_事前_馬場状態コード`,
    base.`事前_レース条件_トラック情報_右左` as `cat_事前_レース条件_トラック情報_右左`,
    base.`事前_レース条件_トラック情報_内外` as `cat_事前_レース条件_トラック情報_内外`,
    base.`事前_レース条件_種別` as `cat_事前_レース条件_種別`,
    base.`事前_レース条件_条件` as `cat_事前_レース条件_条件`,
    base.`事前_レース条件_記号` as `cat_事前_レース条件_記号`,
    base.`事前_レース条件_重量` as `cat_事前_レース条件_重量`,
    base.`事前_レース条件_グレード` as `cat_事前_レース条件_グレード`,
    base.`事前_馬場差` as `num_事前_馬場差`,

    -- Known after race
    -- base.`実績_馬場状態コード` as `cat_実績_馬場状態コード`,
    -- base.`実績_レース条件_トラック情報_右左` as `cat_実績_レース条件_トラック情報_右左`,
    -- base.`実績_レース条件_トラック情報_内外` as `cat_実績_レース条件_トラック情報_内外`,
    -- base.`実績_レース条件_種別` as `cat_実績_レース条件_種別`,
    -- base.`実績_レース条件_条件` as `cat_実績_レース条件_条件`,
    -- base.`実績_レース条件_記号` as `cat_実績_レース条件_記号`,
    -- base.`実績_レース条件_重量` as `cat_実績_レース条件_重量`,
    -- base.`実績_レース条件_グレード` as `cat_実績_レース条件_グレード`,
    -- base_going_actual.`実績_馬場差` as `num_実績_馬場差`,

    -- Common
    base.`頭数` as `num_頭数`, -- only 4 records where before/actual diff exists (and only by 1)
    base.`トラック種別` as `cat_トラック種別`,
    base.`馬場状態内` as `cat_馬場状態内`,
    base.`馬場状態中` as `cat_馬場状態中`,
    base.`馬場状態外` as `cat_馬場状態外`,
    base.`直線馬場差最内` as `num_直線馬場差最内`,
    base.`直線馬場差内` as `num_直線馬場差内`,
    base.`直線馬場差中` as `num_直線馬場差中`,
    base.`直線馬場差外` as `num_直線馬場差外`,
    base.`直線馬場差大外` as `num_直線馬場差大外`,
    base.`芝種類` as `cat_芝種類`,
    base.`草丈` as `cat_草丈`,
    base.`転圧` as `cat_転圧`,
    base.`凍結防止剤` as `cat_凍結防止剤`,
    base.`中間降水量` as `num_中間降水量`
  from
    base
  left join
    base_going_actual
  using
    (`レースキー`)
  )

select * from final
