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

    -- Only 26 records where sed/bac diff exists from 1999 to 2011
    case
      when year(bac.`発走日時`) >= 2012 then sed.`レース条件_記号`
      else sed.`レース条件_記号`
    end `レース条件_記号`,

    -- Always the same between sed/bac
    bac.`レース条件_条件`,

    -- Only 2 records where sed/bac diff exists (one in 2001 and another in 2009)
    case
      when year(bac.`発走日時`) >= 2010 then bac.`レース条件_重量`
      else sed.`レース条件_重量`
    end `レース条件_重量`,

    -- Always the same between sed/bac
    bac.`レース条件_トラック情報_内外`,

    -- Only 2 records where sed/bac diff exists and none after 2001
    case
      when year(bac.`発走日時`) >= 2002 then bac.`レース条件_トラック情報_右左`
      else sed.`レース条件_トラック情報_右左`
    end `レース条件_トラック情報_右左`,

    -- Only 26 records where sed/bac diff exists and none after 2011
    case
      when year(bac.`発走日時`) >= 2012 then bac.`レース条件_種別`
      else sed.`レース条件_種別`
    end `レース条件_種別`,

    bac.`レース条件_グレード` as `レース条件_グレード`,
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

  -- There are about 3 cases where the track condition is different by 1
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
    base.`発走日時` as `meta_int_races_発走日時`,
    base.`場コード` as `meta_int_races_場コード`,
    base.`四半期` as `cat_四半期`,
    base.`距離` as `num_距離`,

    -- Known before race
    base.`事前_馬場状態コード` as `cat_事前_馬場状態コード`,
    base.`事前_馬場差` as `num_事前_馬場差`,

    -- Known after race
    base.`実績_馬場状態コード` as `cat_実績_馬場状態コード`,
    base_going_actual.`実績_馬場差` as `num_実績_馬場差`,

    -- Common
    base.`レース条件_記号` as `cat_レース条件_記号`,
    base.`レース条件_条件` as `cat_レース条件_条件`,
    base.`レース条件_重量` as `cat_レース条件_重量`,
    base.`レース条件_トラック情報_内外` as `cat_レース条件_トラック情報_内外`,
    base.`レース条件_トラック情報_右左` as `cat_レース条件_トラック情報_右左`,
    base.`レース条件_種別` as `cat_レース条件_種別`,
    base.`レース条件_グレード` as `cat_レース条件_グレード`, -- sed/bac always the same
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
    base.`草丈` as `num_草丈`,
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
