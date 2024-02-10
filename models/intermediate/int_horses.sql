with
  sed as (
  select
    *
  from
    {{ ref('stg_jrdb__sed') }}
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
    (血統登録番号, データ年月日) in (select 血統登録番号, MAX(データ年月日) from ukc group by 血統登録番号)
  ),

  good_finish_turf as (
  select
    "競走成績キー_血統登録番号" as "血統登録番号",
    sum(
      case
        when "ＪＲＤＢデータ_馬場差" <= -10 and "馬成績_着順" <= 3 then 1
        else 0
      end
    ) > 0 "瞬発戦好走馬",
    sum(
      case
  	    when "ＪＲＤＢデータ_馬場差" >= 10 and "馬成績_着順" <= 3 then 1
  	    else 0
      end
    ) > 0 "消耗戦好走馬"
  from
    sed
  where
    馬成績_異常区分 = '0'
    and レース条件_トラック情報_芝ダ障害コード = '芝'
  group by
    "血統登録番号"
  ),

  good_finish_dirt as (
  select
    "競走成績キー_血統登録番号" as "血統登録番号",
    sum(
      case
        when "ＪＲＤＢデータ_馬場差" <= -10 and "馬成績_着順" <= 3 then 1
        else 0
      end
    ) > 0 "瞬発戦好走馬",
    sum(
      case
  	    when "ＪＲＤＢデータ_馬場差" >= 10 and "馬成績_着順" <= 3 then 1
  	    else 0
      end
    ) > 0 "消耗戦好走馬"
  from
    sed
  where
    馬成績_異常区分 = '0'
    and レース条件_トラック情報_芝ダ障害コード = 'ダート'
  group by
    "血統登録番号"
  ),

  good_finish_any as (
  select
    "競走成績キー_血統登録番号" as "血統登録番号",
    sum(
      case
        when "ＪＲＤＢデータ_馬場差" <= -10 and "馬成績_着順" <= 3 then 1
        else 0
      end
    ) > 0 "瞬発戦好走馬",
    sum(
      case
  	    when "ＪＲＤＢデータ_馬場差" >= 10 and "馬成績_着順" <= 3 then 1
  	    else 0
      end
    ) > 0 "消耗戦好走馬"
  from
    sed
  where
    馬成績_異常区分 = '0'
  group by
    "血統登録番号"
  ),

  final as (
  select
    ukc."血統登録番号",
    case
      when ukc."性別コード" = '1' then '牡'
      when ukc."性別コード" = '2' then '牝'
      else 'セン'
    end "性別",
    ukc."生年月日" as "生年月日",
    good_finish_turf."瞬発戦好走馬" as "瞬発戦好走馬_芝",
    good_finish_turf."消耗戦好走馬" as "消耗戦好走馬_芝",
    good_finish_dirt."瞬発戦好走馬" as "瞬発戦好走馬_ダート",
    good_finish_dirt."消耗戦好走馬" as "消耗戦好走馬_ダート",
    good_finish_any."瞬発戦好走馬" as "瞬発戦好走馬_総合",
    good_finish_any."消耗戦好走馬" as "消耗戦好走馬_総合"
  from
    ukc_latest as ukc
  left join
    good_finish_turf 
  on
    ukc."血統登録番号" = good_finish_turf."血統登録番号"
  left join
    good_finish_dirt
  on
    ukc."血統登録番号" = good_finish_dirt."血統登録番号"
  left join
    good_finish_any
  on
    ukc."血統登録番号" = good_finish_any."血統登録番号"
  )

select * from final
